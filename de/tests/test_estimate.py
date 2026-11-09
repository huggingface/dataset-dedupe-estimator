from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
import random

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from de.estimate import (
    estimate,
    compare_formats_tables,
    compare_formats,
    write_formats_tables,
)
from de.formats import ParquetCpp

# many content defined chunks of 64KiB on average
SIZE = 4 * 2**20
# the largest chunk either chunker cuts in random data
MAX_CHUNK = 2 * 2**16


@pytest.fixture
def table():
    return pa.table({"a": [1, 2, 3], "b": ["x", "y", "z"]})


@pytest.fixture
def edit():
    return pa.table({"a": [4, 5, 6], "b": ["p", "q", "r"]})


@pytest.fixture(scope="module")
def blobs(tmp_path_factory):
    """Files of random bytes, the estimators chunk files regardless of their format."""
    rng = random.Random(42)
    data = rng.randbytes(SIZE)
    middle = SIZE // 2
    contents = {
        "data": data,
        "copy": data,
        "other": rng.randbytes(SIZE),
        "inserted": data[:middle] + rng.randbytes(100) + data[middle:],
        "text": b"".join(b"row %d\n" % rng.randrange(10**6) for _ in range(SIZE // 12)),
    }
    directory = tmp_path_factory.mktemp("blobs")
    for name, content in contents.items():
        (directory / name).write_bytes(content)
    return directory


class TestEstimate:
    def test_identical_files_deduplicate_to_a_single_copy(self, blobs):
        result = estimate([blobs / "data", blobs / "copy"], plot=False)
        assert result == {
            "numfiles": 2,
            "total_len": 2 * SIZE,
            "chunk_bytes": SIZE,
            "compressed_chunk_bytes": result["compressed_chunk_bytes"],
            "dedup_ratio": 0.5,
            "xet_bytes": SIZE,
            "xet_dedup_ratio": 0.5,
        }

    def test_unrelated_files_dont_deduplicate(self, blobs):
        result = estimate([blobs / "data", blobs / "other"], plot=False)
        assert result["chunk_bytes"] == result["xet_bytes"] == 2 * SIZE
        assert result["dedup_ratio"] == result["xet_dedup_ratio"] == 1.0

    def test_insertion_changes_only_the_chunks_around_it(self, blobs):
        result = estimate([blobs / "data", blobs / "inserted"], plot=False)
        assert result["total_len"] == 2 * SIZE + 100
        # the 100 inserted bytes and the chunks they land in, the cuts resync after
        for unique in (result["chunk_bytes"], result["xet_bytes"]):
            assert SIZE + 100 < unique < SIZE + 2 * MAX_CHUNK

    def test_compressed_chunk_bytes(self, blobs):
        single = estimate([blobs / "data"], plot=False)
        pair = estimate([blobs / "data", blobs / "copy"], plot=False)
        assert pair["compressed_chunk_bytes"] == single["compressed_chunk_bytes"]
        # random bytes don't compress, repetitive text does
        assert single["compressed_chunk_bytes"] >= single["chunk_bytes"]
        text = estimate([blobs / "text"], plot=False)
        assert text["compressed_chunk_bytes"] < text["chunk_bytes"]

    def test_plot_writes_chunk_images(self, tmp_path, table):
        path = tmp_path / "a.parquet"
        pq.write_table(table, path)
        estimate([path], plot=False)
        assert list(tmp_path.glob("*.png")) == []
        estimate([path])
        assert {p.name for p in tmp_path.glob("*.png")} == {
            "a.parquet.png",
            "merged.png",
        }


class TestCompareFormatsTables:
    def test_one_record_per_format_per_variant(self, tmp_path, table, edit):
        formats = [
            ParquetCpp(use_cdc=False),
            ParquetCpp(use_cdc=False, compression="zstd"),
        ]
        results = compare_formats_tables(
            formats,
            {"edit1": {"original": table, "edit1": edit}},
            tmp_path,
        )
        assert len(results) == 2
        assert all(r.group == "edit1" for r in results)
        assert {r.format.paramstem for r in results} == {"", "zstd"}

    def test_record_has_expected_fields(self, tmp_path, table, edit):
        results = compare_formats_tables(
            [ParquetCpp(use_cdc=False)],
            {"edit1": {"original": table, "edit1": edit}},
            tmp_path,
        )
        assert results[0].group == "edit1"
        assert results[0].format.name == "parquet-cpp"
        assert results[0].format.paramstem == ""
        files = (tmp_path / "edit1" / "parquet-cpp").glob("*.parquet")
        assert results[0].total_len == sum(path.stat().st_size for path in files)
        # the files are smaller than a chunk and differ, nothing deduplicates
        assert results[0].chunk_bytes == results[0].total_len

    def test_files_of_a_group_are_estimated_together(self, tmp_path, table):
        [result] = compare_formats_tables(
            [ParquetCpp(use_cdc=False)],
            {"copies": {"original": table, "copy": table}},
            tmp_path,
        )
        assert result.numfiles == 2
        assert result.dedup_ratio == 0.5

    def test_files_written_to_correct_path(self, tmp_path, table, edit):
        compare_formats_tables(
            [ParquetCpp(use_cdc=False)],
            {"edit1": {"original": table, "edit1": edit}},
            tmp_path,
        )
        assert (tmp_path / "edit1" / "parquet-cpp" / "original.parquet").exists()
        assert (tmp_path / "edit1" / "parquet-cpp" / "edit1.parquet").exists()

    def test_numfiles_matches_variant_count(self, tmp_path, table, edit):
        results = compare_formats_tables(
            [ParquetCpp(use_cdc=False)],
            {"edit1": {"original": table, "edit1": edit}},
            tmp_path,
        )
        assert results[0].numfiles == 2

    def test_multiple_variants_produce_multiple_records(self, tmp_path, table, edit):
        results = compare_formats_tables(
            [ParquetCpp(use_cdc=False)],
            {
                "edit1": {"original": table, "edit1": edit},
                "edit2": {"original": table, "edit2": edit},
            },
            tmp_path,
        )
        assert len(results) == 2
        assert {r.group for r in results} == {"edit1", "edit2"}

    def test_paths_variant_rewrites_and_estimates_group(self, tmp_path, table):
        src = tmp_path / "src"
        src.mkdir()
        pq.write_table(table, src / "file0.parquet")
        pq.write_table(table, src / "file1.parquet")
        results = compare_formats_tables(
            [ParquetCpp(use_cdc=False)],
            {
                "combined": {
                    "file0": src / "file0.parquet",
                    "file1": src / "file1.parquet",
                }
            },
            tmp_path,
        )
        assert len(results) == 1
        assert results[0].group == "combined"
        assert results[0].format.paramstem == ""
        rewritten = tmp_path / "combined" / "parquet-cpp"
        assert {path.name for path in rewritten.glob("*.parquet")} == {
            "file0.parquet",
            "file1.parquet",
        }
        # the same source is rewritten to the same bytes
        assert results[0].dedup_ratio == 0.5

    def test_records_in_input_order(self, tmp_path, table, edit):
        formats = [ParquetCpp(use_cdc=False), ParquetCpp(use_cdc=True)]
        results = compare_formats_tables(
            formats,
            {"b": {"original": table, "b": edit}, "a": {"original": table, "a": edit}},
            tmp_path,
        )
        assert [(r.group, r.format) for r in results] == [
            ("b", formats[0]),
            ("b", formats[1]),
            ("a", formats[0]),
            ("a", formats[1]),
        ]


class TestWriteFormatsTables:
    def test_paths_in_input_order(self, tmp_path, table, edit):
        formats = [ParquetCpp(use_cdc=False), ParquetCpp(use_cdc=True)]
        with ThreadPoolExecutor() as executor:
            written = write_formats_tables(
                formats, {"g": {"original": table, "edit": edit}}, tmp_path, executor
            )
        assert list(written) == [("g", formats[0]), ("g", formats[1])]
        for (group, fmt), paths in written.items():
            assert paths == [
                fmt.derive_path(name, tmp_path / group / fmt.name)
                for name in ("original", "edit")
            ]
            assert all(path.exists() for path in paths)


class TestCompareFormats:
    def test_one_record_per_variant(self, tmp_path, table):
        contenders = {
            "v1": ParquetCpp(use_cdc=False, compression="zstd"),
            "v2": ParquetCpp(use_cdc=False, compression="snappy"),
        }
        results = compare_formats(
            ParquetCpp(use_cdc=False), contenders, table, tmp_path
        )
        assert len(results) == 2
        assert {r.format.paramstem for r in results} == {"zstd", "snappy"}

    def test_record_has_expected_fields(self, tmp_path, table):
        results = compare_formats(
            ParquetCpp(use_cdc=False),
            {"v1": ParquetCpp(use_cdc=False, compression="zstd")},
            table,
            tmp_path,
        )
        assert results[0].format.name == "parquet-cpp"
        assert results[0].format.paramstem == "zstd"
        files = [tmp_path / "baseline.parquet", tmp_path / "v1-zstd.parquet"]
        assert results[0].total_len == sum(path.stat().st_size for path in files)

    def test_baseline_and_variant_files_written(self, tmp_path, table):
        compare_formats(
            ParquetCpp(use_cdc=False),
            {"v1": ParquetCpp(use_cdc=False, compression="zstd")},
            table,
            tmp_path,
        )
        assert (tmp_path / "baseline.parquet").exists()
        assert (tmp_path / "v1-zstd.parquet").exists()

    def test_estimate_runs_on_baseline_and_variant(self, tmp_path, table):
        # a variant written like the baseline is a copy of it
        [result] = compare_formats(
            ParquetCpp(use_cdc=False),
            {"same": ParquetCpp(use_cdc=False)},
            table,
            tmp_path,
        )
        assert result.numfiles == 2
        assert result.dedup_ratio == 0.5

    def test_multiple_metrics_merged_into_record(self, tmp_path, table):
        results = compare_formats(
            ParquetCpp(use_cdc=False),
            {
                "v1": ParquetCpp(use_cdc=False, compression="zstd"),
                "v2": ParquetCpp(use_cdc=False, compression="snappy"),
            },
            table,
            tmp_path,
        )
        baseline = tmp_path / "baseline.parquet"
        # the records are in the order the writes completed
        variants = {"zstd": "v1-zstd.parquet", "snappy": "v2-snappy.parquet"}
        for result in results:
            variant = tmp_path / variants[result.format.paramstem]
            metrics = estimate([baseline, variant], plot=False)
            fields = asdict(result)
            assert {key: fields[key] for key in metrics} == metrics
