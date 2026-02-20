from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from de.estimate import (
    estimate_de,
    compare_formats_tables,
    compare_formats,
)
from de.formats import ParquetCpp


@pytest.fixture
def table():
    return pa.table({"a": [1, 2, 3], "b": ["x", "y", "z"]})


@pytest.fixture
def edit():
    return pa.table({"a": [4, 5, 6], "b": ["p", "q", "r"]})


def noop_metric(paths):
    return {"total_len": 100, "chunk_bytes": 50}


class TestEstimateDe:
    def test_returns_expected_keys(self):
        with patch("de.estimate.estimate", return_value=(300, 150, 75)):
            result = estimate_de([Path("a.parquet")])
        assert result == {
            "total_len": 300,
            "chunk_bytes": 150,
            "compressed_chunk_bytes": 75,
        }

    def test_converts_paths_to_strings(self):
        with patch("de.estimate.estimate", return_value=(0, 0, 0)) as mock:
            estimate_de([Path("a.parquet"), Path("b.parquet")])
        mock.assert_called_once_with(["a.parquet", "b.parquet"])


class TestCompareFormatsTables:
    def test_one_record_per_format_per_variant(self, tmp_path, table, edit):
        formats = [ParquetCpp(use_cdc=False), ParquetCpp(use_cdc=False, compression="zstd")]
        results = compare_formats_tables(
            formats,
            {"edit1": {"original": table, "edit1": edit}},
            tmp_path,
            metrics=(noop_metric,),
        )
        assert len(results) == 2
        assert all(r["variant"] == "edit1" for r in results)
        assert {r["params"] for r in results} == {"", "zstd"}

    def test_record_has_expected_fields(self, tmp_path, table, edit):
        results = compare_formats_tables(
            [ParquetCpp(use_cdc=False)],
            {"edit1": {"original": table, "edit1": edit}},
            tmp_path,
            metrics=(noop_metric,),
        )
        assert results[0]["variant"] == "edit1"
        assert results[0]["format"] == "ParquetCpp"
        assert results[0]["params"] == ""
        assert results[0]["total_len"] == 100
        assert results[0]["chunk_bytes"] == 50

    def test_files_written_to_correct_path(self, tmp_path, table, edit):
        compare_formats_tables(
            [ParquetCpp(use_cdc=False)],
            {"edit1": {"original": table, "edit1": edit}},
            tmp_path,
            metrics=(noop_metric,),
        )
        assert (tmp_path / "edit1" / "ParquetCpp" / "original.parquet").exists()
        assert (tmp_path / "edit1" / "ParquetCpp" / "edit1.parquet").exists()

    def test_metric_receives_all_paths(self, tmp_path, table, edit):
        calls = []
        compare_formats_tables(
            [ParquetCpp(use_cdc=False)],
            {"edit1": {"original": table, "edit1": edit}},
            tmp_path,
            metrics=(lambda paths: calls.append(paths) or noop_metric(paths),),
        )
        assert len(calls) == 1
        names = [p.name for p in calls[0]]
        assert any("original" in n for n in names)
        assert any("edit1" in n for n in names)

    def test_multiple_metrics_merged_into_record(self, tmp_path, table, edit):
        results = compare_formats_tables(
            [ParquetCpp(use_cdc=False)],
            {"edit1": {"original": table, "edit1": edit}},
            tmp_path,
            metrics=(noop_metric, lambda _: {"extra": 99}),
        )
        assert results[0]["total_len"] == 100
        assert results[0]["extra"] == 99

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
            metrics=(noop_metric,),
        )
        assert len(results) == 1
        assert results[0]["variant"] == "combined"
        assert results[0]["params"] == ""


class TestCompareFormats:
    def test_one_record_per_variant(self, tmp_path, table):
        variants = [
            ParquetCpp(use_cdc=False, compression="zstd"),
            ParquetCpp(use_cdc=False, compression="snappy"),
        ]
        results = compare_formats(
            ParquetCpp(use_cdc=False), variants, table, tmp_path, metrics=(noop_metric,)
        )
        assert len(results) == 2
        assert {r["params"] for r in results} == {"zstd", "snappy"}

    def test_record_has_expected_fields(self, tmp_path, table):
        results = compare_formats(
            ParquetCpp(use_cdc=False),
            [ParquetCpp(use_cdc=False, compression="zstd")],
            table,
            tmp_path,
            metrics=(noop_metric,),
        )
        assert results[0] == {
            "format": "ParquetCpp",
            "params": "zstd",
            "compression": "zstd",
            "total_len": 100,
            "chunk_bytes": 50,
        }

    def test_baseline_and_variant_files_written(self, tmp_path, table):
        compare_formats(
            ParquetCpp(use_cdc=False),
            [ParquetCpp(use_cdc=False, compression="zstd")],
            table,
            tmp_path,
            metrics=(noop_metric,),
        )
        assert (tmp_path / ".parquet").exists()
        assert (tmp_path / "-zstd.parquet").exists()

    def test_metric_receives_baseline_and_variant_paths(self, tmp_path, table):
        calls = []
        compare_formats(
            ParquetCpp(use_cdc=False),
            [ParquetCpp(use_cdc=False, compression="zstd")],
            table,
            tmp_path,
            metrics=(lambda paths: calls.append(paths) or noop_metric(paths),),
        )
        assert len(calls) == 1
        assert "zstd" in calls[0][1].name

    def test_multiple_metrics_merged_into_record(self, tmp_path, table):
        results = compare_formats(
            ParquetCpp(use_cdc=False),
            [ParquetCpp(use_cdc=False, compression="zstd")],
            table,
            tmp_path,
            metrics=(noop_metric, lambda _: {"extra": 42}),
        )
        assert results[0]["total_len"] == 100
        assert results[0]["extra"] == 42
