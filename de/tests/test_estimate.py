from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from de.estimate import (
    estimate,
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


class TestEstimateDe:
    def test_returns_expected_keys(self):
        with patch("de.estimate._estimate_de", return_value=(300, 150, 75)), patch(
            "de.estimate._estimate_xet", return_value=0
        ):
            result = estimate([Path("a.parquet")])
        assert result["total_len"] == 300
        assert result["chunk_bytes"] == 150
        assert result["compressed_chunk_bytes"] == 75

    def test_converts_paths_to_strings(self):
        with patch(
            "de.estimate._estimate_de", return_value=(100, 50, 25)
        ) as mock, patch("de.estimate._estimate_xet", return_value=10):
            estimate([Path("a.parquet"), Path("b.parquet")])
        mock.assert_called_once_with(["a.parquet", "b.parquet"])


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
        assert results[0].total_len > 0
        assert results[0].chunk_bytes > 0

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
        assert results[0].total_len > 0
        assert results[0].chunk_bytes > 0

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
        results = compare_formats(
            ParquetCpp(use_cdc=False),
            {"v1": ParquetCpp(use_cdc=False, compression="zstd")},
            table,
            tmp_path,
        )
        assert len(results) == 1
        assert results[0].total_len > 0
        assert 0 < results[0].dedup_ratio <= 1

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
        assert len(results) == 2
        assert all(r.total_len > 0 for r in results)
        assert all(r.chunk_bytes > 0 for r in results)
