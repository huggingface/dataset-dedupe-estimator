from pathlib import Path
from unittest.mock import patch, MagicMock

import pyarrow as pa
import pytest

from de.estimate import (
    estimate_de,
    estimate_xtool,
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


class TestEstimateXtool:
    def test_parses_transmitted_bytes(self, monkeypatch):
        monkeypatch.setenv("XTOOL_TOKEN", "fake-token")
        mock_result = MagicMock()
        mock_result.stderr = (
            "Dedupping 2 files...\nUsing lz4 compression\n\n\n"
            "Clean results:\nTransmitted 12345678 bytes in total.\n"
        )
        with patch("de.estimate.subprocess.run", return_value=mock_result):
            result = estimate_xtool([Path("a.parquet"), Path("b.parquet")])
        assert result == {"transmitted_xtool_bytes": 12345678}


class TestCompareFormatsTables:
    def test_one_record_per_format_per_variant(self, tmp_path, table, edit):
        formats = [ParquetCpp(), ParquetCpp(compression="zstd")]
        results = compare_formats_tables(
            formats,
            {"edit1": {"original": table, "edit1": edit}},
            tmp_path,
            metrics=(noop_metric,),
        )
        assert len(results) == 2
        assert results[0]["name"] == "edit1"
        assert results[1]["name"] == "edit1"
        assert results[0]["compression"] == "none"
        assert results[1]["compression"] == "zstd"

    def test_record_has_name_compression_and_metric_fields(self, tmp_path, table, edit):
        results = compare_formats_tables(
            [ParquetCpp()],
            {"edit1": {"original": table, "edit1": edit}},
            tmp_path,
            metrics=(noop_metric,),
        )
        assert results[0] == {
            "name": "edit1",
            "compression": "none",
            "total_len": 100,
            "chunk_bytes": 50,
        }

    def test_files_written_with_prefix(self, tmp_path, table, edit):
        compare_formats_tables(
            [ParquetCpp()],
            {"edit1": {"original": table, "edit1": edit}},
            tmp_path,
            prefix="run1",
            metrics=(noop_metric,),
        )
        assert (tmp_path / "parquet" / "run1-original.parquet").exists()
        assert (tmp_path / "parquet" / "run1-edit1.parquet").exists()

    def test_metric_receives_original_and_edit_paths(self, tmp_path, table, edit):
        calls = []
        compare_formats_tables(
            [ParquetCpp()],
            {"edit1": {"original": table, "edit1": edit}},
            tmp_path,
            metrics=(lambda paths: calls.append(paths) or {},),
        )
        assert len(calls) == 1
        assert "original" in calls[0][0].name
        assert "edit1" in calls[0][1].name

    def test_multiple_metrics_merged_into_record(self, tmp_path, table, edit):
        results = compare_formats_tables(
            [ParquetCpp()],
            {"edit1": {"original": table, "edit1": edit}},
            tmp_path,
            metrics=(lambda _: {"a": 1}, lambda _: {"b": 2}),
        )
        assert results[0]["a"] == 1
        assert results[0]["b"] == 2

    def test_paths_variant_rewrites_and_estimates_group(self, tmp_path, table):
        src = tmp_path / "src"
        src.mkdir()
        import pyarrow.parquet as pq

        pq.write_table(table, src / "file0.parquet")
        pq.write_table(table, src / "file1.parquet")
        results = compare_formats_tables(
            [ParquetCpp()],
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
        assert results[0]["name"] == "combined"
        assert results[0]["compression"] == "none"


class TestCompareFormats:
    def test_one_record_per_variant(self, tmp_path, table):
        variants = [
            ParquetCpp(compression="zstd"),
            ParquetCpp(compression="snappy"),
        ]
        results = compare_formats(
            ParquetCpp(), variants, table, tmp_path, metrics=(noop_metric,)
        )
        assert len(results) == 2
        assert results[0]["kind"] == "parquet-zstd"
        assert results[1]["kind"] == "parquet-snappy"

    def test_record_has_kind_compression_and_metric_fields(self, tmp_path, table):
        results = compare_formats(
            ParquetCpp(),
            [ParquetCpp(compression="zstd")],
            table,
            tmp_path,
            metrics=(noop_metric,),
        )
        assert results[0] == {
            "kind": "parquet-zstd",
            "compression": "zstd",
            "total_len": 100,
            "chunk_bytes": 50,
        }

    def test_baseline_and_variant_files_written(self, tmp_path, table):
        compare_formats(
            ParquetCpp(),
            [ParquetCpp(compression="zstd")],
            table,
            tmp_path,
            metrics=(noop_metric,),
        )
        assert (tmp_path / "parquet" / ".parquet").exists()
        assert (tmp_path / "parquet-zstd" / ".parquet").exists()

    def test_metric_receives_baseline_and_variant_paths(self, tmp_path, table):
        calls = []
        compare_formats(
            ParquetCpp(),
            [ParquetCpp(compression="zstd")],
            table,
            tmp_path,
            metrics=(lambda paths: calls.append(paths) or {},),
        )
        assert len(calls) == 1
        assert "parquet-zstd" in calls[0][1].name

    def test_multiple_metrics_merged_into_record(self, tmp_path, table):
        results = compare_formats(
            ParquetCpp(),
            [ParquetCpp(compression="zstd")],
            table,
            tmp_path,
            metrics=(lambda _: {"a": 1}, lambda _: {"b": 2}),
        )
        assert results[0]["a"] == 1
        assert results[0]["b"] == 2
