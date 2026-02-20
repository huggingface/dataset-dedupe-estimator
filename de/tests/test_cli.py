import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from de.cli import cli


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def capture_results(monkeypatch):
    captured = {}

    def fake_print_table(results):
        captured["results"] = results

    monkeypatch.setattr("de.cli.display.print_table", fake_print_table)
    monkeypatch.setattr("de.cli.display.plot_bars", lambda *a, **kw: None)
    return captured


class TestSyntheticCommand:
    def test_basic_run(self, runner, tmp_path, capture_results):
        result = runner.invoke(
            cli,
            [
                "synthetic",
                "--target-dir",
                str(tmp_path),
                "--size",
                "1",
                "--num-edits",
                "1",
                "--edit-size",
                "10",
                "--no-sanity-check",
                '{"a": "int"}',
            ],
        )
        assert result.exit_code == 0, result.output
        rows = capture_results["results"]
        assert len(rows) > 0
        assert all(hasattr(r, "format") for r in rows)
        assert all(hasattr(r, "dedup_ratio") for r in rows)
        assert all(0 < r.dedup_ratio <= 1 for r in rows)

    def test_variants_present(self, runner, tmp_path, capture_results):
        runner.invoke(
            cli,
            [
                "synthetic",
                "--target-dir",
                str(tmp_path),
                "--size",
                "1",
                "--num-edits",
                "2",
                "--no-sanity-check",
                '{"a": "int"}',
            ],
        )
        variants = {r.group for r in capture_results["results"]}
        assert len(variants) > 1

    def test_multi_column_schema(self, runner, tmp_path, capture_results):
        result = runner.invoke(
            cli,
            [
                "synthetic",
                "--target-dir",
                str(tmp_path),
                "--size",
                "1",
                "--num-edits",
                "1",
                "--no-sanity-check",
                '{"a": "int", "b": "str"}',
            ],
        )
        assert result.exit_code == 0, result.output
        assert len(capture_results["results"]) > 0


class TestParamImpactCommand:
    @pytest.fixture
    def parquet_file(self, tmp_path):
        table = pa.table({"a": list(range(1000)), "b": ["x"] * 1000})
        path = tmp_path / "input.parquet"
        pq.write_table(table, path)
        return path

    def test_row_group_size(self, runner, tmp_path, parquet_file):
        result = runner.invoke(
            cli,
            ["param-impact", str(parquet_file), str(tmp_path), "--row-group-size"],
        )
        assert result.exit_code == 0, result.output

    def test_data_page_size(self, runner, tmp_path, parquet_file):
        result = runner.invoke(
            cli,
            ["param-impact", str(parquet_file), str(tmp_path), "--data-page-size"],
        )
        assert result.exit_code == 0, result.output

    def test_no_flag_exits_with_error(self, runner, tmp_path, parquet_file):
        result = runner.invoke(
            cli,
            ["param-impact", str(parquet_file), str(tmp_path)],
        )
        assert result.exit_code != 0

    def test_plot_shown_with_flag(self, runner, tmp_path, parquet_file, monkeypatch):
        calls = []
        monkeypatch.setattr(
            "de.cli.display.plot_bars", lambda *a, **kw: calls.append(a)
        )
        result = runner.invoke(
            cli,
            [
                "--plot",
                "param-impact",
                str(parquet_file),
                str(tmp_path),
                "--row-group-size",
            ],
        )
        assert result.exit_code == 0, result.output
        assert len(calls) == 1


class TestStatsCommand:
    @pytest.fixture
    def parquet_dir(self, tmp_path):
        table = pa.table({"a": list(range(100)), "b": ["x"] * 100})
        pq.write_table(table, tmp_path / "v1.parquet")
        pq.write_table(table, tmp_path / "v2.parquet")
        return tmp_path

    def test_basic_run(self, runner, parquet_dir, capture_results):
        result = runner.invoke(
            cli,
            [
                "stats",
                "--no-sanity-check",
                str(parquet_dir),
            ],
        )
        assert result.exit_code == 0, result.output
        rows = capture_results["results"]
        assert len(rows) > 0
        assert all(hasattr(r, "format") for r in rows)
        assert all(hasattr(r, "dedup_ratio") for r in rows)
        assert all(r.numfiles == 2 for r in rows)

    def test_with_row_group_size(self, runner, parquet_dir, capture_results):
        result = runner.invoke(
            cli,
            [
                "stats",
                "--no-sanity-check",
                "--row-group-size",
                "50",
                str(parquet_dir),
            ],
        )
        assert result.exit_code == 0, result.output
        assert len(capture_results["results"]) > 0
