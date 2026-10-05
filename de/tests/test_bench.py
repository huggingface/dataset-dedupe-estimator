from collections import Counter
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from de.bench import (
    DEFAULT_WRITERS,
    WRITERS,
    compare_runs,
    load_sources,
    matrix,
    prepare_sources,
    run,
)
from de.formats import CdcParams

CDC = CdcParams(min_chunk_size=8 * 1024, max_chunk_size=32 * 1024, norm_level=0)
SCHEMA = {"a": "int"}
NUM_ROWS = 20_000
DEFAULTS = [WRITERS[name] for name in DEFAULT_WRITERS]
EDIT_POINTS = [0.25, 0.75]


def small_matrix(writers, num_rows=NUM_ROWS):
    return matrix(writers, num_rows, CDC, "snappy", [5000], [3000])


@pytest.fixture(scope="module")
def sources(tmp_path_factory):
    directory = tmp_path_factory.mktemp("sources")
    prepare_sources(directory, SCHEMA, NUM_ROWS, EDIT_POINTS, edit_size=10)
    return directory


@pytest.fixture(scope="module")
def first_run(sources, tmp_path_factory):
    run_dir = tmp_path_factory.mktemp("run")
    return run_dir, run(run_dir, sources, small_matrix(DEFAULTS))


class TestSources:
    def test_original_and_edited_versions(self, sources):
        manifest = load_sources(sources)
        num_rows = {name: f["num_rows"] for name, f in manifest["files"].items()}
        assert num_rows == {"original": 20_000, "inserted": 20_020, "deleted": 19_980}
        assert manifest["edit_rows"] == [5000, 15000]
        for entry in manifest["files"].values():
            metadata = pq.read_metadata(sources / entry["path"])
            assert metadata.num_rows == entry["num_rows"]

    def test_reused_with_same_parameters(self, sources):
        before = {p.name: p.stat().st_mtime_ns for p in sources.iterdir()}
        prepare_sources(sources, SCHEMA, NUM_ROWS, EDIT_POINTS, edit_size=10)
        assert {p.name: p.stat().st_mtime_ns for p in sources.iterdir()} == before

    def test_different_parameters_are_rejected(self, sources):
        with pytest.raises(ValueError, match="different parameters"):
            prepare_sources(sources, SCHEMA, 10_000, EDIT_POINTS, edit_size=10)

    def test_modified_files_are_rejected(self, tmp_path):
        prepare_sources(tmp_path, SCHEMA, 1000, [0.5], edit_size=10)
        original = tmp_path / "original.parquet"
        pq.write_table(pq.read_table(original).slice(1), original)
        with pytest.raises(ValueError, match="doesn't match its manifest"):
            load_sources(tmp_path)

    def test_single_edit_shifts_the_rest(self, tmp_path):
        manifest = prepare_sources(tmp_path, SCHEMA, 1000, [0.25], edit_size=10)
        assert manifest["edit_points"] == [0.25]
        assert manifest["edit_rows"] == [250]
        original = pq.read_table(tmp_path / "original.parquet")
        inserted = pq.read_table(tmp_path / "inserted.parquet")
        deleted = pq.read_table(tmp_path / "deleted.parquet")
        # the rows after the edit are shifted by the edit size
        assert inserted.slice(0, 250).equals(original.slice(0, 250))
        assert inserted.slice(260).equals(original.slice(250))
        assert deleted.slice(0, 240).equals(original.slice(0, 240))
        assert deleted.slice(240).equals(original.slice(250))


class TestMatrix:
    def test_profiles(self):
        profiles = matrix(DEFAULTS, NUM_ROWS, CDC, "snappy", [5000, 2000], [3000])
        counts = {profile: len(formats) for profile, formats in profiles.items()}
        assert counts == {"cut": 8, "aligned": 4, "default": 6, "dict": 2, "nocdc": 2}
        for formats in profiles.values():
            assert len({(f.name, f.paramstem) for f in formats}) == len(formats)
        oracle = profiles["cut"][0]
        assert (oracle.row_group_size, oracle.batch_size) == (NUM_ROWS, None)
        assert all(f.use_dictionary for f in profiles["dict"])
        assert not any(f.use_dictionary for f in profiles["default"])
        assert not any(f.use_cdc for f in profiles["nocdc"])

    def test_oracle_needs_a_single_row_group(self):
        with pytest.raises(AssertionError):
            matrix(DEFAULTS, 65 * 2**20, CDC, "snappy", [2**20], [])

    def test_writers_without_streaming(self):
        profiles = matrix(
            [WRITERS["parquet-java"]],
            NUM_ROWS,
            CDC,
            "snappy",
            [5000],
            [3000],
            ["default"],
        )
        assert [f.batch_size for f in profiles["default"]] == [None]

    def test_selected_profiles(self):
        # deduplication profiles don't need an oracle, so any size is fine
        profiles = matrix(
            DEFAULTS,
            200 * 2**20,
            CDC,
            "snappy",
            [2**24, 2**20],
            [100_000],
            profiles=["default", "nocdc"],
        )
        assert {profile: len(f) for profile, f in profiles.items()} == {
            "default": 6,
            "nocdc": 2,
        }

    def test_run_selected_profiles(self, sources, tmp_path):
        profiles = matrix(
            [WRITERS["parquet-rs"]], NUM_ROWS, CDC, "snappy", [5000], [], ["nocdc"]
        )
        results = run(tmp_path, sources, profiles, max_workers=2)
        assert {(c["profile"], c["edit"]) for c in results["cells"]} == {
            ("nocdc", "inserted"),
            ("nocdc", "deleted"),
        }


class TestRun:
    def test_results_are_saved(self, first_run):
        run_dir, results = first_run
        assert json.loads((run_dir / "results.json").read_text()) == results
        environment = results["environment"]
        assert environment["pyarrow"] == pa.__version__
        assert set(environment["created_by"]) == set(DEFAULT_WRITERS)
        for cell in results["cells"]:
            for path in cell["files"]:
                assert (run_dir / path).exists()

    def test_cells(self, first_run):
        _, results = first_run
        counts = Counter(cell["profile"] for cell in results["cells"])
        assert counts == {"cut": 6, "aligned": 4, "default": 8, "dict": 4, "nocdc": 4}
        for cell in results["cells"]:
            if cell["profile"] == "cut":
                assert len(cell["persistence"]) == 1
            elif cell["profile"] != "aligned":
                assert cell["edit"] in ("inserted", "deleted")
                assert 0.5 <= cell["dedup"]["dedup_ratio"] <= 1
                assert len(cell["files"]) == 2
                assert cell["empty_pages"] >= 0

    def test_arrow_rs_keeps_cdc_state(self, first_run):
        _, results = first_run
        for cell in results["cells"]:
            if cell["profile"] == "cut" and cell["writer"] == "parquet-rs":
                [result] = cell["persistence"]
                assert (result["recall"], result["extra"], result["aligned"]) == (
                    1.0,
                    0,
                    1.0,
                )

    def test_oracle_cuts_agree_across_implementations(self, first_run):
        run_dir, _ = first_run
        [row] = [
            row
            for row in compare_runs([run_dir])["cross_impl"]
            if row["profile"] == "cut" and row["layout"] == "rg=20000"
        ]
        assert row["pair"] == "parquet-cpp vs parquet-rs"
        assert row["common"] > 0
        assert (row["left_only"], row["right_only"]) == (0, 0)


class TestCompareRuns:
    def test_runs_from_the_same_sources(self, sources, first_run, tmp_path):
        first, _ = first_run
        second = tmp_path / "second"
        run(second, sources, small_matrix([WRITERS["parquet-cpp"]]))
        report = compare_runs([first, second])
        assert [r["label"] for r in report["runs"]] == [str(first), str(second)]

        rows = [row for row in report["persistence"] if row["run"] == str(second)]
        assert rows and {row["versus"] for row in rows} == {"identical"}
        # within a layout section the rows are ordered by writer, then by run
        section = [
            (row["writer"], row["run"])
            for row in report["persistence"]
            if row["layout"] == "rg=5000"
        ]
        assert section == [
            ("parquet-cpp", str(first)),
            ("parquet-cpp", str(second)),
            ("parquet-rs", str(first)),
        ]
        assert all(0 <= row["new_bytes"] <= 1 for row in report["dedup"])

        # the second run has no arrow-rs files, it is compared to the first run's
        pairs = {(row["run"], row["pair"]) for row in report["cross_impl"]}
        assert pairs == {
            (str(first), "parquet-cpp vs parquet-rs"),
            (str(second), f"parquet-cpp vs parquet-rs of {first}"),
        }

    def test_different_sources_are_rejected(self, first_run, tmp_path):
        first, _ = first_run
        sources = tmp_path / "sources"
        prepare_sources(sources, SCHEMA, 10_000, EDIT_POINTS, edit_size=10)
        other = tmp_path / "other"
        run(other, sources, small_matrix([WRITERS["parquet-rs"]], num_rows=10_000))
        with pytest.raises(ValueError, match="different source files"):
            compare_runs([first, other])
