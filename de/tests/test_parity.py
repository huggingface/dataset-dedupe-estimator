import io

import pyarrow.parquet as pq
import pytest
from rich.console import Console

from de import display
from de.formats import CdcParams, ParquetCpp, ParquetRs
from de.pages import Layout
from de.parity import (
    CASES,
    DATATYPES,
    PAGE_VERSIONS,
    Cell,
    ParityCheck,
    datatype_label,
)

CDC = CdcParams(min_chunk_size=8 * 1024, max_chunk_size=32 * 1024, norm_level=0)
# more than two row groups of the row-groups case
NUM_ROWS = 20_000
# brackets, which rich would read as markup
ERROR = "cannot write [/tmp/x]"
# a flat, a variable length, a nested and an arrow-only type
TYPES = ["int64", "string", "list<int>", "largestring"]
LABELS = [datatype_label(t, n) for t, n in DATATYPES if t in TYPES]


class Failing(ParquetRs):
    """A writer without a row limit per page, v2 pages nor arrow input that fails
    every write."""

    name = "failing"
    page_row_limit = False
    data_page_v2 = False
    arrow_input = False

    def _write(self, src, dest):
        raise RuntimeError(ERROR)


@pytest.fixture(scope="module")
def directory(tmp_path_factory):
    return tmp_path_factory.mktemp("parity")


@pytest.fixture(scope="module")
def report(directory):
    return ParityCheck(directory, [ParquetRs, Failing], NUM_ROWS, CDC, TYPES).run()


def test_every_page_version_datatype_and_case(report):
    assert report.writers == ["parquet-rs", "failing"]
    cells = {r.cell for r in report.results if r.writer == "parquet-rs"}
    assert cells == {
        Cell(page_version, case, label)
        for page_version in PAGE_VERSIONS
        for case in CASES
        for label in LABELS
    }
    assert Cell("v1", "plain", "int64 10% nulls") in cells
    assert set(report.created_by) == {"parquet-cpp", "parquet-rs"}


def test_cases_are_written_with_their_settings(directory, report):
    for writer in ("parquet-cpp", "parquet-rs"):
        [plain] = (directory / "v1" / "plain" / writer).glob("int64-cdc-*.parquet")
        row_groups_dir = directory / "v1" / "row-groups" / writer
        [row_groups] = row_groups_dir.glob("int64-cdc-*.parquet")
        assert pq.read_metadata(plain).num_row_groups == 1
        assert pq.read_metadata(row_groups).num_row_groups == 3


def test_page_versions_are_written_with_their_data_pages(directory, report):
    for page_version in PAGE_VERSIONS:
        for writer in ("parquet-cpp", "parquet-rs"):
            plain = directory / page_version / "plain" / writer
            [path] = plain.glob("int64-cdc-*.parquet")
            page_types = {page["page_type"] for page in Layout.read(path).pages}
            assert page_types == {f"data_page_{page_version}"}


def test_plain_pages_are_identical(report):
    plain = [
        r for r in report.results if r.writer == "parquet-rs" and r.cell.case == "plain"
    ]
    assert len(plain) == len(PAGE_VERSIONS) * len(LABELS)
    for result in plain:
        assert result.pages > 0
        assert (result.different_cuts, result.different_pages) == (0, 0)
        assert result.empty_pages == 0
        assert result.error is None


def test_codecs_differ_in_bytes_only(report):
    snappy = report.get("parquet-rs", "v1", "snappy", "int64")
    assert snappy is not None
    assert snappy.different_cuts == 0
    assert snappy.different_pages == snappy.pages


def test_failures_and_cases_a_writer_cant_express(report):
    failing = [r for r in report.results if r.writer == "failing"]
    assert {(r.cell.page_version, r.cell.case) for r in failing} == {
        ("v1", case) for case in CASES if case != "row-limit"
    }
    assert {r.error for r in failing} == {f"RuntimeError: {ERROR}"}
    assert report.get("failing", "v1", "row-limit", "int64") is None
    assert report.get("failing", "v2", "plain", "int64") is None
    # an arrow-only type, which a writer without arrow input never sees
    assert report.get("failing", "v1", "plain", "largestring") is None
    assert report.get("parquet-rs", "v1", "plain", "largestring") is not None


def test_errors_are_shown_once_per_writer(report):
    cells = sum(r.writer == "failing" for r in report.results)
    assert len(report.errors()["failing", f"RuntimeError: {ERROR}"]) == cells
    console = Console(file=io.StringIO(), width=200)
    display.print_parity(report, console)
    output = console.file.getvalue()
    assert output.count(f"failing {cells} cells: RuntimeError: {ERROR}") == 1


def test_parquet_cpp_failures(tmp_path, monkeypatch):
    class FailingCpp(ParquetCpp):
        def _write(self, src, dest):
            raise RuntimeError(ERROR)

    monkeypatch.setattr("de.parity.ParquetCpp", FailingCpp)
    report = ParityCheck(tmp_path, [ParquetRs], 1000, CDC, TYPES).run()
    assert len(report.results) == len(PAGE_VERSIONS) * len(CASES) * len(LABELS)
    assert {r.error for r in report.results} == {
        f"parquet-cpp failed: RuntimeError: {ERROR}"
    }
