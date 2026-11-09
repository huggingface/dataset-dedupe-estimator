import pyarrow.parquet as pq
import pytest

from de import display
from de.formats import CdcParams, ParquetCpp, ParquetRs
from de.parity import CASES, DATATYPES, parity

CDC = CdcParams(min_chunk_size=8 * 1024, max_chunk_size=32 * 1024, norm_level=0)
# more than two row groups of the row-groups case
NUM_ROWS = 20_000
# brackets, which rich would read as markup
ERROR = "cannot write [/tmp/x]"


class Failing(ParquetRs):
    """A writer without a row limit per page that fails every write."""

    name = "failing"
    page_row_limit = False

    def _rewrite(self, src, dest):
        raise RuntimeError(ERROR)


@pytest.fixture(scope="module")
def directory(tmp_path_factory):
    return tmp_path_factory.mktemp("parity")


@pytest.fixture(scope="module")
def report(directory):
    return parity(directory, [ParquetRs, Failing], num_rows=NUM_ROWS, cdc=CDC)


def test_every_datatype_and_case(report):
    cells = {
        (r.case, r.datatype) for r in report["results"] if r.writer == "parquet-rs"
    }
    assert cells == {(case, datatype) for case in CASES for datatype in DATATYPES}
    assert set(report["created_by"]) == {"parquet-cpp", "parquet-rs"}


def test_cases_are_written_with_their_settings(directory, report):
    for writer in ("parquet-cpp", "parquet-rs"):
        [plain] = (directory / "plain" / writer).glob("int-*.parquet")
        [row_groups] = (directory / "row-groups" / writer).glob("int-*.parquet")
        assert pq.read_metadata(plain).num_row_groups == 1
        assert pq.read_metadata(row_groups).num_row_groups == 3


def test_plain_pages_are_identical(report):
    plain = [
        r for r in report["results"] if r.writer == "parquet-rs" and r.case == "plain"
    ]
    assert len(plain) == len(DATATYPES)
    for result in plain:
        assert result.pages > 0
        assert (result.different_cuts, result.different_pages) == (0, 0)
        assert result.empty_pages == 0
        assert result.error is None


def test_codecs_differ_in_bytes_only(report):
    [snappy] = [
        r
        for r in report["results"]
        if (r.writer, r.case, r.datatype) == ("parquet-rs", "snappy", "int")
    ]
    assert snappy.different_cuts == 0
    assert snappy.different_pages == snappy.pages


def test_failures_and_cases_a_writer_cant_express(report):
    failing = [r for r in report["results"] if r.writer == "failing"]
    assert {r.case for r in failing} == set(CASES) - {"row-limit"}
    assert {r.error for r in failing} == {f"RuntimeError: {ERROR}"}


def test_errors_are_shown_once_per_writer(report, capsys):
    display.print_parity(report)
    output = capsys.readouterr().out
    cells = (len(CASES) - 1) * len(DATATYPES)
    assert output.count(f"failing {cells} cells: RuntimeError: {ERROR}") == 1


def test_parquet_cpp_failures(tmp_path, monkeypatch):
    class FailingCpp(ParquetCpp):
        def _write(self, src, dest):
            raise RuntimeError(ERROR)

    monkeypatch.setattr("de.parity.ParquetCpp", FailingCpp)
    report = parity(tmp_path, [ParquetRs], num_rows=1000, cdc=CDC)
    assert len(report["results"]) == len(CASES) * len(DATATYPES)
    assert {r.error for r in report["results"]} == {
        f"parquet-cpp failed: RuntimeError: {ERROR}"
    }
