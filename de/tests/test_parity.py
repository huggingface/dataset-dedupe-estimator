import pytest

from de.formats import CdcParams, ParquetRs
from de.parity import CASES, DATATYPES, parity

CDC = CdcParams(min_chunk_size=8 * 1024, max_chunk_size=32 * 1024, norm_level=0)


class Failing(ParquetRs):
    """A writer without a row limit per page that fails every write."""

    name = "failing"
    page_row_limit = False

    def _rewrite(self, src, dest):
        raise RuntimeError("cannot write")


@pytest.fixture(scope="module")
def report(tmp_path_factory):
    directory = tmp_path_factory.mktemp("parity")
    return parity(directory, [ParquetRs, Failing], num_rows=5000, cdc=CDC)


def test_every_datatype_and_case(report):
    cells = {
        (r.case, r.datatype) for r in report["results"] if r.writer == "parquet-rs"
    }
    assert cells == {(case, datatype) for case in CASES for datatype in DATATYPES}
    assert set(report["created_by"]) == {"parquet-cpp", "parquet-rs"}


def test_plain_pages_are_identical(report):
    plain = [
        r for r in report["results"] if r.writer == "parquet-rs" and r.case == "plain"
    ]
    assert len(plain) == len(DATATYPES)
    for result in plain:
        assert result.pages > 0
        assert (result.different_cuts, result.different_pages) == (0, 0)
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
    assert {r.error for r in failing} == {"RuntimeError: cannot write"}
