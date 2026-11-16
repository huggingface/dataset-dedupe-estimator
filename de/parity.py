"""Page parity of parquet writers with parquet-cpp.

Every writer writes the same tables, one per datatype, with the settings of each
case and both data page versions, and its pages are diffed with parquet-cpp's: the
row group, the number of values and the payload bytes of each page. The page
headers are left out, they differ for the reasons below. With v2 data pages every
writer uses the encodings of writer version 2.0 (see de.formats.Parquet), the only
ones parquet-java writes v2 pages with.

The parquet spec says nothing about content defined chunking, nor about writers
writing the same bytes, so the known differences are all valid parquet. Payloads,
counted as different pages with the same cuts:

- level encoding: parquet-java bit-packs runs of fewer than 8 repeated levels where
  parquet-cpp writes RLE runs, the decoded levels are the same. Mostly in nested
  columns, and in pages of a few values of flat ones.
- dictionary: parquet-java falls back to PLAIN for the whole column chunk unless the
  dictionary and the indices of its first page are smaller than PLAIN, parquet-cpp
  keeps the dictionary until the dictionary page reaches 1 MiB.
- snappy: the decompressed payloads are the same, the libraries differ: snappy-java
  1.1.10 in parquet-java, a newer snappy bundled with parquet-cpp and the pure Rust
  snap crate in parquet-rs.
- zstd: parquet-java compresses with level 3 instead of 1, with the streaming API
  (no content size in the frame header) and flushes before closing (an empty last
  block).

Payloads of v2 data pages only:

- delta encoding: parquet-java's DELTA_BINARY_PACKED has blocks of 128 values for
  INT64 where the others have 256, and leaves stale bits in the padding and in the
  bit widths of the unused miniblocks of the last block, which the spec says should
  be zero. So its integer and string pages all differ.
- dictionary: pyarrow can't combine column_encoding with dictionary encoding, so
  parquet-cpp falls back to PLAIN, the others to delta encodings.
- compression: v2 compresses the values only. parquet-cpp leaves them uncompressed
  unless that makes them smaller, parquet-rs unless they get smaller than the levels
  and the values together, parquet-java always compresses them.

Page headers, which aren't compared:

- statistics: parquet-cpp writes them into every data page header unless it writes
  a page index, as an empty struct with write_statistics=False, so ParquetCpp
  writes a page index. parquet-rs and parquet-java write none.
- CRC: parquet-java writes one by default, ParquetRewrite.java turns it off.
- parquet-java labels the missing repetition levels of flat columns BIT_PACKED, the
  others RLE.
- parquet-java's writer version 1.0 encodes dictionary pages and indices as the
  deprecated PLAIN_DICTIONARY and leaves out is_sorted, the others use PLAIN and
  RLE_DICTIONARY.
- parquet-java marks the values of every v2 data page compressed (is_compressed),
  even in uncompressed files.

Deduplication across writers needs the same bytes, headers included: xet chunks
(64 KiB on average) span several pages, so a single different header byte makes a
chunk new. parquet-cpp and parquet-rs write the same bytes with both page versions,
parquet-java only with v1 pages of nested columns where the level encoding agrees.
"""

from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from difflib import SequenceMatcher
from functools import cached_property
from pathlib import Path
import re
from typing import Any, Iterator, NamedTuple, Optional

import pyarrow.parquet as pq
from tqdm import tqdm

from .bench import MAX_ROW_GROUP_SIZE, NO_PAGE_LIMITS
from .formats import CdcParams, Parquet, ParquetCpp
from .pages import Layout
from .synthetic import DataGenerator

# a single leaf column each
DATATYPES: dict[str, Any] = {
    "int": "int",
    "int64": "int64",
    "float": "float",
    "bool": "bool",
    "str": "str",
    "largestr": "largestr",
    "zeros": "zeros",
    "list<int>": ["int"],
    "list<str>": ["str"],
    "list<list<int>>": [["int"]],
    "struct<float>": {"x": "float"},
    "struct<list<int>>": {"x": ["int"]},
    "list<struct<int>>": [{"x": "int"}],
}

# PLAIN and uncompressed in a single row group, pages end at CDC chunk boundaries
PLAIN: dict[str, Any] = dict(
    use_dictionary=False, row_group_size=MAX_ROW_GROUP_SIZE, **NO_PAGE_LIMITS
)
# each case changes one setting of PLAIN
CASES: dict[str, dict[str, Any]] = {
    "plain": PLAIN,
    # a multiple of the 2048 row vectors duckdb rounds row groups up to
    "row-groups": dict(PLAIN, row_group_size=4 * 2048),
    # a multiple of the 1024 values mini-batches arrow-rs checks its row limit after
    "row-limit": dict(PLAIN, max_rows_per_page=1024),
    "size-limit": dict(PLAIN, data_page_size=4096),
    "dictionary": dict(PLAIN, use_dictionary=True),
    "snappy": dict(PLAIN, compression="snappy"),
    "zstd": dict(PLAIN, compression="zstd"),
}
# every case is written with both data page versions
PAGE_VERSIONS = {"v1": "1.0", "v2": "2.0"}


class Cell(NamedTuple):
    """A table every writer writes: a datatype with the settings of a page version and
    a case."""

    page_version: str
    case: str
    datatype: str


@dataclass(frozen=True)
class Parity:
    """A writer's pages of a cell against parquet-cpp's.

    Parity means no different cuts and no different pages.
    """

    cell: Cell
    writer: str
    # the dictionary and data pages of the file with more pages
    pages: int
    # page cuts in only one of the files
    different_cuts: int
    # pages of the file with more pages left unmatched by a diff of the two files'
    # pages, matching in row group, values and payload bytes (headers aside)
    different_pages: int
    # the writer's data pages without values, parquet-cpp writes none
    empty_pages: int
    # why the writer or parquet-cpp failed, e.g. a file that can't be read back
    error: Optional[str]

    @classmethod
    def compare(
        cls, cell: Cell, writer: str, reference: Layout, layout: Layout
    ) -> "Parity":
        """Diff the writer's pages of every column with parquet-cpp's."""
        pages = different_cuts = different_pages = empty_pages = 0
        for column in reference.columns:
            cuts = set(reference.cuts(column)) ^ set(layout.cuts(column))
            different_cuts += len(cuts)
            empty_pages += layout.empty_pages(column)
            expected, actual = (
                [
                    (p["row_group"], p["num_values"], p["payload_hash"])
                    for p in side.column_pages(column)
                ]
                for side in (reference, layout)
            )
            # a diff, so an extra or a missing page doesn't shift the rest
            matcher = SequenceMatcher(None, expected, actual, autojunk=False)
            longer = max(len(expected), len(actual))
            pages += longer
            different_pages += longer - sum(
                block.size for block in matcher.get_matching_blocks()
            )
        return cls(
            cell, writer, pages, different_cuts, different_pages, empty_pages, None
        )

    @classmethod
    def failed(
        cls, cell: Cell, writer: str, error: BaseException, prefix: str = ""
    ) -> "Parity":
        """A writer's parity that can't be told, as the writer or parquet-cpp failed."""
        message = f"{prefix}{type(error).__name__}: {error}"
        return cls(cell, writer, 0, 0, 0, 0, message)


@dataclass(frozen=True)
class ParityReport:
    """The parity of every writer per cell."""

    # the created_by of each writer's files, parquet-cpp's included
    created_by: dict[str, str]
    results: list[Parity]

    @property
    def writers(self) -> list[str]:
        return list(dict.fromkeys(result.writer for result in self.results))

    @cached_property
    def _results(self) -> dict[tuple[str, Cell], Parity]:
        return {(result.writer, result.cell): result for result in self.results}

    def get(
        self, writer: str, page_version: str, case: str, datatype: str
    ) -> Optional[Parity]:
        """A writer's parity of a cell, None if the writer can't express the case."""
        return self._results.get((writer, Cell(page_version, case, datatype)))

    def errors(self) -> dict[tuple[str, str], list[Parity]]:
        """The failed results per writer and the first line of their error.

        A writer usually fails many cells the same way, e.g. without
        PARQUET_JAVA_CLASSPATH.
        """
        errors: dict[tuple[str, str], list[Parity]] = {}
        for result in self.results:
            if result.error is not None:
                line = result.error.strip().splitlines()[0]
                errors.setdefault((result.writer, line), []).append(result)
        return errors


class ParityCheck:
    """Writes every datatype with every page version, case and writer, and compares
    each writer's pages with parquet-cpp's.

    The files are kept at directory/page_version/case/writer/datatype-params.parquet.
    """

    def __init__(
        self,
        directory: Path,
        writers: list[type[Parquet]],
        num_rows: int,
        cdc: CdcParams,
    ):
        assert num_rows <= MAX_ROW_GROUP_SIZE, "plain needs a single row group"
        self.directory = directory
        # parquet-cpp is written once, as the reference
        self.writers: list[type[Parquet]] = list(dict.fromkeys([ParquetCpp, *writers]))
        self.num_rows = num_rows
        self.cdc = cdc

    def formats(self) -> Iterator[tuple[str, str, Parquet]]:
        """Each writer's format of every page version and case, except the cases a
        writer can't express (e.g. duckdb has no row limit per page)."""
        for page_version, data_page_version in PAGE_VERSIONS.items():
            for case, params in CASES.items():
                for writer in self.writers:
                    try:
                        fmt = writer(
                            use_cdc=self.cdc,
                            data_page_version=data_page_version,
                            **params,
                        )
                    except ValueError:
                        continue
                    yield page_version, case, fmt

    def run(self, max_workers: Optional[int] = None) -> ParityReport:
        """Write every table with every format and compare them with parquet-cpp's."""
        return self._compare(self._write(max_workers))

    def _write(self, max_workers: Optional[int]) -> dict[Cell, dict[str, Future[Path]]]:
        """Write every table with every format, the written file of each cell per
        writer."""
        tables = {
            datatype: DataGenerator({"x": spec}).generate_table(self.num_rows)
            for datatype, spec in DATATYPES.items()
        }
        written: dict[Cell, dict[str, Future[Path]]] = {}
        with ThreadPoolExecutor(max_workers) as executor:
            for page_version, case, fmt in self.formats():
                out = self.directory / page_version / case / fmt.name
                out.mkdir(parents=True, exist_ok=True)
                for datatype, table in tables.items():
                    stem = re.sub(r"[^a-z0-9]+", "-", datatype).strip("-")
                    cell = Cell(page_version, case, datatype)
                    written.setdefault(cell, {})[fmt.name] = executor.submit(
                        fmt.write, stem, table, out
                    )
            futures = [f for files in written.values() for f in files.values()]
            for _ in tqdm(as_completed(futures), total=len(futures)):
                pass
        return written

    def _compare(self, written: dict[Cell, dict[str, Future[Path]]]) -> ParityReport:
        """Compare the files of each cell with parquet-cpp's."""
        created_by: dict[str, str] = {}
        results = []
        for cell, files in written.items():
            for name, future in files.items():
                if name not in created_by and future.exception() is None:
                    created_by[name] = pq.read_metadata(future.result()).created_by
            reference = files[ParquetCpp.name]
            others = {name: f for name, f in files.items() if name != ParquetCpp.name}
            error = reference.exception()
            if error is not None:
                prefix = "parquet-cpp failed: "
                results += [Parity.failed(cell, name, error, prefix) for name in others]
                continue
            cpp = Layout.read(reference.result())
            for name, future in others.items():
                error = future.exception()
                if error is not None:
                    results.append(Parity.failed(cell, name, error))
                else:
                    layout = Layout.read(future.result())
                    results.append(Parity.compare(cell, name, cpp, layout))
        return ParityReport(created_by, results)
