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
from pathlib import Path
import re
from typing import Any, Optional

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


@dataclass(frozen=True)
class Parity:
    """A writer's pages of a datatype, case and page version against parquet-cpp's.

    Parity means no different cuts and no different pages.
    """

    page_version: str
    case: str
    datatype: str
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


def parity(
    directory: Path,
    writers: list[type[Parquet]],
    num_rows: int,
    cdc: CdcParams,
    max_workers: Optional[int] = None,
) -> dict:
    """Write every datatype with every case, page version and writer, compare them
    to parquet-cpp.

    The files are kept at directory/page_version/case/writer/datatype-params.parquet.
    Cases a writer can't express (e.g. duckdb has no row limit per page) are left
    out. Returns the results and the created_by of each writer.
    """
    assert num_rows <= MAX_ROW_GROUP_SIZE, "plain needs a single row group"
    tables = {
        datatype: DataGenerator({"x": spec}).generate_table(num_rows)
        for datatype, spec in DATATYPES.items()
    }
    # parquet-cpp is written once, as the reference
    unique: list[type[Parquet]] = list(dict.fromkeys([ParquetCpp, *writers]))
    # the written file of each writer per (page version, case, datatype)
    futures: dict[tuple[str, str, str], dict[str, Future[Path]]] = {}
    with ThreadPoolExecutor(max_workers) as executor:
        for page_version, data_page_version in PAGE_VERSIONS.items():
            for case, params in CASES.items():
                for writer in unique:
                    try:
                        fmt = writer(
                            use_cdc=cdc, data_page_version=data_page_version, **params
                        )
                    except ValueError:
                        continue
                    out = directory / page_version / case / fmt.name
                    out.mkdir(parents=True, exist_ok=True)
                    for datatype, table in tables.items():
                        stem = re.sub(r"[^a-z0-9]+", "-", datatype).strip("-")
                        key = (page_version, case, datatype)
                        futures.setdefault(key, {})[fmt.name] = executor.submit(
                            fmt.write, stem, table, out
                        )
        pending = [f for written in futures.values() for f in written.values()]
        for _ in tqdm(as_completed(pending), total=len(pending)):
            pass

    created_by: dict[str, str] = {}
    results = []
    for (page_version, case, datatype), written in futures.items():
        for name, future in written.items():
            if name not in created_by and future.exception() is None:
                created_by[name] = pq.read_metadata(future.result()).created_by
        reference = written.pop(ParquetCpp.name)
        if reference.exception() is not None:
            error = reference.exception()
            message = f"parquet-cpp failed: {type(error).__name__}: {error}"
            results += [
                Parity(page_version, case, datatype, name, 0, 0, 0, 0, message)
                for name in written
            ]
            continue
        cpp = Layout.read(reference.result())
        for name, future in written.items():
            error = future.exception()
            if error is not None:
                message = f"{type(error).__name__}: {error}"
                results.append(
                    Parity(page_version, case, datatype, name, 0, 0, 0, 0, message)
                )
                continue
            layout = Layout.read(future.result())
            pages = different_cuts = different_pages = empty_pages = 0
            for column in cpp.columns:
                different_cuts += len(set(cpp.cuts(column)) ^ set(layout.cuts(column)))
                empty_pages += layout.empty_pages(column)
                expected, actual = (
                    [
                        (p["row_group"], p["num_values"], p["payload_hash"])
                        for p in side.column_pages(column)
                    ]
                    for side in (cpp, layout)
                )
                # a diff, so an extra or a missing page doesn't shift the rest
                matcher = SequenceMatcher(None, expected, actual, autojunk=False)
                longer = max(len(expected), len(actual))
                pages += longer
                different_pages += longer - sum(
                    block.size for block in matcher.get_matching_blocks()
                )
            results.append(
                Parity(
                    page_version,
                    case,
                    datatype,
                    name,
                    pages,
                    different_cuts,
                    different_pages,
                    empty_pages,
                    error=None,
                )
            )
    return dict(created_by=created_by, results=results)
