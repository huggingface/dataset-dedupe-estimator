"""Page parity of parquet writers with parquet-cpp.

Every writer writes the same tables, one per datatype, with the settings of each
case, and its pages are diffed with parquet-cpp's: the row group, the number of
values and the payload bytes of each page. Payloads are compared without the page
headers, which carry each writer's statistics.
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


@dataclass(frozen=True)
class Parity:
    """A writer's pages of a datatype and case against parquet-cpp's.

    Parity means no different cuts and no different pages.
    """

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
    """Write every datatype with every case and writer, compare them to parquet-cpp.

    The files are kept at directory/case/writer/datatype-params.parquet. Cases a
    writer can't express (e.g. duckdb has no row limit per page) are left out.
    Returns the results and the created_by of each writer.
    """
    assert num_rows <= MAX_ROW_GROUP_SIZE, "plain needs a single row group"
    tables = {
        datatype: DataGenerator({"x": spec}).generate_table(num_rows)
        for datatype, spec in DATATYPES.items()
    }
    # parquet-cpp is written once, as the reference
    unique: list[type[Parquet]] = list(dict.fromkeys([ParquetCpp, *writers]))
    # the written file of each writer per (case, datatype)
    futures: dict[tuple[str, str], dict[str, Future[Path]]] = {}
    with ThreadPoolExecutor(max_workers) as executor:
        for case, params in CASES.items():
            for writer in unique:
                try:
                    fmt = writer(use_cdc=cdc, **params)
                except ValueError:
                    continue
                out = directory / case / fmt.name
                out.mkdir(parents=True, exist_ok=True)
                for datatype, table in tables.items():
                    stem = re.sub(r"[^a-z0-9]+", "-", datatype).strip("-")
                    futures.setdefault((case, datatype), {})[fmt.name] = (
                        executor.submit(fmt.write, stem, table, out)
                    )
        pending = [f for written in futures.values() for f in written.values()]
        for _ in tqdm(as_completed(pending), total=len(pending)):
            pass

    created_by: dict[str, str] = {}
    results = []
    for (case, datatype), written in futures.items():
        for name, future in written.items():
            if name not in created_by and future.exception() is None:
                created_by[name] = pq.read_metadata(future.result()).created_by
        reference = written.pop(ParquetCpp.name)
        if reference.exception() is not None:
            error = reference.exception()
            message = f"parquet-cpp failed: {type(error).__name__}: {error}"
            results += [
                Parity(case, datatype, name, 0, 0, 0, 0, message) for name in written
            ]
            continue
        cpp = Layout.read(reference.result())
        for name, future in written.items():
            error = future.exception()
            if error is not None:
                message = f"{type(error).__name__}: {error}"
                results.append(Parity(case, datatype, name, 0, 0, 0, 0, message))
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
