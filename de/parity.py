"""Page parity of parquet writers with parquet-cpp.

Every writer writes the same tables, one per datatype, with the settings of each
case, and its pages are compared to parquet-cpp's: the cuts, then the payloads.
Payloads are compared without the page headers, which carry each writer's
statistics.
"""

from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
import re
from typing import Any, Optional

import pyarrow.parquet as pq
from tqdm import tqdm

from .bench import MAX_ROW_GROUP_SIZE, NO_PAGE_LIMITS
from .formats import CdcParams, Parquet, ParquetCpp
from .pages import Layout, compare
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
    # parquet-cpp's dictionary and data pages
    pages: int
    # page cuts in only one of the files
    different_cuts: int
    # the writer's pages without a byte-identical payload in parquet-cpp's file
    different_pages: int
    # why the writer failed, e.g. it wrote a file that can't be read back
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
    futures: dict[tuple[str, str, str], Future[Path]] = {}
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
                    futures[case, datatype, fmt.name] = executor.submit(
                        fmt.write, stem, table, out
                    )
        for _ in tqdm(as_completed(futures.values()), total=len(futures)):
            pass

    created_by: dict[str, str] = {}
    results = []
    for (case, datatype, name), future in futures.items():
        error = future.exception()
        if error is None:
            created_by.setdefault(name, pq.read_metadata(future.result()).created_by)
        if name == ParquetCpp.name:
            continue
        cpp = Layout.read(futures[case, datatype, ParquetCpp.name].result())
        pages = sum(len(cpp.column_pages(column)) for column in cpp.columns)
        if error is not None:
            message = f"{type(error).__name__}: {error}"
            results.append(Parity(case, datatype, name, pages, 0, 0, message))
            continue
        diffs = compare(cpp, Layout.read(future.result()))
        results.append(
            Parity(
                case,
                datatype,
                name,
                pages,
                different_cuts=sum(d.left_only_cuts + d.right_only_cuts for d in diffs),
                different_pages=sum(d.new_payloads for d in diffs),
                error=None,
            )
        )
    return dict(created_by=created_by, results=results)
