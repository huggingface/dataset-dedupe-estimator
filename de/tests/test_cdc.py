"""Content defined chunking conformance of every parquet writer.

Every writer writes the same generated single column tables with the same settings,
and the assertions only read the page layouts of the files, so parquet-cpp,
parquet-rs, parquet-java, DuckDB and parquet-go are held to one standard. No writer
is the reference: the tests state what a correct chunker writes, whether the writers
write the same pages is de parity's question.

The most important property is that a writer carries its chunker state across row
groups and write calls: a row group boundary forces a page cut, but the content
defined cuts after it stay where a single row group puts them.

parquet-java, DuckDB and parquet-go are skipped unless PARQUET_JAVA_CLASSPATH,
DUCKDB_BINARY and PARQUET_GO_BINARY are set. Settings a writer can't express are skipped, a known bug fails.
"""

from dataclasses import dataclass, replace
from functools import lru_cache
import os
from pathlib import Path
import re
from types import TracebackType
from typing import Any, Iterator

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

from de.formats import (
    NO_LIMIT,
    CdcParams,
    Parquet,
    ParquetCpp,
    ParquetDuckDB,
    ParquetGo,
    ParquetJava,
    ParquetRs,
)
from de.pages import Layout, Persistence, changes, persistence
from de.parity import DATATYPES, TYPES, generator, unsupported
from de.synthetic import evenly_spaced

CDC = CdcParams(min_chunk_size=8 * 1024, max_chunk_size=32 * 1024, norm_level=0)

# about the bytes the chunker hashes per table, which also keeps the plain values
# below the 1MiB dictionary page limit of parquet-cpp, parquet-rs and parquet-java,
# so dictionary encoding never falls back to plain
BUDGET = 512 * 1024

# the columns that never match the gear hash, only the maximum chunk size cuts them
CONSTANT = {("zeros", "0%"), ("all-null", "0%")}

# no page limits, so the pages of a single row group are the CDC chunks: a page size
# limit larger than any table, as parquet-java allocates a compressor buffer of it
PAGE_LIMIT = 64 * 2**20
BASELINE = dict(
    use_cdc=CDC,
    use_dictionary=False,
    data_page_size=PAGE_LIMIT,
    max_rows_per_page=NO_LIMIT,
)
# settings that must not move a cut, on top of the baseline
SETTINGS: dict[str, dict[str, Any]] = {
    "v1": {},
    "dict": {"use_dictionary": True},
    "v2": {"data_page_version": "2.0"},
    "v2-dict": {"data_page_version": "2.0", "use_dictionary": True},
    "snappy": {"compression": "snappy"},
}
LAYOUTS = [
    "rg-before-cut",
    "rg-at-cut",
    "rg-after-cut",
    "tiny-rgs",
    "large-rgs",
    "batches",
    "tiny-batches",
]
# edit -> (kind, positions): the generator's edits, at least two maximum chunks apart
EDITS: dict[str, tuple[str, list[float]]] = {
    "insert": ("insert", [0.5]),
    "insert-twice": ("insert", evenly_spaced(2)),
    "delete": ("delete", [0.5]),
    "delete-twice": ("delete", evenly_spaced(2)),
    "update": ("update", [0.5]),
    "update-twice": ("update", evenly_spaced(2)),
    "prepend": ("prepend", [0.0]),
    "append": ("append", [1.0]),
}
# about the bytes the chunker hashes per edit
EDIT_BYTES = 256

# the chunk sizes are checked page by page on flat columns, where a level is a row
NESTED = {
    type
    for type, spec in TYPES.items()
    if isinstance(spec, (list, dict)) or spec == "nullsegments"
}

# (min, max, norm level, accepted): a positive minimum below the maximum, and a mask
# with bits left at the norm level, as parquet-cpp's ChunkSizeParameterValidation
# checks them
OPTION_SETS = [
    (0, 32, 0, True),
    (32, 64, 0, True),
    (1, 65, 1, True),
    (0, 128, 2, True),
    (0, 16, 0, False),
    (32, 48, 0, False),
    (32, 32, 0, False),
    (1024, 512, 0, False),
    (16, 0, 0, False),
    (0, 0, 0, False),
    (1, 32, 1, False),
    (1, 33, 1, False),
    (0, 123, 2, False),
    (-16, -16, 0, False),
    (-1, 0, 0, False),
    (0, -1, 0, False),
]
# the gear hash mask depends on the chunk sizes and the norm level
PARAMETER_SIZES = [(4096, 16384), (8192, 32768), (65536, 262144)]
PARAMETER_TYPES = ["int64", "string"]
NORM_LEVELS = range(-3, 4)
# the default chunk sizes, 256KiB to 1MiB
DEFAULT_SIZES = (256 * 1024, 1024 * 1024)
DEFAULT_TYPES = ["int64", "string"]
# the chunks per table of the parameters and the defaults
CHUNKS = 25
# the bytes a chunk can run past the maximum size, until the record ends
RECORD_BYTES = 4096
WRITERS = [
    ParquetCpp,
    ParquetRs,
    pytest.param(
        ParquetJava,
        marks=pytest.mark.skipif(
            not os.environ.get("PARQUET_JAVA_CLASSPATH"),
            reason="PARQUET_JAVA_CLASSPATH is not set",
        ),
    ),
    pytest.param(
        ParquetDuckDB,
        marks=pytest.mark.skipif(
            not os.environ.get("DUCKDB_BINARY"), reason="DUCKDB_BINARY is not set"
        ),
    ),
    pytest.param(
        ParquetGo,
        marks=pytest.mark.skipif(
            not os.environ.get("PARQUET_GO_BINARY"),
            reason="PARQUET_GO_BINARY is not set",
        ),
    ),
]

PERFECT = dict(recall=1.0, extra=0, aligned=1.0, resync=0.0, empty=0)


@dataclass(frozen=True)
class Key:
    """A file of the matrix: a table written with a setting in a layout."""

    type: str
    nulls: str
    setting: str = "v1"
    table: str = "base"
    layout: str = "oracle"

    @property
    def stem(self) -> str:
        fields = (self.type, self.nulls, self.setting, self.table, self.layout)
        return re.sub(r"[^a-zA-Z0-9]+", "-", "_".join(fields)).strip("-")


@lru_cache(maxsize=256)
def read_layout(path: Path) -> Layout:
    return Layout.read(path)


def levels(table: pa.Table) -> list[int]:
    """The number of levels of each leaf column of the table."""
    out = pa.BufferOutputStream()
    pq.write_table(table, out, use_dictionary=False, row_group_size=max(1, len(table)))
    metadata = pq.ParquetFile(pa.BufferReader(out.getvalue())).metadata
    if metadata.num_row_groups == 0:
        return [0] * metadata.num_columns
    row_group = metadata.row_group(0)
    return [row_group.column(i).num_values for i in range(metadata.num_columns)]


def value_bytes(array: pa.Array) -> np.ndarray:
    """The bytes the chunker hashes for each value of a leaf array, 0 for a null:
    the value's arrow width, a boolean's byte, a dictionary's index, a binary's
    bytes."""
    if isinstance(array, pa.ExtensionArray):
        array = array.storage
    if isinstance(array, pa.DictionaryArray):
        width = array.type.index_type.bit_width // 8
    elif pa.types.is_boolean(array.type):
        width = 1
    elif array.type in (pa.binary(), pa.large_binary(), pa.string(), pa.large_string()):
        return pc.binary_length(array).fill_null(0).to_numpy()
    else:
        width = array.type.byte_width
    return np.where(array.is_valid().to_numpy(zero_copy_only=False), width, 0)


def leaves(array: pa.Array, repeated: bool = False) -> list[tuple[pa.Array, bool]]:
    """The values written to each leaf column, and whether it has repetition levels.
    A list's flatten drops the values of its null entries, a struct's merges its
    validity into the fields'."""
    if pa.types.is_list(array.type):
        return leaves(array.flatten(), True)
    if pa.types.is_struct(array.type):
        return [leaf for field in array.flatten() for leaf in leaves(field, repeated)]
    return [(array, repeated)]


class Corpus:
    """Every file of the matrix, written in one batch per writer on first use.

    A writer's oracles, single row group files without page limits whose pages are
    exactly the CDC chunks, are written first; the other layouts place their row
    groups relative to the writer's own oracle cuts.
    """

    def __init__(self, directory: Path) -> None:
        self.directory = directory
        self._tables: dict[tuple[str, str, str], pa.Table] = {}
        self._written: dict[tuple[type[Parquet], Key], Path | BaseException] = {}
        self._phases: set[tuple[type[Parquet], bool]] = set()
        self._tracebacks: dict[tuple[type[Parquet], Key], TracebackType | None] = {}
        self._bytes_per_row: dict[tuple[str, str], float] = {}
        self._first_cut_rows: dict[tuple[type[Parquet], str, str], int] = {}

    def keys(self) -> Iterator[Key]:
        for type, nulls in DATATYPES:
            for setting in SETTINGS:
                yield Key(type, nulls, setting)
                for layout in LAYOUTS:
                    yield Key(type, nulls, setting, layout=layout)
            yield Key(type, nulls, "no-cdc")
            for edit in EDITS:
                if not self._constant_update(type, nulls, edit):
                    yield Key(type, nulls, table=edit)
            for table in ("base", "insert", "delete", "update"):
                if not self._constant_update(type, nulls, table):
                    yield Key(type, nulls, table=table, layout="middle-rg")
            yield Key(type, nulls, layout="row-limit")
        for type in TYPES:
            yield Key(type, "0%", table="empty")
        for min_size, max_size, norm, _ in OPTION_SETS:
            setting = f"options={min_size},{max_size},{norm}"
            yield Key("int64", "0%", setting, table="small")
        for type in PARAMETER_TYPES:
            for min_size, max_size in PARAMETER_SIZES:
                for norm in NORM_LEVELS:
                    setting = f"cdc={min_size},{max_size},{norm}"
                    table = f"budget={min_size},{max_size}"
                    yield Key(type, "0%", setting, table)
        for type in DEFAULT_TYPES:
            yield Key(
                type, "0%", "cdc=default", table="budget={},{}".format(*DEFAULT_SIZES)
            )

    def table(self, type: str, nulls: str, name: str = "base") -> pa.Table:
        if (type, nulls, name) not in self._tables:
            self._tables[type, nulls, name] = self._generate(type, nulls, name)
        return self._tables[type, nulls, name]

    def bytes_per_row(self, type: str, nulls: str) -> float:
        """About the bytes the chunker hashes per row: the arrow bytes and the
        definition level."""
        if (type, nulls) not in self._bytes_per_row:
            sample = generator(type, nulls).generate_table(1000)
            [array] = sample.column(0).chunks
            # a dictionary array's dictionary is shared by every row
            if isinstance(array, pa.DictionaryArray):
                array = array.indices
            self._bytes_per_row[type, nulls] = array.nbytes / len(array) + 2
        return self._bytes_per_row[type, nulls]

    def format(self, writer: type[Parquet], key: Key) -> Parquet:
        """The writer's format of the key, skipping the test if it can't write it."""
        fmt = self._format(writer, key)
        if isinstance(fmt, str):
            pytest.skip(fmt)
        return fmt

    def layout(self, writer: type[Parquet], key: Key) -> Layout:
        return read_layout(self.path(writer, key))

    def path(self, writer: type[Parquet], key: Key) -> Path:
        """The file of the key, raising what writing it raised."""
        self.format(writer, key)
        oracle = key.layout == "oracle"
        if (writer, oracle) not in self._phases:
            self._write(writer, oracle)
        result = self._written[writer, key]
        if isinstance(result, BaseException):
            # the traceback of the write, not grown by every test re-raising it
            raise result.with_traceback(self._tracebacks[writer, key])
        return result

    def first_cut_row(self, writer: type[Parquet], type: str, nulls: str) -> int:
        """The row where the writer's baseline oracle cuts its first leaf first, the
        middle row if it doesn't cut it (test_every_column_is_chunked fails then)."""
        if (writer, type, nulls) in self._first_cut_rows:
            return self._first_cut_rows[writer, type, nulls]
        table = self.table(type, nulls)
        cuts = self.layout(writer, Key(type, nulls)).cuts(0)
        if not cuts:
            return len(table) // 2
        cut, low, high = cuts[0], 0, len(table)
        while low < high:
            middle = (low + high) // 2
            if levels(table.slice(0, middle))[0] < cut:
                low = middle + 1
            else:
                high = middle
        assert levels(table.slice(0, low))[0] == cut, "a cut within a record"
        self._first_cut_rows[writer, type, nulls] = low
        return low

    def _generate(self, type: str, nulls: str, name: str) -> pa.Table:
        if name == "base":
            rows = int(BUDGET / self.bytes_per_row(type, nulls))
            return generator(type, nulls).generate_table(rows)
        if name == "empty":
            return generator(type, nulls).generate_table(0)
        if name == "small":
            return generator(type, nulls).generate_table(1000)
        if name.startswith("budget="):
            min_size, max_size = map(int, name.removeprefix("budget=").split(","))
            budget = CHUNKS * (min_size + max_size) / 2
            rows = int(budget / self.bytes_per_row(type, nulls))
            return generator(type, nulls).generate_table(rows)
        base = self.table(type, nulls)
        editor = generator(type, nulls, seed=1)
        size = self.edit_size(type, nulls)
        kind, positions = EDITS[name]
        if kind == "insert":
            return editor.insert_rows(base, positions, size)
        if kind == "delete":
            return editor.delete_rows(base, positions, size)
        if kind == "update":
            return editor.update_rows(base, positions, size)
        edit = editor.generate_table(size)
        pieces = [edit, base] if kind == "prepend" else [base, edit]
        return pa.concat_tables(pieces).combine_chunks()

    def hashed_bytes(self, type: str, nulls: str, table: str = "base") -> np.ndarray:
        """The cumulative bytes the chunker hashes for the rows of a flat type, with a
        leading 0: each value and its 2 byte definition level."""
        array = self.table(type, nulls, table).column(0).combine_chunks()
        return np.concatenate([[0], np.cumsum(value_bytes(array) + 2)])

    def hashed_totals(self, type: str, nulls: str) -> list[int]:
        """The bytes the chunker hashes for each leaf column of the base table: its
        values, and 2 bytes per definition level and per repetition level."""
        table = self.table(type, nulls)
        array = table.column(0).combine_chunks()
        return [
            int(value_bytes(values).sum()) + count * (4 if repeated else 2)
            for (values, repeated), count in zip(leaves(array), levels(table))
        ]

    def edit_size(self, type: str, nulls: str) -> int:
        return max(1, int(EDIT_BYTES / self.bytes_per_row(type, nulls)))

    def edit_levels(
        self, type: str, nulls: str, edit: str, column: int
    ) -> list[tuple[int, int]]:
        """The base level where each edit starts and its level delta, by the
        generator's row arithmetic: an insert goes in at row int(p * n), a delete
        removes the edit size rows before it, an update replaces the row at it."""
        base, edited = self.table(type, nulls), self.table(type, nulls, edit)
        n, size = len(base), self.edit_size(type, nulls)
        kind, positions = EDITS[edit]

        def count(table: pa.Table) -> int:
            return levels(table)[column]

        if kind == "prepend":
            return [(0, count(edited.slice(0, size)))]
        if kind == "append":
            return [(count(base), count(edited.slice(n, size)))]
        result = []
        for i, position in enumerate(positions):
            row = int(position * n)
            if kind == "insert":
                inserted = edited.slice(row + i * size, size)
                result.append((count(base.slice(0, row)), count(inserted)))
            elif kind == "delete":
                start = row - size
                deleted = base.slice(start, size)
                result.append((count(base.slice(0, start)), -count(deleted)))
            else:
                updated = edited.slice(row, 1)
                delta = count(updated) - count(base.slice(row, 1))
                result.append((count(base.slice(0, row)), delta))
        return result

    def _constant_update(self, type: str, nulls: str, table: str) -> bool:
        """An update of a constant column, which has no value to change."""
        return (type, nulls) in CONSTANT and table.startswith("update")

    def _format(self, writer: type[Parquet], key: Key) -> Parquet | str:
        """The writer's format of the key, or why it can't write it."""
        # the layout comes last, it reads the writer's oracle, which these lack
        try:
            fmt = writer(**{**BASELINE, **self._setting(key)})
            return unsupported(fmt, key.type) or replace(
                fmt, **self._layout(writer, key)
            )
        except ValueError as e:
            return str(e)

    def _setting(self, key: Key) -> dict[str, Any]:
        if key.setting == "no-cdc":
            return {"use_cdc": False}
        if key.setting == "cdc=default":
            return {"use_cdc": True}
        if key.setting.startswith(("options=", "cdc=")):
            min_size, max_size, norm = map(int, key.setting.split("=")[1].split(","))
            return {"use_cdc": CdcParams(min_size, max_size, norm)}
        return SETTINGS[key.setting]

    def _layout(self, writer: type[Parquet], key: Key) -> dict[str, Any]:
        rows = len(self.table(key.type, key.nulls, key.table))
        large = rows // 3 + 1
        tiny = max(
            1, int(CDC.min_chunk_size / 4 / self.bytes_per_row(key.type, key.nulls))
        )
        if key.layout == "oracle":
            return {"row_group_size": max(1, rows)}
        if key.layout == "middle-rg":
            return {"row_group_size": len(self.table(key.type, key.nulls)) // 5 + 1}
        if key.layout == "row-limit":
            cut = self.first_cut_row(writer, key.type, key.nulls)
            return {"row_group_size": max(1, rows), "max_rows_per_page": cut // 3 + 1}
        if key.layout == "large-rgs":
            return {"row_group_size": large}
        if key.layout == "tiny-rgs":
            return {"row_group_size": tiny}
        if key.layout == "batches":
            return {"row_group_size": large, "batch_size": rows // 7 + 3}
        if key.layout == "tiny-batches":
            return {"row_group_size": large, "batch_size": tiny}
        cut = self.first_cut_row(writer, key.type, key.nulls)
        offsets = {"rg-before-cut": -1, "rg-at-cut": 0, "rg-after-cut": 1}
        return {"row_group_size": cut + offsets[key.layout]}

    def _write(self, writer: type[Parquet], oracle: bool) -> None:
        directory = self.directory / writer.__name__
        directory.mkdir(parents=True, exist_ok=True)
        keys, jobs = [], []
        for key in self.keys():
            if (key.layout == "oracle") != oracle:
                continue
            fmt = self._format(writer, key)
            if isinstance(fmt, Parquet):
                keys.append(key)
                table = self.table(key.type, key.nulls, key.table)
                jobs.append((fmt, key.stem, table, directory))
        self._phases.add((writer, oracle))
        try:
            results = writer.write_many(jobs)
        except Exception as e:
            # a failed batch fails its keys once, instead of every test retrying it
            results = [e] * len(jobs)
        for key, result in zip(keys, results):
            self._written[writer, key] = result
            if isinstance(result, BaseException):
                self._tracebacks[writer, key] = result.__traceback__


@pytest.fixture(scope="session")
def corpus(tmp_path_factory) -> Corpus:
    return Corpus(tmp_path_factory.mktemp("cdc"))


@pytest.fixture(scope="session", params=WRITERS, ids=lambda writer: writer.name)
def writer(request):
    return request.param


@pytest.mark.parametrize("layout", LAYOUTS)
@pytest.mark.parametrize("setting", SETTINGS)
@pytest.mark.parametrize("type, nulls", DATATYPES)
def test_chunker_carries_over_row_groups(corpus, writer, type, nulls, setting, layout):
    """The cuts are the oracle's and the row group starts, nothing else."""
    key = Key(type, nulls, setting, layout=layout)
    result = corpus.layout(writer, key)
    oracle = corpus.layout(writer, replace(key, layout="oracle"))
    assert all(result.row_group_starts(c) for c in result.columns)
    assert persistence(result, oracle) == [
        Persistence(column=c, **PERFECT) for c in result.columns
    ]


@pytest.mark.parametrize("setting", [s for s in SETTINGS if s != "v1"])
@pytest.mark.parametrize("layout", ["oracle", *LAYOUTS])
@pytest.mark.parametrize("type, nulls", DATATYPES)
def test_settings_dont_move_cuts(corpus, writer, type, nulls, layout, setting):
    """Dictionary encoding, the data page version and the codec cut the same pages."""
    key = Key(type, nulls, setting, layout=layout)
    result = corpus.layout(writer, key)
    baseline = corpus.layout(writer, replace(key, setting="v1"))
    assert [result.spans(c) for c in result.columns] == [
        baseline.spans(c) for c in baseline.columns
    ]


@pytest.mark.parametrize("layout", ["rg-at-cut", "tiny-rgs", "large-rgs"])
@pytest.mark.parametrize("type", ["int64", "string", "list<int>"])
def test_resetting_chunkers_would_be_caught(corpus, tmp_path, type, layout):
    """Chunkers reset at every row group miss oracle cuts or add others.

    The reset layout concatenates the pages of the row groups written as separate
    files, so a fresh chunker starts at each row group: any writer would do, the
    test checks the carry-over assertion.
    """
    table = corpus.table(type, "0%")
    oracle = corpus.layout(ParquetCpp, Key(type, "0%"))
    fmt = corpus.format(ParquetCpp, Key(type, "0%"))
    size = corpus.format(ParquetCpp, Key(type, "0%", layout=layout)).row_group_size
    assert size is not None
    paths = [
        fmt.write(f"rg{i}", table.slice(start, size), tmp_path)
        for i, start in enumerate(range(0, len(table), size))
    ]
    reset = Layout(
        [
            dict(page, row_group=i)
            for i, path in enumerate(paths)
            for page in Layout.read(path).pages
        ]
    )
    assert all(r.recall < 1 or r.extra > 0 for r in persistence(reset, oracle))


@pytest.mark.parametrize("type, nulls", DATATYPES)
def test_no_cuts_without_cdc(corpus, writer, type, nulls):
    result = corpus.layout(writer, Key(type, nulls, "no-cdc"))
    assert [result.cuts(c) for c in result.columns] == [[] for _ in result.columns]


@pytest.mark.parametrize("edit", EDITS)
@pytest.mark.parametrize("type, nulls", DATATYPES)
def test_edit_is_local(corpus, writer, type, nulls, edit):
    """The pages an edit changes are the ones around it, by exactly its levels.

    Every change covers an edit and every edit is covered, each change's level delta
    is exactly its edits', and every other page is byte identical. How soon the
    chunker resyncs on the content after an edit depends on the data (a cut needs
    consecutive gear hash matches past the minimum chunk size, and low entropy data
    is mostly cut at the maximum size), so close edits may share a change.

    The constant columns only cut at the maximum chunk size, so every edit shifts
    their content to the last page, which is the only one changing.
    """
    if (type, nulls) in CONSTANT and edit.startswith("update"):
        pytest.skip("a constant column has no value to update")
    base = corpus.layout(writer, Key(type, nulls))
    edited = corpus.layout(writer, Key(type, nulls, table=edit))
    for column in base.columns:
        found = changes(base, edited, column)
        expected = corpus.edit_levels(type, nulls, edit, column)
        if (type, nulls) in CONSTANT:
            [change] = found
            assert change.base[1] == base.spans(column)[-1].end
            assert change.delta == sum(delta for _, delta in expected)
            continue

        def covers(change, position):
            return change.base[0] <= position <= change.base[1]

        assert all(any(covers(c, p) for c in found) for p, _ in expected)
        for change in found:
            assert any(covers(change, p) for p, _ in expected)
            assert change.delta == sum(d for p, d in expected if covers(change, p))


@pytest.mark.parametrize("edit", ["insert", "delete", "update"])
@pytest.mark.parametrize("type, nulls", DATATYPES)
def test_edit_in_middle_row_group(corpus, writer, type, nulls, edit):
    """With row groups, an edit renews only the pages its single row group change
    renews, and the pages next to the row group boundaries it shifts: the two on each
    side of a boundary, as a shifted cut can move a page's content across it. The row
    groups before the edit stay identical, and an update shifts nothing."""
    if (type, nulls) in CONSTANT and edit == "update":
        pytest.skip("a constant column has no value to update")
    base = corpus.layout(writer, Key(type, nulls, layout="middle-rg"))
    edited = corpus.layout(writer, Key(type, nulls, table=edit, layout="middle-rg"))
    oracles = (Key(type, nulls), Key(type, nulls, table=edit))
    for column in base.columns:
        renewed = changes(*(corpus.layout(writer, k) for k in oracles), column)
        [(position, _)] = corpus.edit_levels(type, nulls, edit, column)
        payloads = {p["payload_hash"] for p in base.column_pages(column)}
        pages = [
            p for p in edited.column_pages(column) if p["page_type"] != "dictionary"
        ]
        spans = edited.spans(column)
        edited_rg = next(s.row_group for s in spans if s.end > position)
        for page, span in zip(pages, spans):
            if page["payload_hash"] in payloads:
                continue
            assert span.row_group >= edited_rg
            in_group = [s for s in spans if s.row_group == span.row_group]
            index = in_group.index(span)
            at_boundary = edit != "update" and (index < 2 or index >= len(in_group) - 2)
            near_change = any(
                span.start <= c.edited[1] and c.edited[0] <= span.end for c in renewed
            )
            assert near_change or at_boundary, span


@pytest.mark.parametrize("type, nulls", DATATYPES)
def test_page_row_limit_keeps_cdc_cuts(corpus, writer, type, nulls):
    """Pages cut by the row limit don't reset the chunker: every CDC cut stays."""
    limited = corpus.layout(writer, Key(type, nulls, layout="row-limit"))
    oracle = corpus.layout(writer, Key(type, nulls))
    assert all(r.recall == 1.0 and r.empty == 0 for r in persistence(limited, oracle))


FLAT = [(type, nulls) for type, nulls in DATATYPES if type not in NESTED]


def assert_chunk_sizes(
    layout: Layout, hashed: np.ndarray, min_size: int, max_size: int, by_content: bool
) -> None:
    """Every page but the last holds between the minimum and maximum chunk size of
    hashed bytes. The level a cut happens at is hashed into the chunk before it, so
    a page from s to e was counted over [s + 1, e + 1), and over [0, e + 1) the
    first one. Cut by content, the gear hash cuts every page before the maximum size
    could."""
    *pages, _ = layout.spans(0)
    for i, span in enumerate(pages):
        counted = span.start if i == 0 else span.start + 1
        assert min_size <= hashed[span.end + 1] - hashed[counted]
        assert hashed[span.end] - hashed[counted] < max_size
        if by_content:
            assert hashed[span.end + 1] - hashed[counted] < max_size


@pytest.mark.parametrize("type, nulls", FLAT)
def test_chunk_sizes(corpus, writer, type, nulls):
    """The pages are chunks of the minimum to maximum chunk size, cut by content:
    only the zeros reach the maximum size."""
    layout = corpus.layout(writer, Key(type, nulls))
    hashed = corpus.hashed_bytes(type, nulls)
    by_content = type not in ("zeros", "all-null")
    assert_chunk_sizes(
        layout, hashed, CDC.min_chunk_size, CDC.max_chunk_size, by_content
    )


@pytest.mark.parametrize("type, nulls", DATATYPES)
def test_every_column_is_chunked(corpus, writer, type, nulls):
    """Every leaf column, nested ones too, has as many pages as its hashed bytes
    need: at least one per maximum chunk size, a chunk can run past it until the
    record ends, and at most one per minimum chunk size."""
    layout = corpus.layout(writer, Key(type, nulls))
    for column, hashed in zip(layout.columns, corpus.hashed_totals(type, nulls)):
        pages = len(layout.spans(column))
        assert hashed // (CDC.max_chunk_size + RECORD_BYTES) <= pages
        assert pages <= hashed // CDC.min_chunk_size + 1


@pytest.mark.parametrize("type, nulls", sorted(CONSTANT))
def test_constant_column_cuts_at_max_size(corpus, writer, type, nulls):
    """Only the maximum chunk size cuts: the first page one row short of it, as the
    other pages' first row is hashed into the chunk before them."""
    row_bytes = int(corpus.hashed_bytes(type, nulls)[1])
    forced = -(-CDC.max_chunk_size // row_bytes)
    spans = corpus.layout(writer, Key(type, nulls)).spans(0)
    *pages, _ = [span.end - span.start for span in spans]
    assert pages == [forced - 1] + [forced] * (len(pages) - 1)


@pytest.mark.parametrize("type", TYPES)
def test_empty_table(corpus, writer, type):
    result = corpus.layout(writer, Key(type, "0%", table="empty"))
    assert all(s.end == s.start for c in result.columns for s in result.spans(c))


@pytest.mark.parametrize("offset", [512, 1024])
@pytest.mark.parametrize("type, nulls", DATATYPES)
@pytest.mark.parametrize("arrow_writer", [ParquetCpp, ParquetRs], ids=lambda w: w.name)
def test_sliced_table_writes_the_same_bytes(
    corpus, tmp_path, arrow_writer, type, nulls, offset
):
    """The chunker respects the arrays' offsets: a slice writes the bytes of its copy."""
    fmt = corpus.format(arrow_writer, Key(type, nulls))
    sliced = corpus.table(type, nulls).slice(offset)
    copy = sliced.take(pa.array(range(len(sliced))))
    assert sliced.column(0).chunk(0).offset == offset
    assert (
        fmt.write("sliced", sliced, tmp_path).read_bytes()
        == fmt.write("copy", copy, tmp_path).read_bytes()
    )


@pytest.mark.parametrize("min_size, max_size, norm, accepted", OPTION_SETS, ids=str)
def test_options_are_validated(corpus, writer, min_size, max_size, norm, accepted):
    """The rejected options fail the write, the accepted ones cut chunks of their
    sizes."""
    key = Key("int64", "0%", f"options={min_size},{max_size},{norm}", table="small")
    corpus.format(writer, key)
    if accepted:
        hashed = corpus.hashed_bytes("int64", "0%", "small")
        layout = corpus.layout(writer, key)
        assert_chunk_sizes(layout, hashed, min_size, max_size, by_content=False)
    else:
        # a panic of the Rust extension isn't an Exception
        with pytest.raises(BaseException):
            corpus.layout(writer, key)


@pytest.mark.parametrize("norm", NORM_LEVELS)
@pytest.mark.parametrize("sizes", PARAMETER_SIZES, ids=str)
@pytest.mark.parametrize("type", PARAMETER_TYPES)
def test_chunk_sizes_follow_the_parameters(corpus, writer, type, sizes, norm):
    """The chunks are of the given sizes at every norm level."""
    table = "budget={},{}".format(*sizes)
    key = Key(type, "0%", "cdc={},{},{}".format(*sizes, norm), table)
    hashed = corpus.hashed_bytes(type, "0%", table)
    assert_chunk_sizes(corpus.layout(writer, key), hashed, *sizes, by_content=False)


@pytest.mark.parametrize("type", DEFAULT_TYPES)
def test_default_chunk_sizes(corpus, writer, type):
    """The default chunks are of 256KiB to 1MiB, cut by content."""
    table = "budget={},{}".format(*DEFAULT_SIZES)
    hashed = corpus.hashed_bytes(type, "0%", table)
    layout = corpus.layout(writer, Key(type, "0%", "cdc=default", table))
    assert_chunk_sizes(layout, hashed, *DEFAULT_SIZES, by_content=True)
