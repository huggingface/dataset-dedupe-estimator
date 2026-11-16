"""Content defined chunking conformance of every parquet writer.

Every writer writes the same generated single column tables with the same settings,
and the assertions only read the page layouts of the files, so parquet-cpp,
parquet-rs, parquet-java and DuckDB are held to one standard.

The most important property is that a writer carries its chunker state across row
groups and write calls: a row group boundary forces a page cut, but the content
defined cuts after it stay where a single row group puts them.

parquet-java and DuckDB are skipped unless PARQUET_JAVA_CLASSPATH and DUCKDB_BINARY
are set. Settings a writer can't express are skipped, a known bug fails.
"""

from dataclasses import dataclass, replace
from functools import lru_cache
import os
from pathlib import Path
import re
from types import TracebackType
from typing import Any, Iterator

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from de.formats import (
    NO_LIMIT,
    CdcParams,
    Parquet,
    ParquetCpp,
    ParquetDuckDB,
    ParquetJava,
    ParquetRs,
)
from de.pages import Layout, Persistence, changes, persistence
from de.synthetic import DataGenerator, evenly_spaced

CDC = CdcParams(min_chunk_size=8 * 1024, max_chunk_size=32 * 1024, norm_level=0)

# about the bytes the chunker hashes per table, which also keeps the plain values
# below the 1MiB dictionary page limit of parquet-cpp, parquet-rs and parquet-java,
# so dictionary encoding never falls back to plain
BUDGET = 512 * 1024

# test id -> DataGenerator spec of the table's single column
TYPES: dict[str, Any] = {
    "bool": "bool",
    "int8": "int8",
    "int16": "int16",
    "int32": "int32",
    "int64": "int",
    "uint8": "uint8",
    "uint16": "uint16",
    "uint32": "uint32",
    "uint64": "uint64",
    "float16": "float16",
    "float32": "float32",
    "float64": "float",
    "decimal128": "decimal128",
    "decimal256": "decimal256",
    "date32": "date32",
    "time32": "time32",
    "time64": "time64",
    "timestamp": "timestamp",
    "binary": "binary",
    "fixed": "fixed",
    "string": "str",
    "list<int16>": ["int16"],
    "list<int>": ["int"],
    "list<string>": ["str"],
    "struct<int,string>": {"a": "int", "b": "str"},
    "struct<float64>": {"x": "float"},
    "list<struct<int>>": [{"x": "int"}],
    "list<list<int>>": [["int"]],
    "list<struct<list<int>>>": [{"x": ["int"]}],
    "zeros": "zeros",
    "all-null": "nulls",
    "largebinary": "largebinary",
    "largestring": "largestring",
    "dictionary": "dictionary",
    "duration": "duration",
    "json": "json",
    "list<int>/null-segments": "nullsegments",
}
# parquet-java and DuckDB rewrite a parquet file, so they never see these arrow types
ARROW_ONLY = {
    "largebinary",
    "largestring",
    "dictionary",
    "duration",
    "json",
    "list<int>/null-segments",
}
NULLS = {"0%": 0.0, "10%": 0.1}
# all-null has no non-null values, null segments need null lists
CASES = [
    (type, nulls)
    for type in TYPES
    for nulls in NULLS
    if not (type == "all-null" and nulls != "0%")
    and not (type == "list<int>/null-segments" and nulls != "10%")
]
# the columns that never match the gear hash, only the maximum chunk size cuts them
CONSTANT = {("zeros", "0%"), ("all-null", "0%")}

# no page limits, so the pages of a single row group are the CDC chunks
BASELINE = dict(
    use_cdc=CDC,
    use_dictionary=False,
    data_page_size=NO_LIMIT,
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
# (writer, type) -> why it's skipped: the writer writes the type as another parquet type
UNSUPPORTED: dict[tuple[str, str], str] = {
    ("duckdb", "float16"): "duckdb writes float16 as FLOAT",
    ("duckdb", "fixed"): "duckdb writes fixed size binary as variable length binary",
    ("duckdb", "decimal256"): "duckdb writes decimal(40, 6) as DOUBLE",
    ("duckdb", "time32"): "duckdb writes TIME(MILLIS) as TIME(MICROS)",
}

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


@lru_cache(maxsize=4096)
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


class Corpus:
    """Every file of the matrix, written in one batch per writer on first use.

    A writer's oracles, single row group files without page limits whose pages are
    exactly the CDC chunks, are written first; the other layouts place their row
    groups relative to parquet-cpp's oracle cuts.
    """

    def __init__(self, directory: Path) -> None:
        self.directory = directory
        self._tables: dict[tuple[str, str, str], pa.Table] = {}
        self._written: dict[tuple[type[Parquet], Key], Path | BaseException] = {}
        self._phases: set[tuple[type[Parquet], bool]] = set()
        self._tracebacks: dict[tuple[type[Parquet], Key], TracebackType | None] = {}
        self._bytes_per_row: dict[tuple[str, str], float] = {}
        self._first_cut_rows: dict[tuple[str, str], int] = {}

    def keys(self) -> Iterator[Key]:
        for type, nulls in CASES:
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

    def table(self, type: str, nulls: str, name: str = "base") -> pa.Table:
        if (type, nulls, name) not in self._tables:
            self._tables[type, nulls, name] = self._generate(type, nulls, name)
        return self._tables[type, nulls, name]

    def bytes_per_row(self, type: str, nulls: str) -> float:
        """About the bytes the chunker hashes per row: the arrow bytes and the
        definition level."""
        if (type, nulls) not in self._bytes_per_row:
            sample = self._generator(type, nulls).generate_table(1000)
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

    def first_cut_row(self, type: str, nulls: str) -> int:
        """The row where parquet-cpp's baseline oracle cuts its first leaf first."""
        if (type, nulls) in self._first_cut_rows:
            return self._first_cut_rows[type, nulls]
        cut = self.layout(ParquetCpp, Key(type, nulls)).cuts(0)[0]
        table = self.table(type, nulls)
        low, high = 0, len(table)
        while low < high:
            middle = (low + high) // 2
            if levels(table.slice(0, middle))[0] < cut:
                low = middle + 1
            else:
                high = middle
        assert levels(table.slice(0, low))[0] == cut, "a cut within a record"
        self._first_cut_rows[type, nulls] = low
        return low

    def _generator(self, type: str, nulls: str, seed: int = 0) -> DataGenerator:
        return DataGenerator({"x": TYPES[type]}, seed=seed, null_ratio=NULLS[nulls])

    def _generate(self, type: str, nulls: str, name: str) -> pa.Table:
        if name == "base":
            rows = int(BUDGET / self.bytes_per_row(type, nulls))
            return self._generator(type, nulls).generate_table(rows)
        base = self.table(type, nulls)
        generator = self._generator(type, nulls, seed=1)
        size = self.edit_size(type, nulls)
        kind, positions = EDITS[name]
        if kind == "insert":
            return generator.insert_rows(base, positions, size)
        if kind == "delete":
            return generator.delete_rows(base, positions, size)
        if kind == "update":
            return generator.update_rows(base, positions, size)
        edit = generator.generate_table(size)
        pieces = [edit, base] if kind == "prepend" else [base, edit]
        return pa.concat_tables(pieces).combine_chunks()

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
        try:
            fmt = writer(**{**BASELINE, **self._setting(key), **self._layout(key)})
        except ValueError as e:
            return str(e)
        if (fmt.name, key.type) in UNSUPPORTED:
            return UNSUPPORTED[fmt.name, key.type]
        if key.type in ARROW_ONLY and writer not in (ParquetCpp, ParquetRs):
            return f"{fmt.name} never sees the arrow type of {key.type}"
        if key.type == "dictionary" and key.setting == "v2":
            return "pyarrow can't read delta encoded pages back as a dictionary array"
        return fmt

    def _setting(self, key: Key) -> dict[str, Any]:
        if key.setting == "no-cdc":
            return {"use_cdc": False}
        return SETTINGS[key.setting]

    def _layout(self, key: Key) -> dict[str, Any]:
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
            cut = self.first_cut_row(key.type, key.nulls)
            return {"row_group_size": max(1, rows), "max_rows_per_page": cut // 3 + 1}
        if key.layout == "large-rgs":
            return {"row_group_size": large}
        if key.layout == "tiny-rgs":
            return {"row_group_size": tiny}
        if key.layout == "batches":
            return {"row_group_size": large, "batch_size": rows // 7 + 3}
        if key.layout == "tiny-batches":
            return {"row_group_size": large, "batch_size": tiny}
        cut = self.first_cut_row(key.type, key.nulls)
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
        for key, result in zip(keys, writer.write_many(jobs)):
            self._written[writer, key] = result
            if isinstance(result, BaseException):
                self._tracebacks[writer, key] = result.__traceback__
        self._phases.add((writer, oracle))


@pytest.fixture(scope="session")
def corpus(tmp_path_factory) -> Corpus:
    return Corpus(tmp_path_factory.mktemp("cdc"))


@pytest.fixture(scope="session", params=WRITERS, ids=lambda writer: writer.name)
def writer(request):
    return request.param


@pytest.mark.parametrize("layout", LAYOUTS)
@pytest.mark.parametrize("setting", SETTINGS)
@pytest.mark.parametrize("type, nulls", CASES)
def test_chunker_carries_over_row_groups(corpus, writer, type, nulls, setting, layout):
    """The cuts are the oracle's and the row group starts, nothing else."""
    key = Key(type, nulls, setting, layout=layout)
    result = corpus.layout(writer, key)
    oracle = corpus.layout(writer, replace(key, layout="oracle"))
    assert all(result.row_group_starts(c) for c in result.columns)
    assert persistence(result, oracle) == [
        Persistence(column=c, **PERFECT) for c in result.columns
    ]


@pytest.mark.parametrize("layout", LAYOUTS)
@pytest.mark.parametrize("setting", SETTINGS)
@pytest.mark.parametrize("type, nulls", CASES)
def test_same_cuts_as_parquet_cpp_across_row_groups(
    corpus, writer, type, nulls, setting, layout
):
    """The pages are parquet-cpp's with the same row group starts.

    If the row group starts differ, as DuckDB rounds row groups to whole vectors,
    the cuts are parquet-cpp's oracle cuts and the writer's row group starts: what
    parquet-cpp writes for those starts, as it carries its chunker over.
    """
    key = Key(type, nulls, setting, layout=layout)
    result = corpus.layout(writer, key)
    cpp = corpus.layout(ParquetCpp, key)
    columns = result.columns
    starts = [result.row_group_starts(c) for c in columns]
    if starts == [cpp.row_group_starts(c) for c in columns]:
        assert [result.spans(c) for c in columns] == [cpp.spans(c) for c in columns]
    else:
        cpp_oracle = corpus.layout(ParquetCpp, replace(key, layout="oracle"))
        assert persistence(result, cpp_oracle) == [
            Persistence(column=c, **PERFECT) for c in columns
        ]


@pytest.mark.parametrize("setting", [s for s in SETTINGS if s != "v1"])
@pytest.mark.parametrize("layout", ["oracle", *LAYOUTS])
@pytest.mark.parametrize("type, nulls", CASES)
def test_settings_dont_move_cuts(corpus, writer, type, nulls, layout, setting):
    """Dictionary encoding, the data page version and the codec cut the same pages."""
    key = Key(type, nulls, setting, layout=layout)
    result = corpus.layout(writer, key)
    baseline = corpus.layout(writer, replace(key, setting="v1"))
    assert [result.spans(c) for c in result.columns] == [
        baseline.spans(c) for c in baseline.columns
    ]


@pytest.mark.parametrize(
    "setting, table",
    [(setting, "base") for setting in SETTINGS] + [("v1", edit) for edit in EDITS],
)
@pytest.mark.parametrize("type, nulls", CASES)
def test_same_cuts_as_parquet_cpp(corpus, writer, type, nulls, setting, table):
    """The oracles' pages are parquet-cpp's, of every setting and edited table: so
    every writer resyncs after an edit exactly like parquet-cpp."""
    if (type, nulls) in CONSTANT and table.startswith("update"):
        pytest.skip("a constant column has no value to update")
    key = Key(type, nulls, setting, table)
    result, cpp = corpus.layout(writer, key), corpus.layout(ParquetCpp, key)
    assert [result.spans(c) for c in result.columns] == [
        cpp.spans(c) for c in cpp.columns
    ]


@pytest.mark.parametrize("layout", ["rg-at-cut", "tiny-rgs", "large-rgs"])
@pytest.mark.parametrize("type", ["int64", "string", "list<int>"])
def test_resetting_chunkers_would_be_caught(corpus, tmp_path, type, layout):
    """Chunkers reset at every row group miss oracle cuts or add others.

    The reset layout concatenates the pages of the row groups written as separate
    parquet-cpp files, so a fresh chunker starts at each row group.
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


@pytest.mark.parametrize("type, nulls", CASES)
def test_no_cuts_without_cdc(corpus, writer, type, nulls):
    result = corpus.layout(writer, Key(type, nulls, "no-cdc"))
    assert [result.cuts(c) for c in result.columns] == [[] for _ in result.columns]


@pytest.mark.parametrize("edit", EDITS)
@pytest.mark.parametrize("type, nulls", CASES)
def test_edit_is_local(corpus, writer, type, nulls, edit):
    """The pages an edit changes are the ones around it, by exactly its levels.

    Every change covers an edit and every edit is covered, each change's level delta
    is exactly its edits', and every other page is byte identical. How soon the
    chunker resyncs on the content after an edit depends on the data (a cut needs
    consecutive gear hash matches past the minimum chunk size, and low entropy data
    is mostly cut at the maximum size), so close edits may share a change;
    test_same_cuts_as_parquet_cpp holds every writer to parquet-cpp's resync.

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
@pytest.mark.parametrize("type, nulls", CASES)
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


@pytest.mark.parametrize("type, nulls", CASES)
def test_page_row_limit_keeps_cdc_cuts(corpus, writer, type, nulls):
    """Pages cut by the row limit don't reset the chunker: every CDC cut stays."""
    limited = corpus.layout(writer, Key(type, nulls, layout="row-limit"))
    oracle = corpus.layout(writer, Key(type, nulls))
    assert all(r.recall == 1.0 and r.empty == 0 for r in persistence(limited, oracle))
