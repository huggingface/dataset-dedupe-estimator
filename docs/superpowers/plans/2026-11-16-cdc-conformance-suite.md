# CDC Conformance Suite Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** One pytest suite, `de/tests/test_cdc.py`, that holds parquet-cpp, parquet-rs,
parquet-java and DuckDB to the same content defined chunking standard. Carrying the
chunker state across row groups and write calls is its most thoroughly tested property.

**Architecture:**
- The suite writes a matrix of generated single-column tables once per session and
  writer, then asserts only on page layouts (`de.pages.Layout`).
- Three writer changes make the matrix reachable and fast:
  - parquet-java writes many files in one JVM;
  - parquet-rs takes tables in-process through the Arrow C stream interface;
  - parquet-cpp's write calls go through a small C++ program, which keeps them inside
    row groups.

**Tech Stack:** Python 3.12, pytest, pyarrow 26, Rust (pyo3 0.23, parquet/arrow-array
58), Java (parquet-java via `ParquetRewrite.java`), C++20 (against pyarrow's bundled
Arrow/Parquet).

**Spec:** `docs/superpowers/specs/2026-11-16-cdc-conformance-suite-design.md`. Its checks
1–10 are referenced by number below.

## Global Constraints

- **CDC parameters:** `CdcParams(min_chunk_size=8 * 1024, max_chunk_size=32 * 1024,
  norm_level=0)`, except check 10's grid and defaults.
- **Writers:** parquet-cpp, parquet-rs, parquet-java (skipped unless
  `PARQUET_JAVA_CLASSPATH` is set) and duckdb (skipped unless `DUCKDB_BINARY` is set).
- **Known bugs fail outright:** no `xfail`.
  - Settings a writer can't express are skipped through the `ValueError` that
    `Parquet.__post_init__` raises.
  - A type a writer writes as a *different parquet type* is skipped with the reason,
    through `UNSUPPORTED` in `test_cdc.py`.
  - Anything else fails.
- **Dependencies:**
  - No new Python dependencies.
  - Rust: only the `ffi` feature of `arrow-array`, with no new crates.
  - C++: the system compiler (`CXX`, default `c++`), `-std=c++20`, built against
    `pyarrow.get_include()` and `pyarrow.get_library_dirs()`.
- **Test style:** plain `assert`, `tmp_path`/`tmp_path_factory`, public functions under
  test.
- **Failure output:** a test id names writer, type, nulls, setting and layout. There is
  no custom failure output.
- **Runtime:** under 5 minutes for all four writers.
- **README:** `README.md` and `README.md.jinja2` are kept identical.
- **Commits:** one per task, ending with
  `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- **Verify every task** with `uv run ruff check de && uv run ruff format --check de &&
  uv run mypy de`, plus `cargo clippy` for Rust tasks.

**Refinements of the spec** (all within its intent):
- **Dictionary fallback:** avoided by sizing every table to a 512 KiB budget of plain
  values, below the 1 MiB dictionary page limit of parquet-cpp, parquet-rs and
  parquet-java, instead of drawing values from a bounded pool.
- **Check 7 across row groups:**
  - When a writer's row group starts equal parquet-cpp's file of the same key, the page
    spans must equal that file's.
  - Otherwise (DuckDB rounds row groups to 2048-row vectors), the writer's cuts must
    equal parquet-cpp's oracle cuts plus the writer's own row group starts. That is
    exactly what parquet-cpp writes for those starts, as check 1 verifies for
    parquet-cpp.
- **Check 4:**
  - For updates, every row group except the edited one is byte identical, and the
    edited one has at most 2 new pages.
  - For inserts and deletes, the row groups before the edit are identical, and the
    edited file has at most `2 + 3 × (row group starts after the edit)` pages whose
    payload isn't in the base file. A shifted row group boundary can legitimately renew
    3 pages: the last page before it, the first page after it, and the page whose
    content range used to straddle it.
- **Constant columns** (`zeros` without nulls, `all-null`) get no update edits, since no
  value can change while the column stays constant.

## Review Focus

1. **No C++ compiler:** a parquet-cpp write with `batch_size` set and no working `CXX`
   must raise a `RuntimeError` naming the compiler, not a `FileNotFoundError` deep in
   subprocess. Tested in Task 5.
2. **Spaces in parquet-java paths:** a directory with a space in its name must work
   with the tab-separated stdin jobs. Tested in Task 3.
3. **Broken parquet-java classpath:** a broken `PARQUET_JAVA_CLASSPATH` must give every
   job a `RuntimeError`, without hanging or raising out of `write_many`. Tested in
   Task 3.
4. **Multi-chunk tables in parquet-rs:** a table with several chunks written by
   parquet-rs without `batch_size` must still be written with one write call, so the
   row groups follow `row_group_size` only. Tested in Task 4.
5. **Arrow-only types in parquet-cpp streaming:** large strings and dictionaries must
   read back unchanged after streaming through the C++ program, since `store_schema`
   must be on. Tested in Task 5.

---

### Task 1: Generated data: nulls, the new types, shared dictionary, real updates

**Files:**
- Modify: `de/synthetic.py`
- Test: `de/tests/test_synthetic.py`

**Interfaces:**
- Produces:
  - `DataGenerator(schema, seed=42, null_ratio: float = 0.0)`;
  - `generate_array(rng, dtype, num_samples, null_ratio: float = 0.0) -> pa.Array`;
  - the new dtype strings and arrow types:

    | dtype | arrow type |
    |---|---|
    | `"int8"` | `pa.int8()` |
    | `"int16"` | `pa.int16()` |
    | `"int32"` | `pa.int32()` |
    | `"uint8"` | `pa.uint8()` |
    | `"uint16"` | `pa.uint16()` |
    | `"uint32"` | `pa.uint32()` |
    | `"uint64"` | `pa.uint64()` |
    | `"float16"` | `pa.float16()` |
    | `"float32"` | `pa.float32()` |
    | `"decimal128"` | `pa.decimal128(18, 6)` |
    | `"decimal256"` | `pa.decimal256(40, 6)` |
    | `"date32"` | `pa.date32()` |
    | `"time32"` | `pa.time32("ms")` |
    | `"time64"` | `pa.time64("ns")` |
    | `"timestamp"` | `pa.timestamp("ns")` |
    | `"duration"` | `pa.duration("ns")` |
    | `"binary"` | `pa.binary()` |
    | `"largebinary"` | `pa.large_binary()` |
    | `"fixed"` | `pa.binary(16)` |
    | `"dictionary"` | `pa.dictionary(pa.int32(), pa.string())` |
    | `"json"` | `pa.json_(pa.string())` |
    | `"nulls"` | `pa.int32()`, every value null |
    | `"nullsegments"` | `pa.list_(pa.int64())`, null entries keep non-empty child ranges |

  - `DataGenerator.update_rows` always changes each updated value, and raises
    `ValueError` when it can't (a constant column).

- [ ] **Step 1: Write the failing tests** in `de/tests/test_synthetic.py`:

```python
NEW_TYPES = [
    ("int8", pa.int8()), ("int16", pa.int16()), ("int32", pa.int32()),
    ("uint8", pa.uint8()), ("uint16", pa.uint16()), ("uint32", pa.uint32()),
    ("uint64", pa.uint64()), ("float16", pa.float16()), ("float32", pa.float32()),
    ("decimal128", pa.decimal128(18, 6)), ("decimal256", pa.decimal256(40, 6)),
    ("date32", pa.date32()), ("time32", pa.time32("ms")), ("time64", pa.time64("ns")),
    ("timestamp", pa.timestamp("ns")), ("duration", pa.duration("ns")),
    ("binary", pa.binary()), ("largebinary", pa.large_binary()),
    ("fixed", pa.binary(16)), ("dictionary", pa.dictionary(pa.int32(), pa.string())),
    ("json", pa.json_(pa.string())), ("nulls", pa.int32()),
    ("nullsegments", pa.list_(pa.int64())),
]

class TestNewTypes:
    @pytest.mark.parametrize("dtype, arrow_type", NEW_TYPES)
    def test_type_and_length(self, dtype, arrow_type):
        data = generate_array(np.random.default_rng(0), dtype, 100)
        assert data.type == arrow_type
        assert len(data) == 100

    def test_all_nulls(self):
        assert generate_array(np.random.default_rng(0), "nulls", 100).null_count == 100

    def test_dictionary_is_shared_by_every_generator(self):
        a = generate_array(np.random.default_rng(1), "dictionary", 100)
        b = generate_array(np.random.default_rng(2), "dictionary", 100)
        assert a.dictionary.equals(b.dictionary)

    def test_edits_keep_the_dictionary(self):
        gen = DataGenerator({"d": "dictionary"})
        table = gen.generate_table(1000)
        inserted = DataGenerator({"d": "dictionary"}, seed=1).insert_rows(table, [0.5])
        assert inserted.column("d").chunk(0).dictionary.equals(
            table.column("d").chunk(0).dictionary
        )

class TestNulls:
    def test_null_ratio_at_every_nesting_level(self):
        data = generate_array(
            np.random.default_rng(0), [{"x": ["int"]}], 10_000, null_ratio=0.1
        )
        structs = data.values
        ints = structs.field("x").values
        for level in (data, structs, structs.field("x"), ints):
            assert 0.08 < level.null_count / len(level) < 0.12

    def test_null_list_entries_have_empty_child_ranges(self):
        data = generate_array(np.random.default_rng(0), ["int"], 10_000, null_ratio=0.1)
        lengths = np.diff(data.offsets.to_numpy())
        assert (lengths[data.is_null().to_numpy(zero_copy_only=False)] == 0).all()

    def test_null_segments_keep_child_ranges(self):
        data = generate_array(
            np.random.default_rng(0), "nullsegments", 10_000, null_ratio=0.1
        )
        lengths = np.diff(data.offsets.to_numpy())
        assert (lengths[data.is_null().to_numpy(zero_copy_only=False)] > 0).any()

    def test_zero_ratio_draws_no_masks(self):
        # the existing generated data stays the same: no rng draws for masks
        a = DataGenerator({"a": "int", "b": ["str"]}).generate_table(100)
        b = DataGenerator({"a": "int", "b": ["str"]}, null_ratio=0.0).generate_table(100)
        assert a.equals(b) and a.column("a").null_count == 0

class TestUpdateRows:
    def test_updates_change_every_edited_value(self):
        gen = DataGenerator({"a": "bool"})
        table = gen.generate_table(1000)
        updated = gen.update_rows(table, [0.25, 0.5, 0.75], edit_size=1)
        before, after = table.column("a").to_pylist(), updated.column("a").to_pylist()
        assert [i for i, (x, y) in enumerate(zip(before, after)) if x != y] == [
            250, 500, 750
        ]

    def test_constant_column_cant_be_updated(self):
        gen = DataGenerator({"a": "zeros"})
        with pytest.raises(ValueError, match="constant"):
            gen.update_rows(gen.generate_table(100), [0.5])
```

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest de/tests/test_synthetic.py -q`
Expected: the new tests FAIL (`Unsupported data type`, unexpected keyword `null_ratio`).

- [ ] **Step 3: Implement in `de/synthetic.py`**
  - **Nulls:**
    - `generate_array(rng, dtype, num_samples, null_ratio=0.0)` draws a validity mask
      (`rng.random(num_samples) < null_ratio`) **only when `null_ratio > 0`**, and
      passes `null_ratio` down to children.
    - Lists take the mask with zero lengths at the null entries. `"nullsegments"` keeps
      their lengths.
    - Structs take the mask through `StructArray.from_arrays(..., mask=...)`.
    - Dictionary indices and JSON storage take the mask before wrapping.
  - **`generate_table`:** builds the top-level struct without a mask, so rows are never
    null.
  - **`"dictionary"`:** indices into a module-level `DICTIONARY` of 1000 strings,
    generated once from `np.random.default_rng(0)`, so every generator and edit shares
    the dictionary.
  - **`update_rows`:** takes each replacement from generated candidate rows, skipping
    those equal (`as_py()`) to the original. It raises
    `ValueError("can't update a constant column")` when no candidate differs.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest de/tests/test_synthetic.py -q`
Expected: PASS, the existing tests included.

- [ ] **Step 5: Commit**

```bash
git add de/synthetic.py de/tests/test_synthetic.py
git commit -m "feat: generate nulls and every column type of the CDC suite"
```

### Task 2: Find the pages an edit changed

**Files:**
- Modify: `de/pages.py`
- Test: `de/tests/test_pages.py`

**Interfaces:**
- Produces:

```python
@dataclass(frozen=True)
class Change:
    """A run of data pages of one column that differs between a base and an edited file."""
    column: int
    base: tuple[int, int]    # level range [start, end) of the base's differing pages
    edited: tuple[int, int]  # level range of the edited file's differing pages
    removed: int             # base pages in the run
    added: int               # edited pages in the run

    @property
    def delta(self) -> int:  # edited levels minus base levels

def changes(base: Layout, edited: Layout, column: int) -> list[Change]:
```

  - Data pages only, matched by `payload_hash` with
    `difflib.SequenceMatcher(None, a, b, autojunk=False)`.
  - Every non-`equal` opcode is one `Change`.
  - A pure insertion has an empty base range at its position. That position is the base
    page's start, or the column's last end at the end of the file.

- [ ] **Step 1: Write the failing tests**, a `TestChanges` class in `test_pages.py`:

```python
def layout_with_payloads(*row_groups):
    """Single column layout from the (num_values, payload) data pages of each row group."""
    return Layout([
        {"column": 0, "row_group": rg, "page_type": "data_page_v1",
         "num_values": n, "payload_hash": payload}
        for rg, pages in enumerate(row_groups) for n, payload in pages
    ])

BASE = [(10, "a"), (10, "b"), (10, "c"), (10, "d"), (10, "e")]

class TestChanges:
    def test_identical(self):
        assert changes(layout_with_payloads(BASE), layout_with_payloads(BASE), 0) == []

    def test_insert_within_a_page(self):
        edited = layout_with_payloads([(10, "a"), (12, "x"), *BASE[2:]])
        [change] = changes(layout_with_payloads(BASE), edited, 0)
        assert change == Change(0, base=(10, 20), edited=(10, 22), removed=1, added=1)
        assert change.delta == 2

    def test_delete_splitting_a_page(self):
        edited = layout_with_payloads([(10, "a"), (3, "x"), (4, "y"), *BASE[2:]])
        [change] = changes(layout_with_payloads(BASE), edited, 0)
        assert (change.removed, change.added, change.delta) == (1, 2, -3)

    def test_two_edits(self):
        edited = layout_with_payloads(
            [(10, "a"), (11, "x"), (10, "c"), (9, "y"), (10, "e")]
        )
        assert [c.base for c in changes(layout_with_payloads(BASE), edited, 0)] == [
            (10, 20), (30, 40)
        ]

    def test_prepend_and_append(self):
        edited = layout_with_payloads([(13, "p"), *BASE[1:4], (12, "q")])
        assert [(c.base, c.edited) for c in changes(layout_with_payloads(BASE), edited, 0)] == [
            ((0, 10), (0, 13)), ((40, 50), (40, 52))
        ]

    def test_pure_insertion(self):
        edited = layout_with_payloads([*BASE[:2], (3, "n"), *BASE[2:]])
        [change] = changes(layout_with_payloads(BASE), edited, 0)
        assert change == Change(0, base=(20, 20), edited=(20, 23), removed=0, added=1)

    def test_dictionary_pages_are_ignored(self):
        def with_dictionary(payload):
            layout = layout_with_payloads(BASE)
            page = {"column": 0, "row_group": 0, "page_type": "dictionary",
                    "num_values": 5, "payload_hash": payload}
            return Layout([page, *layout.pages])
        assert changes(with_dictionary("d1"), with_dictionary("d2"), 0) == []

    def test_constant_pages_change_only_at_the_end(self):
        base = layout_with_payloads([(10, "z")] * 4 + [(5, "w")])
        edited = layout_with_payloads([(10, "z")] * 4 + [(7, "v")])
        [change] = changes(base, edited, 0)
        assert (change.base, change.edited) == ((40, 45), (40, 47))

    def test_row_group_boundaries_dont_split_matches(self):
        edited = layout_with_payloads(BASE[:2], BASE[2:])
        assert changes(layout_with_payloads(BASE), edited, 0) == []
```

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest de/tests/test_pages.py -q -k TestChanges`
Expected: FAIL with `ImportError: cannot import name 'changes'`.

- [ ] **Step 3: Implement `Change` and `changes`** in `de/pages.py`, after `compare`.
  Level ranges come from `Layout.spans(column)`, whose order matches the data pages of
  `Layout.column_pages(column)` without dictionary pages.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest de/tests/test_pages.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add de/pages.py de/tests/test_pages.py
git commit -m "feat: find the pages an edit changed"
```

### Task 3: Write many files at once, parquet-java in one JVM

**Files:**
- Modify: `de/formats.py`
- Modify: `de/java/ParquetRewrite.java`
- Test: `de/tests/test_formats.py`

**Interfaces:**
- Produces:
  - **`Parquet._check(self, src, dest) -> None`:** the read-back check extracted from
    `Parquet.write`, raising `AssertionError` with the existing message. `write` calls
    it when `sanity_check` is set.
    - It compares tables with dictionary columns decoded on both sides, since a reader
      rebuilds the dictionary in first-appearance order.
    - It reads with `arrow_extensions_enabled=True`, so the JSON type reads back as
      itself.
    - Both are subject to the confirmation in Step 3.
  - **`Parquet.write_many(cls, jobs: Sequence[tuple[Parquet, str, pa.Table, Path]],
    max_workers: Optional[int] = None) -> list[Path | Exception]`:** a classmethod
    that writes each job's table with its format into its directory. It returns, in
    job order, the written path or the exception the job raised (its write or its
    check). The default runs `fmt.write` in a `ThreadPoolExecutor(max_workers)`.
  - **`ParquetJava.write_many`:** the same signature.
    - It writes each distinct table (by `id`) once to a temporary source file.
    - It splits the jobs into `max_workers` groups (default `min(4, os.cpu_count())`)
      and runs one JVM per group, the groups in parallel threads.
    - It checks each written file with `_check`.
    - A JVM that exits non-zero gives each of its unfinished jobs
      `RuntimeError(f"parquet-java failed: {stderr}")`.
  - **`ParquetJava._rewrite`:** sends a single job through the same protocol and raises
    its error as `RuntimeError(f"parquet-java failed: {message}")`.
  - **The `ParquetRewrite.java` protocol:**
    - stdin lines `SRC\tDEST\tKEY=VALUE\t...`, with the same keys as today;
    - stdout lines `ok\tDEST` or `error\tDEST\tMESSAGE`, with newlines in the message
      replaced by spaces;
    - one job's exception never stops the others.

- [ ] **Step 1: Write the failing tests**:

```python
class TestWriteMany:  # in test_formats.py, no external writers needed
    def test_paths_and_errors_in_job_order(self, tmp_path, table):
        good = ParquetRs(use_cdc=False)
        bad = ParquetRs(use_cdc=False, compression="nonexistent")
        results = ParquetRs.write_many(
            [(good, "a", table, tmp_path), (bad, "b", table, tmp_path),
             (good, "c", table, tmp_path)]
        )
        assert isinstance(results[1], ValueError)
        assert pq.read_table(results[0]).equals(table)
        assert pq.read_table(results[2]).equals(table)

@needs_java
class TestParquetJava:  # added to the existing class
    def test_write_many_in_one_jvm(self, tmp_path, table, monkeypatch):
        calls = []
        run = subprocess.Popen
        monkeypatch.setattr(subprocess, "Popen", lambda *a, **kw: calls.append(a) or run(*a, **kw))
        fmt = ParquetJava(use_cdc=CDC)
        results = ParquetJava.write_many(
            [(fmt, f"t{i}", table, tmp_path) for i in range(6)], max_workers=1
        )
        assert len(calls) == 1
        assert all(pq.read_table(path).equals(table) for path in results)

    def test_a_failing_job_doesnt_fail_the_others(self, tmp_path, table):
        good, bad = ParquetJava(use_cdc=False), ParquetJava(use_cdc=False, compression="nonexistent")
        results = ParquetJava.write_many(
            [(good, "a", table, tmp_path), (bad, "b", table, tmp_path), (good, "c", table, tmp_path)],
            max_workers=1,
        )
        assert "NONEXISTENT" in str(results[1])
        assert pq.read_table(results[0]).equals(table) and pq.read_table(results[2]).equals(table)

    def test_paths_with_spaces(self, tmp_path, table):  # Review Focus 2
        directory = tmp_path / "with space"
        directory.mkdir()
        [path] = ParquetJava.write_many([(ParquetJava(use_cdc=False), "a b", table, directory)])
        assert pq.read_table(path).equals(table)

    def test_broken_classpath_fails_every_job(self, tmp_path, table, monkeypatch):  # Review Focus 3
        monkeypatch.setenv("PARQUET_JAVA_CLASSPATH", str(tmp_path / "nothing.jar"))
        fmt = ParquetJava(use_cdc=False)
        results = ParquetJava.write_many([(fmt, "a", table, tmp_path), (fmt, "b", table, tmp_path)])
        assert all(isinstance(r, RuntimeError) for r in results)
```

  `subprocess.run` creates its process through the module's `Popen`, so patching
  `Popen` counts the JVM launches either way.

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest de/tests/test_formats.py -q -k "WriteMany or write_many or failing_job or spaces or classpath"`
Expected: FAIL (`write_many` doesn't exist). The parquet-java tests need
`PARQUET_JAVA_CLASSPATH`. Rebuild it per the project memory if needed: `-pl
parquet-column,parquet-hadoop` without `-am`.

- [ ] **Step 3: Implement**
  - **`ParquetRewrite.java`:** `main` reads the jobs from stdin and runs a
    `rewrite(Path src, Path dest, Map<String, String> options)` method holding today's
    body.
  - **`formats.py`:** `_check`, `write_many` and the `ParquetJava` changes above.
  - **Confirm the read-back behaviour** with a two-line check:
    - is a dictionary column's dictionary rebuilt on read-back?
    - does `pq.read_table(..., arrow_extensions_enabled=True)` restore `pa.json_`?

    Keep only the parts of `_check` the check shows are needed.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest de/tests/test_formats.py de/tests/test_parity.py -q`
Expected: PASS. The parity check still writes through `fmt.write`.

- [ ] **Step 5: Commit**

```bash
git add de/formats.py de/java/ParquetRewrite.java de/tests/test_formats.py
git commit -m "feat: write many files at once, with parquet-java in one JVM"
```

### Task 4: parquet-rs writes tables in-process

**Files:**
- Modify: `Cargo.toml`: `arrow-array = { version = "58.1", features = ["ffi"] }`
- Modify: `src/fileutils.rs`, `src/lib.rs`, `de/formats.py`
- Test: `de/tests/test_formats.py`

**Interfaces:**
- Produces:
  - **Rust, `src/fileutils.rs`:**
    - `fn write_batches(reader: impl RecordBatchReader, dest_path: &str, options:
      &WriterOptions) -> PyResult<()>`: one `ArrowWriter::write` per batch, shared by
      both entry points;
    - `rewrite_to_parquet_rs` builds its parquet reader and calls `write_batches`;
    - `pub(crate) fn write_to_parquet_rs(reader: ArrowArrayStreamReader, dest_path:
      &str, options: &WriterOptions) -> PyResult<()>`.
  - **Python, `de.core.write_to_parquet_rs(data, dest_path: str, options: Parquet) ->
    None`:**
    - `data` is any object with `__arrow_c_stream__`;
    - the capsule pointer is imported with `ArrowArrayStreamReader::from_raw`;
    - the GIL is released around the writing if the reader is `Send`.
  - **`ParquetRs._write(src, dest)`:**
    - a `Path` goes to `rewrite_to_parquet_rs`;
    - a table goes to `write_to_parquet_rs(pa.RecordBatchReader.from_batches(src.schema,
      batches), ...)`, where `batches` is:
      - without `batch_size`: `src.to_batches()`, after `combine_chunks()` only if a
        column has more than one chunk, so a single-chunk slice keeps its offset;
      - with `batch_size`: `src.slice(offset, batch_size).to_batches()` for each
        offset.

- [ ] **Step 1: Write the failing tests** in `TestParquetRs`:

```python
    @pytest.mark.parametrize("batch_size", [None, 100])
    def test_sliced_table_writes_the_same_bytes_as_a_copy(self, tmp_path, batch_size):
        data = DataGenerator({"a": "int", "b": ["str"]}, null_ratio=0.1).generate_table(2000)
        sliced = data.slice(512)
        copy = sliced.take(pa.array(range(len(sliced))))
        assert sliced.column(0).chunk(0).offset == 512
        fmt = ParquetRs(use_cdc=CDC, use_dictionary=False, batch_size=batch_size)
        assert fmt.write("sliced", sliced, tmp_path).read_bytes() == fmt.write(
            "copy", copy, tmp_path
        ).read_bytes()

    def test_null_segments_reach_the_writer(self, tmp_path, monkeypatch):
        # nothing is written to a temporary file: the arrays arrive unchanged
        monkeypatch.setattr(pq, "write_table", None)
        data = DataGenerator({"a": "nullsegments", "d": "dictionary"}, null_ratio=0.1).generate_table(2000)
        ParquetRs(use_cdc=CDC).write("out", data, tmp_path)

    def test_chunked_table_is_one_write_call(self, tmp_path):  # Review Focus 4
        data = pa.concat_tables([pa.table({"x": list(range(70))})] * 2)
        assert data.column(0).num_chunks == 2
        path = ParquetRs(use_cdc=False, row_group_size=100).write("out", data, tmp_path)
        assert row_group_sizes(path) == [100, 40]
```

  Note the `(ParquetRs, 25, ...)` row of `test_row_groups_per_mode` stays
  `[30, 30, 30, 10]`.

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest de/tests/test_formats.py -q -k ParquetRs`
Expected: `test_null_segments_reach_the_writer` FAILS (`'NoneType' object is not
callable`). The others may pass already through the file rewrite.

- [ ] **Step 3: Implement**, then rebuild with `uv run maturin develop --release`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo clippy && uv run pytest de/tests/test_formats.py de/tests/test_pages.py -q`
Expected: PASS, and `test_arrow_rs_stream_matches_single_shot` still passes.

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml Cargo.lock src/fileutils.rs src/lib.rs de/formats.py de/tests/test_formats.py
git commit -m "feat: hand tables to parquet-rs in-process through the arrow C stream interface"
```

### Task 5: parquet-cpp write calls inside row groups

**Files:**
- Create: `de/cpp/ParquetWrite.cc`
- Modify: `de/formats.py`
- Modify: `README.md` and `README.md.jinja2`, identically. In "Benchmark Parquet
  Writers", streaming is no longer `pq.ParquetWriter`; add the C++ compiler requirement.
- Test: `de/tests/test_formats.py`

**Interfaces:**
- Produces:
  - **`de/cpp/ParquetWrite.cc`:**
    - usage `ParquetWrite SRC DEST batch_size=N [KEY=VALUE...]`, with `ParquetRewrite.java`'s
      keys plus `column_encoding=PATH:ENCODING,...`;
    - it reads SRC with `parquet::arrow::OpenFile` and `ReadTable`;
    - it writes DEST with `parquet::arrow::FileWriter::Open` and one `WriteRecordBatch`
      per `TableBatchReader` batch of `batch_size` rows;
    - `WriterProperties`:
      - compression and dictionary;
      - `enable_content_defined_chunking` with `content_defined_chunking_options`;
      - `data_pagesize`, `max_rows_per_page`, `data_page_version`;
      - `max_row_group_length` (`row_group_size`);
      - `enable_write_page_index`;
      - `encoding` with `disable_dictionary` per `column_encoding` path;
    - `ArrowWriterProperties` with `store_schema()`;
    - errors go to stderr with exit code 1.
  - **`ParquetCpp._column_encoding(self, schema: pa.Schema) -> dict[str, str]`:** the
    v2-without-dictionary mapping, extracted from `_write`.
  - **`ParquetCpp._program(cls) -> Path`:** builds the program once per source digest
    and pyarrow version. There is no in-process memoization: each call checks for the
    built file, so a changed `tempfile.tempdir` or `CXX` takes effect.
    - into `Path(tempfile.gettempdir()) / "de" / f"ParquetWrite-{digest}"`;
    - with `pa.create_library_symlinks()`, then `[os.environ.get("CXX", "c++"),
      "-std=c++20", "-O2", src, "-I", include, "-L", libdir, "-larrow", "-lparquet",
      f"-Wl,-rpath,{libdir}", "-o", tmp]` and `os.replace`;
    - under a module-level `threading.Lock`;
    - a missing or failing compiler raises
      `RuntimeError(f"building {src.name} with {cxx} failed: ...")`.
  - **`ParquetCpp._write`:**
    - without `batch_size` it's unchanged (`pq.write_table`);
    - with `batch_size` it defers to `Parquet._write`, which writes a temporary file for
      tables, so `_rewrite(src, dest)` runs the program.

- [ ] **Step 1: Write the failing tests**:

```python
    # TestParquet: replace the (ParquetCpp, 25, [25, 25, 25, 25]) row of
    # test_row_groups_per_mode by (ParquetCpp, 25, [30, 30, 30, 10])

class TestParquetCppStreaming:
    def test_write_calls_stay_inside_row_groups(self, tmp_path):
        data = DataGenerator({"a": "int"}).generate_table(100_000)
        single = ParquetCpp(use_cdc=CDC, use_dictionary=False, row_group_size=30_011)
        streamed = replace(single, batch_size=7_001)
        assert Layout.read(single.write("single", data, tmp_path)).spans(0) == (
            Layout.read(streamed.write("streamed", data, tmp_path)).spans(0)
        )

    def test_arrow_only_types_read_back(self, tmp_path):  # Review Focus 5
        data = DataGenerator({"s": "largestr", "d": "dictionary", "b": "largebinary"}).generate_table(1000)
        ParquetCpp(use_cdc=CDC, batch_size=100).write("out", data, tmp_path)  # write() checks

    def test_missing_compiler(self, tmp_path, monkeypatch):  # Review Focus 1
        monkeypatch.setenv("CXX", str(tmp_path / "no-compiler"))
        monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
        with pytest.raises(RuntimeError, match="no-compiler"):
            ParquetCpp(use_cdc=False, batch_size=10).write("out", pa.table({"x": [1]}), tmp_path)
```

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest de/tests/test_formats.py -q -k "row_groups_per_mode or Streaming"`
Expected: FAIL. Today each write call ends a row group, and no program exists.

- [ ] **Step 3: Spike the build before writing the program.** Compile a 5-line
  `main` that opens a `parquet::arrow::FileWriter` with the command above. Proceed only
  once it links and runs. If it doesn't, stop and report: this task's approach depends
  on it.

- [ ] **Step 4: Implement** the program, `_column_encoding`, `_program` and `_write`.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run pytest de/tests/test_formats.py de/tests/test_cli.py de/tests/test_bench.py -q`
Expected: PASS. The CLI and bench tests stream parquet-cpp through the program now; the
first run builds it.

- [ ] **Step 6: Commit**

```bash
git add de/cpp/ParquetWrite.cc de/formats.py de/tests/test_formats.py README.md README.md.jinja2
git commit -m "feat: keep parquet-cpp's write calls inside row groups"
```

### Task 6: The suite's matrix and the row group checks

**Files:**
- Create: `de/tests/test_cdc.py`
- Modify: `de/tests/test_pages.py`:
  - remove `TestContentDefinedChunking`, its `table`, `writer` and `oracle` fixtures,
    and `SCHEMA`, `NUM_ROWS`, `FIRST_CUT`, `ROW_GROUP_SIZES` and
    `VECTOR_ROW_GROUP_SIZES`;
  - `WRITERS` moves to `test_cdc.py`;
  - `ints` and `without_page_limits` stay for `TestPersistence`.

**Interfaces:**
- Consumes:
  - `DataGenerator` and the dtypes (Task 1);
  - `Parquet.write_many` (Task 3);
  - `de.pages.persistence`, `Layout`.
- Produces, in `test_cdc.py`, for Tasks 7–8:

```python
CDC = CdcParams(min_chunk_size=8 * 1024, max_chunk_size=32 * 1024, norm_level=0)
BUDGET = 512 * 1024  # plain value bytes per table
TYPES: dict[str, Any] = {   # test id -> DataGenerator spec
    "bool": "bool", "int8": "int8", "int16": "int16", "int32": "int32", "int64": "int",
    "uint8": "uint8", "uint16": "uint16", "uint32": "uint32", "uint64": "uint64",
    "float16": "float16", "float32": "float32", "float64": "float",
    "decimal128": "decimal128", "decimal256": "decimal256", "date32": "date32",
    "time32": "time32", "time64": "time64", "timestamp": "timestamp",
    "binary": "binary", "fixed": "fixed", "string": "str",
    "list<int16>": ["int16"], "list<int>": ["int"], "list<string>": ["str"],
    "struct<int,string>": {"a": "int", "b": "str"}, "struct<float64>": {"x": "float"},
    "list<struct<int>>": [{"x": "int"}], "list<list<int>>": [["int"]],
    "list<struct<list<int>>>": [{"x": ["int"]}],
    "zeros": "zeros", "all-null": "nulls",
    # arrow-only, parquet-cpp and parquet-rs
    "largebinary": "largebinary", "largestring": "largestr", "dictionary": "dictionary",
    "duration": "duration", "json": "json", "list<int>/null-segments": "nullsegments",
}
ARROW_ONLY = {"largebinary", "largestring", "dictionary", "duration", "json", "list<int>/null-segments"}
NULLS = {"0%": 0.0, "10%": 0.1}
# (type, nulls) pairs: all-null only at 0%, null segments only at 10%
CASES = [(t, n) for t in TYPES for n in NULLS
         if not (t == "all-null" and n != "0%") and not (t == "list<int>/null-segments" and n != "10%")]
CONSTANT = {("zeros", "0%"), ("all-null", "0%")}
SETTINGS = {  # on top of BASELINE
    "v1": {}, "dict": {"use_dictionary": True}, "v2": {"data_page_version": "2.0"},
    "v2-dict": {"data_page_version": "2.0", "use_dictionary": True},
    "snappy": {"compression": "snappy"},
}
BASELINE = dict(use_cdc=CDC, use_dictionary=False, data_page_size=NO_LIMIT, max_rows_per_page=NO_LIMIT)
LAYOUTS = ["rg-before-cut", "rg-at-cut", "rg-after-cut", "tiny-rgs", "large-rgs", "batches", "tiny-batches"]
# (writer name, type) -> why it's skipped: the writer writes a different parquet type
UNSUPPORTED: dict[tuple[str, str], str] = {
    ("duckdb", "float16"): "duckdb writes float16 as FLOAT",
    ("duckdb", "fixed"): "duckdb writes fixed size binary as variable length binary",
}

@dataclass(frozen=True)
class Key:
    type: str
    nulls: str
    setting: str = "v1"
    table: str = "base"     # "base", an edit name (Task 7), "empty", ...
    layout: str = "oracle"  # "oracle", a LAYOUTS entry, "middle-rg", "row-limit", ...

class Corpus:
    """Every table of the matrix, written once per writer on first use."""
    def __init__(self, directory: Path) -> None
    def table(self, type: str, nulls: str, name: str = "base") -> pa.Table      # cached
    def format(self, writer: type[Parquet], key: Key) -> Parquet  # pytest.skip when inexpressible
    def layout(self, writer: type[Parquet], key: Key) -> Layout   # writes the batch on first use, re-raises the job's exception
    def path(self, writer: type[Parquet], key: Key) -> Path
    def first_cut_row(self, type: str, nulls: str) -> int          # from parquet-cpp's baseline oracle
    def levels(self, table: pa.Table) -> list[int]                 # levels per leaf column

@pytest.fixture(scope="session")
def corpus(tmp_path_factory) -> Corpus
```

- **Tables:**
  - Base tables are `DataGenerator({"x": spec}, seed=0, null_ratio=...)`, with
    `BUDGET // bytes_per_row` rows, where `bytes_per_row` is the `nbytes / len` of a
    1000-row sample.
  - Tables are generated lazily and cached.
- **Keys of a writer:**
  - Task 6 enumerates the keys its tests need; Tasks 7 and 8 extend that enumeration.
  - The keys `format()` rejects, and `UNSUPPORTED`, are left out.
  - `ARROW_ONLY` types exist only for parquet-cpp and parquet-rs.
  - A key's file name is `re.sub(r"[^a-zA-Z0-9]+", "-", ...)` of its fields joined, as
    `de.parity` slugs its datatypes.
- **Writing:**
  - `layout()` writes all of a writer's `"oracle"` keys on first use of one, and all of
    its other keys on first use of one, each with `writer.write_many`.
  - Writing the other keys needs `first_cut_row`, which reads parquet-cpp's baseline
    oracle.
  - Files land in `directory / writer.name / slug(key)`.
  - Layouts are read lazily, with `functools.lru_cache(maxsize=4096)`.
- **Layout parameters** (`R = first_cut_row`, `n = len(table)`, `bpr = bytes per row`):

  | Layout | `row_group_size` | `batch_size` |
  |---|---|---|
  | rg-before-cut | `R - 1` | |
  | rg-at-cut | `R` | |
  | rg-after-cut | `R + 1` | |
  | tiny-rgs | `max(1, CDC.min_chunk_size // 4 // bpr)` | |
  | large-rgs | `n // 3 + 1` | |
  | batches | `n // 3 + 1` | `n // 7 + 3` |
  | tiny-batches | `n // 3 + 1` | the tiny-rgs size |

- **`first_cut_row`:** the row where the column's first non-empty page after level 0
  starts, from `Layout.cuts(0)[0]`. For nested types it's mapped from levels to rows by
  a binary search over `levels(table.slice(0, rows))[0]`.
- **`levels`:** writes the table to an in-memory parquet file (`pq.write_table`, no
  dictionary) and returns `metadata.row_group(0).column(i).num_values` per leaf.
- Writer parametrization uses `WRITERS` from today's `test_pages.py`, with its
  `skipif` marks.

- [ ] **Step 1: Write the tests**:

```python
@pytest.mark.parametrize("layout", LAYOUTS)
@pytest.mark.parametrize("setting", SETTINGS)
@pytest.mark.parametrize("type, nulls", CASES)
def test_chunker_carries_over_row_groups(corpus, writer, type, nulls, setting, layout):
    key = Key(type, nulls, setting, layout=layout)
    result = corpus.layout(writer, key)
    oracle = corpus.layout(writer, replace(key, layout="oracle"))
    assert all(result.row_group_starts(c) for c in result.columns)
    assert persistence(result, oracle) == [
        Persistence(column=c, recall=1.0, extra=0, aligned=1.0, resync=0.0, empty=0)
        for c in result.columns
    ]

@pytest.mark.parametrize("layout", LAYOUTS)
@pytest.mark.parametrize("setting", SETTINGS)
@pytest.mark.parametrize("type, nulls", CASES)
def test_same_cuts_as_parquet_cpp_across_row_groups(corpus, writer, type, nulls, setting, layout):
    key = Key(type, nulls, setting, layout=layout)
    result = corpus.layout(writer, key)
    cpp = corpus.layout(ParquetCpp, key)
    columns = result.columns
    if [result.row_group_starts(c) for c in columns] == [cpp.row_group_starts(c) for c in columns]:
        assert [result.spans(c) for c in columns] == [cpp.spans(c) for c in columns]
    else:  # e.g. duckdb's vector-rounded row groups
        cpp_oracle = corpus.layout(ParquetCpp, replace(key, layout="oracle"))
        assert persistence(result, cpp_oracle) == [
            Persistence(column=c, recall=1.0, extra=0, aligned=1.0, resync=0.0, empty=0)
            for c in columns
        ]

@pytest.mark.parametrize("setting", [s for s in SETTINGS if s != "v1"])
@pytest.mark.parametrize("layout", ["oracle", *LAYOUTS])
@pytest.mark.parametrize("type, nulls", CASES)
def test_settings_dont_move_cuts(corpus, writer, type, nulls, layout, setting):
    key = Key(type, nulls, setting, layout=layout)
    result, baseline = corpus.layout(writer, key), corpus.layout(writer, replace(key, setting="v1"))
    assert [result.spans(c) for c in result.columns] == [baseline.spans(c) for c in baseline.columns]

@pytest.mark.parametrize("setting", SETTINGS)
@pytest.mark.parametrize("type, nulls", CASES)
def test_same_cuts_as_parquet_cpp(corpus, writer, type, nulls, setting):
    key = Key(type, nulls, setting)
    result, cpp = corpus.layout(writer, key), corpus.layout(ParquetCpp, key)
    assert [result.spans(c) for c in result.columns] == [cpp.spans(c) for c in cpp.columns]

@pytest.mark.parametrize("type", ["int64", "string", "list<int>"])
@pytest.mark.parametrize("layout", ["rg-at-cut", "tiny-rgs", "large-rgs"])
def test_resetting_chunkers_would_be_caught(corpus, tmp_path, type, layout):
    """The row groups written as separate parquet-cpp files, concatenated, fail the
    carry-over check: as if the chunker were reset at every row group."""
    # as today's test_resetting_chunkers_would_be_caught, with ParquetCpp and
    # corpus.format(ParquetCpp, Key(type, "0%", layout=layout)).row_group_size
    assert all(r.recall < 1 or r.extra > 0 for r in persistence(reset, oracle))

@pytest.mark.parametrize("type, nulls", CASES)
def test_no_cuts_without_cdc(corpus, writer, type, nulls):
    result = corpus.layout(writer, Key(type, nulls, setting="no-cdc"))
    assert [result.cuts(c) for c in result.columns] == [[] for _ in result.columns]
```

  - `"no-cdc"` is a corpus setting with `use_cdc=False` on top of `BASELINE`, used only
    by that test. `SETTINGS` iteration doesn't include it.
  - `writer` is a session-scoped fixture parametrized by `WRITERS`.

- [ ] **Step 2: Run the suite and classify every failure**

Run: `uv run pytest de/tests/test_cdc.py -q --co | tail -1`, then
`uv run pytest de/tests/test_cdc.py -q 2>&1 | tail -60`

Expected: the parquet-cpp tests PASS. Other failures fall into three groups:
- **Known writer bugs, which stay failing:**
  - parquet-rs: empty pages, and wrong list cuts after the first row group;
  - DuckDB: no cuts in list, struct and decimal leaves;
  - parquet-java: decimals hashed at their stored width.
- **Read-back schema changes:** add each to `UNSUPPORTED` with the observed reason.
  Only when `pq.read_table(dest).schema` differs from the source; a data difference is
  a bug and stays failing.
- **Anything else** is a bug in the corpus. Fix it; never loosen an assertion for one
  writer.

- [ ] **Step 3: Run the rest of the tests**

Run: `uv run pytest de -q --deselect de/tests/test_cdc.py 2>&1 | tail -5`
Expected: PASS, apart from the known parquet-rs failures outside the CDC suite, if any.

- [ ] **Step 4: Commit**

```bash
git add de/tests/test_cdc.py de/tests/test_pages.py
git commit -m "test: content defined chunking conformance across row groups for every writer"
```

### Task 7: Edits

**Files:**
- Modify: `de/tests/test_cdc.py`

**Interfaces:**
- Consumes: `Corpus` and `Key` (Task 6), `changes` and `Change` (Task 2).
- Produces:
  - **`Corpus.edit_levels(type, nulls, edit, column) -> list[tuple[int, int]]`:** per
    edit, the base level position where the edited rows start and the level delta.
    - It replicates the generator's row arithmetic, with `n = len(base)`:
      - an insert goes in at row `int(p * n)`;
      - a delete removes rows `[int(p * n) - edit_size, int(p * n))`;
      - an update replaces row `int(p * n)`;
      - prepend is at row 0, and append at row `n`.
    - A row is mapped to a level with `levels(base.slice(0, row))[column]`.
    - The delta is the inserted rows' levels, minus the deleted rows', or the updated
      row's new levels minus its old ones.
    - Column indices follow the leaf order of `levels`.
  - **`EDITS`:** `dict[str, (positions: list[float], kind: str)]`:
    `insert=[0.5]`, `insert-twice=evenly_spaced(2)`, `delete=[0.5]`,
    `delete-twice=evenly_spaced(2)`, `update=[0.5]`, `update-twice=evenly_spaced(2)`,
    `prepend=[0.0]` and `append=[1.0]`.
  - **`Corpus.table(type, nulls, edit)`:**
    - edits a base table with `DataGenerator(spec, seed=1, null_ratio=...)`, through
      `insert_rows`, `delete_rows` and `update_rows`, with `edit_size = max(1, 256 //
      bytes_per_row)`;
    - prepend is `pa.concat_tables([gen.generate_table(edit_size), table])`, and
      append is the reverse;
    - every result is `combine_chunks()`ed.
  - **Corpus keys:**
    - `Key(type, nulls, table=edit)` at the baseline, for every edit except updates of
      `CONSTANT` cases;
    - `Key(type, nulls, table=t, layout="middle-rg")` for `t` in base, insert, delete
      and update, with `row_group_size = len(base) // 5 + 1`;
    - `Key(type, nulls, layout="row-limit")` with `max_rows_per_page =
      first_cut_row // 3 + 1`, where supported.

- [ ] **Step 1: Write the tests**

```python
@pytest.mark.parametrize("edit", EDITS)
@pytest.mark.parametrize("type, nulls", CASES)
def test_edit_is_local(corpus, writer, type, nulls, edit):
    if (type, nulls) in CONSTANT and edit.startswith("update"):
        pytest.skip("a constant column has no value to update")
    base = corpus.layout(writer, Key(type, nulls))
    edited = corpus.layout(writer, Key(type, nulls, table=edit))
    for column in base.columns:
        found = changes(base, edited, column)
        expected = corpus.edit_levels(type, nulls, edit, column)  # [(level position, level delta)] per edit
        if (type, nulls) in CONSTANT:
            [change] = found
            assert change.base[1] == base.spans(column)[-1].end
            assert change.delta == sum(delta for _, delta in expected)
        else:
            assert len(found) == len(expected)
            for change, (position, delta) in zip(found, expected):
                assert change.base[0] <= position <= change.base[1]
                assert change.delta == delta
                assert change.removed <= 2 and change.added <= 2

@pytest.mark.parametrize("edit", ["insert", "delete", "update"])
@pytest.mark.parametrize("type, nulls", CASES)
def test_edit_in_middle_row_group(corpus, writer, type, nulls, edit):
    if (type, nulls) in CONSTANT and edit == "update":
        pytest.skip("a constant column has no value to update")
    base = corpus.layout(writer, Key(type, nulls, layout="middle-rg"))
    edited = corpus.layout(writer, Key(type, nulls, table=edit, layout="middle-rg"))
    for column in base.columns:
        [(position, _)] = corpus.edit_levels(type, nulls, edit, column)
        edited_rg = next(s.row_group for s in edited.spans(column) if s.end > position)
        payloads = {p["payload_hash"] for p in base.column_pages(column)}
        new = [p for p in edited.column_pages(column)
               if p["page_type"] != "dictionary" and p["payload_hash"] not in payloads]
        assert all(p["row_group"] >= edited_rg for p in new)
        if edit == "update":
            assert len(new) <= 2 and all(p["row_group"] == edited_rg for p in new)
        else:
            later_starts = [g for g in edited.row_group_starts(column) if g > position]
            assert len(new) <= 2 + 3 * len(later_starts)

@pytest.mark.parametrize("type, nulls", CASES)
def test_page_row_limit_keeps_cdc_cuts(corpus, writer, type, nulls):
    limited = corpus.layout(writer, Key(type, nulls, layout="row-limit"))
    oracle = corpus.layout(writer, Key(type, nulls))
    assert all(r.recall == 1.0 and r.empty == 0 for r in persistence(limited, oracle))
```

- [ ] **Step 2: Run and classify as in Task 6, Step 2**

Run: `uv run pytest de/tests/test_cdc.py -q -k "edit or row_limit" 2>&1 | tail -40`
Expected: parquet-cpp PASSES.

  If parquet-cpp itself needs a looser page bound for a type, raise the bound for every
  writer, as the spec says, and note it in the commit message.

- [ ] **Step 3: Commit**

```bash
git add de/tests/test_cdc.py
git commit -m "test: edits change only the pages around them, in every writer"
```

### Task 8: Chunk sizes, edge cases, options and parameters

**Files:**
- Modify: `de/tests/test_cdc.py`

**Interfaces:**
- Consumes: `Corpus` and `Key` (Task 6).
- Produces:
  - **`HASHED_WIDTH: dict[str, int]`:** the value bytes parquet-cpp's chunker hashes,
    by test type:
    - 0 bytes: all-null, whose values are never hashed;
    - 1 byte: bool, int8, uint8;
    - 2 bytes: int16, uint16, float16;
    - 4 bytes: int32, uint32, float32, date32, time32, dictionary (int32 indices);
    - 8 bytes: int64, uint64, float64, time64, timestamp, duration, zeros;
    - 16 bytes: decimal128, fixed;
    - 32 bytes: decimal256.

    Binary-like types (binary, string, largebinary, largestring, json) are left out of
    the dict and hash their byte length. Every level also hashes 2 bytes for its
    definition level, since every generated field is nullable. A null value hashes only
    those 2 bytes.
  - **`NESTED`:** the test types whose spec is a list or dict, plus
    `"list<int>/null-segments"`. They're left out of the chunk size check.
  - **`OPTION_SETS`:** from parquet-cpp's `ChunkSizeParameterValidation`, as
    `(min, max, norm_level, accepted)`:
    - accepted: `(0, 32, 0)`, `(32, 64, 0)`, `(1, 65, 1)` and `(0, 128, 2)`;
    - rejected: `(0, 16, 0)`, `(32, 48, 0)`, `(32, 32, 0)`, `(1024, 512, 0)`,
      `(16, 0, 0)`, `(0, 0, 0)`, `(1, 32, 1)`, `(1, 33, 1)`, `(0, 123, 2)`,
      `(-16, -16, 0)`, `(-1, 0, 0)` and `(0, -1, 0)`.
  - **`PARAMETERS`:** size pairs `[(4096, 16384), (8192, 32768), (65536, 262144)]` ×
    norm levels -3 to 3, on types `["int64", "string", "list<int>"]`. Each pair's tables
    use a budget of `25 × (min + max) / 2`.
  - **Defaults:** `use_cdc=True`, on types `["int64", "string"]`, with a budget of
    `25 × 640 KiB`.

- [ ] **Step 1: Write the tests**

```python
FLAT = [(t, n) for t, n in CASES if t not in NESTED]

@pytest.mark.parametrize("type, nulls", FLAT)
def test_chunk_sizes(corpus, writer, type, nulls):
    """Every page but the last holds between the minimum and maximum chunk size of
    hashed bytes. The level a cut happens at is hashed into the chunk before it, so a
    page starting at s and ending at e was counted over [s + 1, e + 1) (from s for the
    first page)."""
    nbytes = corpus.hashed_bytes(type, nulls)  # per row, numpy cumulative sum with a leading 0
    *pages, _ = corpus.layout(writer, Key(type, nulls)).spans(0)
    for i, span in enumerate(pages):
        counted = span.start if i == 0 else span.start + 1
        assert CDC.min_chunk_size <= nbytes[span.end + 1] - nbytes[counted]
        assert nbytes[span.end] - nbytes[counted] < CDC.max_chunk_size

@pytest.mark.parametrize("type, nulls", sorted(CONSTANT))
def test_constant_column_cuts_at_max_size(corpus, writer, type, nulls):
    row_bytes = 2 + HASHED_WIDTH[type]  # zeros: 10, all-null: 2
    forced = -(-CDC.max_chunk_size // row_bytes)
    *pages, _ = [s.end - s.start for s in corpus.layout(writer, Key(type, nulls)).spans(0)]
    assert pages == [forced - 1] + [forced] * (len(pages) - 1)

@pytest.mark.parametrize("type", TYPES)
def test_empty_table(corpus, writer, type):
    result = corpus.layout(writer, Key(type, "0%", table="empty"))
    assert all(s.end == s.start for c in result.columns for s in result.spans(c))

@pytest.mark.parametrize("offset", [512, 1024])
@pytest.mark.parametrize("type, nulls", CASES)
@pytest.mark.parametrize("arrow_writer", [ParquetCpp, ParquetRs], ids=lambda w: w.name)
def test_sliced_table_writes_the_same_bytes(corpus, tmp_path, arrow_writer, type, nulls, offset):
    fmt = corpus.format(arrow_writer, Key(type, nulls))
    sliced = corpus.table(type, nulls).slice(offset)
    copy = sliced.take(pa.array(range(len(sliced))))
    assert fmt.write("sliced", sliced, tmp_path).read_bytes() == fmt.write("copy", copy, tmp_path).read_bytes()

@pytest.mark.parametrize("min_size, max_size, norm, accepted", OPTION_SETS, ids=str)
def test_options_are_validated_like_parquet_cpp(corpus, writer, min_size, max_size, norm, accepted):
    key = Key("int64", "0%", setting=f"options={min_size},{max_size},{norm}", table="small")
    if accepted:
        assert corpus.layout(writer, key).spans(0) == corpus.layout(ParquetCpp, key).spans(0)
    else:
        with pytest.raises(Exception):
            corpus.layout(writer, key)

@pytest.mark.parametrize("norm", range(-3, 4))
@pytest.mark.parametrize("sizes", [(4096, 16384), (8192, 32768), (65536, 262144)], ids=str)
@pytest.mark.parametrize("type", ["int64", "string", "list<int>"])
def test_chunking_parameters_match_parquet_cpp(corpus, writer, type, sizes, norm):
    key = Key(type, "0%", setting=f"cdc={sizes[0]},{sizes[1]},{norm}", table=f"budget={sizes[0]},{sizes[1]}")
    try:
        cpp = corpus.layout(ParquetCpp, key)
    except Exception as e:
        pytest.skip(f"parquet-cpp rejects it: {e}")
    assert corpus.layout(writer, key).spans(0) == cpp.spans(0)

@pytest.mark.parametrize("type", ["int64", "string"])
def test_default_options_match_parquet_cpp(corpus, writer, type):
    key = Key(type, "0%", setting="cdc=default", table="large")
    assert corpus.layout(writer, key).spans(0) == corpus.layout(ParquetCpp, key).spans(0)
```

  - **Corpus additions:**
    - the tables `"empty"` (0 rows), `"small"` (1000 rows), `"large"` (the defaults
      budget) and `"budget=MIN,MAX"`;
    - the settings `"options=MIN,MAX,NORM"` and `"cdc=MIN,MAX,NORM"` (an explicit
      `CdcParams`) and `"cdc=default"` (`use_cdc=True`), each on top of `BASELINE`;
    - `hashed_bytes(type, nulls) -> np.ndarray`: the cumulative hashed bytes of the
      base table's rows, as `HASHED_WIDTH` describes, with a leading 0.
  - A rejected job's exception is stored like any other and re-raised by `layout()`.

- [ ] **Step 2: Run and classify as in Task 6, Step 2**

Run: `uv run pytest de/tests/test_cdc.py -q -k "sizes or constant or empty or sliced or options or parameters or default" 2>&1 | tail -40`
Expected: parquet-cpp PASSES. If the exact chunk size bounds fail on parquet-cpp for a
type, check `HASHED_WIDTH` against `chunker_internal.cc` before anything else.

- [ ] **Step 3: Commit**

```bash
git add de/tests/test_cdc.py
git commit -m "test: chunk sizes, edge cases, option validation and chunking parameters"
```

### Task 9: Full run and runtime

**Files:**
- Modify: `de/tests/test_cdc.py`, only to trim the matrix if the budget is missed.

- [ ] **Step 1: Run everything with all four writers**

Run:
```bash
export PARQUET_JAVA_CLASSPATH=... DUCKDB_BINARY=...
time uv run pytest de -q --durations=15 2>&1 | tail -40
```

Expected:
- the whole run under 5 minutes;
- failures only in the classified groups of Task 6, Step 2.

- [ ] **Step 2: If the run takes over 5 minutes,** trim in the spec's order:
  1. first, the settings of the edited files;
  2. last, carry-over coverage.

  Re-run and record each writer's write time, which `write_many` duration prints per
  writer, in the commit message.

- [ ] **Step 3: Run lint, types and Rust checks**

Run: `just lint && just typecheck`
Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add -A de
git commit -m "test: the CDC suite within its runtime budget"
```
