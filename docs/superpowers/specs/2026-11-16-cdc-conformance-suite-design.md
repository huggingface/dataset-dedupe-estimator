# CDC conformance suite

## Intent

parquet-cpp, parquet-rs and parquet-java each have their own content defined chunking
(CDC) tests, and DuckDB has none that de can see. Those tests are written against each
implementation's internals, use different data and assertions, and contain slips
(parquet-cpp's UpdateOnce loop starts at 2, its Append assertion is reversed). None of
them can tell whether two implementations chunk the same data the same way.

de already drives all four writers through `de.formats` and reads exact page layouts
through `de.pages`. This suite uses that to hold every writer to one standard, from
outside any implementation: the same data, the same settings, the same assertions.

The most important property is that **the chunker state is carried across row
groups**: a row group boundary forces a page cut, but must not restart the chunker, so
the content defined cuts after it stay where a single row group would put them. That's
what makes the same data deduplicate however it's split into row groups and write calls.

## Goals

- One pytest suite, `de/tests/test_cdc.py`, run against parquet-cpp, parquet-rs,
  parquet-java and DuckDB with identical assertions.
- **The best coverage of any CDC test suite** (the user's goal). Everything the
  implementations' own suites check that can be seen in the written files is covered,
  for every writer, plus what none of them check. See [Prior art](#prior-art): column
  types, nulls, dictionary, edits, empty tables, sliced arrays, multiple row groups, write
  calls, option validation, norm levels. The additions are:
  - v2 data pages across the whole matrix;
  - both nullabilities for every type;
  - row groups placed relative to the cuts;
  - page row limits;
  - settings that must not move cuts;
  - cross-writer comparisons.
- Make chunker carry-over across row groups and write calls the most thoroughly tested
  property.
- Exact assertions: an edit changes the pages around it by exactly the edit's number of
  levels, and every other page is byte identical.
- A full run with all four writers finishes in a few minutes; the target is under five
  on a laptop.

## Non-goals

- Byte parity of page payloads across writers. The level and value encodings differ
  for reasons unrelated to chunking (see `de/parity.py`). `de parity` reports that, and
  this suite compares page boundaries only across writers.
- Testing the gear hash itself, or the chunker's internals. The suite only sees pages.
- Dictionary fallback: data in the dictionary scenarios stays below every writer's
  dictionary limits, so no fallback cut happens.
- Custom failure output: a failing test's id (writer, type, nulls, setting, layout) and
  pytest's assertion diff are enough for now.

## Anti-goals

- Assertions loose enough to pass a writer that resets its chunker at row group or
  write call boundaries. The suite keeps a test that proves a reset chunker fails it.
- Per-writer exceptions to the assertions. A writer that chunks differently fails.

## Constraints

- **Known bugs fail outright.** No xfail: parquet-rs's empty pages and wrong list cuts
  after the first row group fail until fixed upstream. (the user's decision)
- **Settings a writer can't express are skipped**, the way `de.formats.Parquet` raises
  `ValueError` for them today: v2 pages and row limits for DuckDB, write calls for
  parquet-java and DuckDB. A type a writer can't write as the same parquet type is
  skipped for that writer with the reason. For example, DuckDB writes float16 as FLOAT
  and fixed size binary as variable length binary. `Parquet.write`'s read-back check
  stays as it is. Anything a writer can write but chunks differently fails.
- parquet-java and DuckDB are skipped when `PARQUET_JAVA_CLASSPATH` / `DUCKDB_BINARY`
  aren't set, as in `test_pages.py` today.
- Follow the user's code preferences: plain pytest asserts, `tmp_path`/`tmp_path_factory`,
  public functions under test, no new dependencies, existing patterns over new
  abstractions.

## Design

### Where it lives

- `de/tests/test_cdc.py`, new. `TestContentDefinedChunking` in `de/tests/test_pages.py`
  is removed; everything it checks is covered by the new suite. `TestLayout`,
  `TestCompare` and `TestPersistence` stay in `test_pages.py`, they test de's own code.
- New comparison code goes into `de/pages.py` next to `compare` and `persistence`, and
  its unit tests into `test_pages.py` using the existing `layout_of` helper.

### Writers

Two changes to `de.formats` let the arrow-level cases reach both arrow writers, and let
write calls stay inside row groups for both:

- **parquet-rs takes tables in-process** through the Arrow C stream interface, instead
  of rewriting a temporary file:
  - Python hands over `pa.Table.__arrow_c_stream__()`, and Rust imports it with
    `arrow-array`'s `ffi` feature, which de already depends on.
  - Sliced arrays, dictionary arrays with their own dictionary, and lists whose null
    entries have non-empty child ranges then reach parquet-rs exactly as they reach
    parquet-cpp. A file rewrite would normalize all of them away.
  - With `batch_size`, every write call gets a slice of the table, with a non-zero
    offset.
  - `Path` sources, used by the benchmark, keep the file rewrite.
- **parquet-cpp write calls go through a small C++ program**, the way parquet-java runs
  through `ParquetRewrite.java`:
  - The program is `de/cpp/ParquetWrite.cc`. It's built with the system C++ compiler
    against pyarrow's own headers and libraries (`pyarrow.get_include()`,
    `get_library_dirs()`, `create_library_symlinks()`), and cached until its source
    changes.
  - It calls `parquet::arrow::FileWriter::WriteRecordBatch`, which writes into
    buffered row groups, once per batch. That's the only way to keep a parquet-cpp write
    call inside a row group: pyarrow's `ParquetWriter` and dataset writer both call
    `WriteTable`, which ends a row group at every call.
  - Without `batch_size`, `ParquetCpp` keeps writing in-process with
    `pq.write_table`.
  - This changes what `--batch-size` means for parquet-cpp in `de bench`: batches no
    longer end row groups, the same as for parquet-rs, and as `Parquet`'s docstring
    already describes.

Every written file is read back and compared with its table (`Parquet.write`'s check),
so the whole matrix also serves as the roundtrip test that parquet-rs's suite has for
lists and deeply nested types.

### Data

`de.synthetic.DataGenerator` is extended, not replaced:

- a `null_ratio` (default 0, so the benchmarks are unaffected); the suite uses 0 and
  0.1, like parquet-cpp's 10%. For lists: null lists, empty lists and null elements;
  for structs: null structs and null fields.
- the column types parquet-cpp tests, by arrow type, since the chunkers may hash the
  arrow width rather than the physical one (an int8 is an INT32 in parquet): bool, int8,
  int16, int32, int64, uint8, uint16, uint32, uint64, float16, float32, float64,
  decimal128, decimal256, date32, time32, time64, timestamp, binary, fixed size binary,
  string, list<int16>, list<int>, list<string>, struct<int, string>, struct<float64>,
  list<struct<int>>, list<list<int>>, list<struct<list<int>>>.
- two constant columns, which never match the gear hash and so are only cut at the
  maximum chunk size: the existing `zeros`, and an all-null int32 column, whose pages
  must still account for every row (parquet-rs once wrote none).
- arrow-only types: large binary, large string, dictionary<string>, duration, the JSON
  extension type, and lists whose null entries have non-empty child ranges. These run
  on parquet-cpp and parquet-rs only. parquet-java and DuckDB rewrite a parquet file
  and never see the arrow type:
  - a large string is a string to them;
  - a dictionary array is its strings;
  - a duration is an int64;
  - a null list's child values are gone.
  - parquet-cpp hashes a dictionary array's *indices*, not its strings
    (`chunker_internal.cc`). So the dictionary<string> base and edited tables must
    share one dictionary, so that an edit doesn't renumber the indices around it.
  - For decimal128, parquet-cpp hashes the 16-byte arrow value. parquet-java hashes
    the shorter fixed length bytes stored in parquet, and DuckDB leaves decimals uncut.
    Both fail, as differences in what the same parquet type hashes to.
- one type per table, so each table has a single column (a struct has several leaves),
  boundaries can be placed relative to that column's cuts, and a failure names its
  type.
- seeded and deterministic. Dictionary scenarios draw values from a bounded pool so no
  writer falls back to plain.

The edits are the generator's existing `insert_rows`, `delete_rows`, `update_rows` and
`append_rows`, plus prepend (an insert at position 0): insert once and twice, delete once
and twice, update once and twice, prepend and append. Edits are spaced at least two
maximum chunks apart and away from the ends, so their changes can't merge.

CDC parameters stay `min_chunk_size=8KiB, max_chunk_size=32KiB, norm_level=0`, except
for check 10's parameter grid and defaults. Each table is sized to a few dozen chunks.

### Files written

Each test scenario is a table (type × nulls × edit) written with a layout and a
setting:

- **Layouts.** The *oracle*, meaning a single row group with no page limits, so its cuts
  are exactly the CDC boundaries, as in `test_pages.py`'s `oracle` fixture. Then, for the
  unedited table:
  - a row group ending one row before, at, and one row after the column's first cut.
    These are computed from parquet-cpp's oracle and used for every writer, so all
    writers get the same row group sizes, even DuckDB, which doesn't cut nested
    leaves. For nested columns the cut's level offset is mapped to its row, and for
    structs the first leaf is targeted.
  - tiny row groups holding less than a quarter of the minimum chunk size, so a single
    chunk spans many row groups.
  - a large row group size not aligned with the cuts.
  - write calls in batches not aligned with the row groups or the cuts, and tiny batches
    below the minimum chunk size, for the writers that stream: parquet-cpp and
    parquet-rs, both with write calls inside row groups (see Writers).
  - a row limit per page smaller than the chunks, where supported.
  - for the insert-once, delete-once and update-once tables, a fixed row group size with
    the edit inside a middle row group.
- **Settings.** The baseline is v1 pages, dictionary off and uncompressed. The variants
  are dictionary on, v2 pages, v2 with dictionary, and snappy.

DuckDB rounds row groups up to 2048-row vectors. The assertions use the row group
starts found in the file, so they hold anyway, just not at the targeted rows.

### Assertions

1. **Carry-over (the core).** Every multi-row-group or multi-write file against that
   writer's oracle of the same table and setting: `de.pages.persistence` reports recall
   1, no extra cuts, alignment 1, resync 0 and no empty pages, for every column. The
   page-limit layout only requires recall 1, because its limit cuts are allowed as extras.
2. **Reset chunkers are caught.** The existing `test_resetting_chunkers_would_be_caught`
   moves over: it concatenates separately written row groups and must fail check 1, for
   every row group layout.
3. **Edits are local.** Each edited oracle against the unedited oracle, baseline
   setting, compared with a new `de.pages` function. It matches the two files' pages by
   payload hash with `difflib.SequenceMatcher`, as `de.parity.Parity.compare` does, and
   returns one change per run of differing pages, holding the base and edited level
   ranges and page counts. The assertions:
   - one change per edit;
   - each change covers its edit's position;
   - its level delta equals the edit's level delta exactly, computed from the data
     (rows for flat columns, levels for nested ones);
   - it replaces at most two pages with at most two pages;
   - every other page is byte identical.

   Prepend changes the first pages. Append changes only the last page and adds new
   ones. The constant columns only cut at the maximum size, so an edit changes only
   their last page; they get that as their own expectation. If parquet-cpp's output
   needs a looser page bound for some type, the bound is raised for every writer, never
   per writer.
4. **Edits in a middle row group.** The fixed-row-group files of an edited table against
   those of its unedited table. Every page of the edited file that is neither the first
   nor last page of its row group, nor part of an edit's change, is byte identical to a
   page of the unedited file. For updates every row group except the edited one is byte
   identical. This is checked at the baseline setting only, since a dictionary rebuilt
   for each row group changes the bytes of every later page.
5. **Settings don't move cuts.** Within a writer and scenario, every setting's page spans
   (`Layout.spans`, empty pages included) equal the baseline's. Dictionary encoding,
   page version and codec must not change where pages are cut.
6. **Chunk sizes.** In the oracle, every page except the last holds between the minimum
   and maximum chunk size of hashed bytes. The existing `test_chunk_sizes` handles the
   value that crosses the limit, and the constant columns cut at exactly the maximum.
   The hashed bytes per level (value width as the chunker hashes it, plus 2 bytes per
   definition and repetition level present) are taken from parquet-cpp's
   `chunker_internal.cc`. Nested columns are included if the accounting is exact for
   them, and left out if not.
7. **Same cuts as parquet-cpp.** Every file of every writer has the same page spans as
   a parquet-cpp reference of the same table and setting. parquet-cpp is the reference,
   as in `de parity`. This is what makes DuckDB's uncut list and struct leaves fail.
   - For the oracles, the reference is parquet-cpp's oracle.
   - For multiple row groups and write calls, the reference is parquet-cpp written
     with the **same row group starts as the writer's file**, read from that file:
     one `ParquetWriter.write_table` call per row group.

   The reference follows the writer's file because writers can put row groups in
   different places for reasons unrelated to chunking; DuckDB, for one, rounds row
   groups to whole vectors.
8. **Edge cases.**
   - No cuts without CDC (moved from `test_pages.py`).
   - An empty table writes a readable file with no non-empty pages.
   - A sliced table (offsets 512 and 1024, like parquet-cpp's) writes the same bytes as
     its unsliced copy. This runs on parquet-cpp and parquet-rs, which take tables
     in-process.
9. **Options are validated like parquet-cpp's.** The option sets come from parquet-cpp's
   `ChunkSizeParameterValidation`, both the rejected and the accepted ones, at their
   boundaries:
   - a minimum of 0 with maximum 16 rejected, and with 32 accepted;
   - 32 to 48 rejected, 32 to 64 accepted;
   - minimum 1 at norm level 1: maximum 33 rejected, 65 accepted;
   - minimum 0 at norm level 2: maximum 123 rejected, 128 accepted;
   - negative sizes, and a minimum not below the maximum, rejected.

   Every writer rejects the rejected sets, with the write failing. It writes the
   accepted ones with the same cuts as parquet-cpp. Each option set is its own test
   case.
10. **Chunking parameters give the same cuts as parquet-cpp.** The gear hash mask
    depends on the minimum, maximum and norm level. The implementations unit-test the
    mask values, which can't be seen from outside, so the suite compares cuts instead:
    - a grid of size pairs (4–16KiB, 8–32KiB, 64–256KiB) × norm levels -3 to 3, wherever
      parquet-cpp accepts them, on the unedited oracles of a few flat and nested types;
    - each writer's defaults (`use_cdc=True`, no parameters) against parquet-cpp's
      defaults, on tables large enough for several default-sized chunks.

### Test list

The parameters:

- *writer*: parquet-cpp, parquet-rs, parquet-java, duckdb.
- *type*: the column types above.
- *nulls*: 0% or 10%.
- *setting*: v1, dict, v2, v2-dict or snappy.
- *layout*: rg-before-cut, rg-at-cut, rg-after-cut, tiny-rgs, large-rgs, batches or
  tiny-batches.
- *edit*: insert, insert-twice, delete, delete-twice, update, update-twice, prepend or
  append.

`de/tests/test_cdc.py`, within one row group:

| Test | Parameters | Check |
|---|---|---|
| `test_no_cuts_without_cdc` | writer, type | 8 |
| `test_chunk_sizes` | writer, type, nulls | 6 |
| `test_constant_column_cuts_at_max_size` | writer, constant column (zeros, all-null) | 6 |
| `test_same_cuts_as_parquet_cpp` | writer, type, nulls, setting | 7 |
| `test_chunking_parameters_match_parquet_cpp` | writer, a few types, size pair, norm level | 10 |
| `test_default_options_match_parquet_cpp` | writer, a few types | 10 |
| `test_edit_is_local` | writer, type, nulls, edit | 3 |

`de/tests/test_cdc.py`, across row groups and write calls:

| Test | Parameters | Check |
|---|---|---|
| `test_chunker_carries_over_row_groups` | writer, type, nulls, setting, layout | 1 |
| `test_same_cuts_as_parquet_cpp_across_row_groups` | writer, type, nulls, setting, layout | 7 |
| `test_settings_dont_move_cuts` | writer, type, nulls, setting, layout or oracle | 5 |
| `test_edit_in_middle_row_group` | writer, type, nulls, edit (insert, delete, update) | 4 |
| `test_page_row_limit_keeps_cdc_cuts` | writer, type, nulls | 1 |
| `test_resetting_chunkers_would_be_caught` | layout (parquet-cpp) | 2 |

`de/tests/test_cdc.py`, edge cases:

| Test | Parameters | Check |
|---|---|---|
| `test_empty_table` | writer, type | 8 |
| `test_sliced_table_writes_the_same_bytes` | parquet-cpp and parquet-rs, type, offset | 8 |
| `test_options_are_validated_like_parquet_cpp` | writer, option set | 9 |

Supporting unit tests, next to the code they cover:

| File | Tests |
|---|---|
| `test_pages.py` | the new change-finding function on hand-built layouts: identical files, one and two edits, insert, delete, update, prepend, append, a change spanning two pages |
| `test_synthetic.py` | the null ratio at every nesting level, each new type, the shared dictionary of edited dictionary tables, the level delta of each edit |
| `test_formats.py` | parquet-java writing several files in one JVM, one failing job not failing the others; parquet-cpp's write calls staying inside row groups; parquet-rs writing a sliced and a dictionary table in-process |

### Writing fast enough

- **Write once, assert many.** The matrix is plain data at the top of `test_cdc.py`. A
  session-scoped fixture per writer writes all of that writer's files on first use, in
  two phases: the oracles, then the layouts computed from them. Tests only read
  `Layout`s. Writes run in a thread pool, as `ParityCheck._write` does.
- **One JVM for many files.** `de/java/ParquetRewrite.java` reads its jobs from stdin,
  one per line (source, destination, options), instead of from its arguments, and
  writes them all in one JVM. A failing job reports its error and the others carry on.
  `ParquetJava` sends a single job for one `write`, and the batch for the suite, split
  over a few JVMs in parallel. A batched write's failure fails only the tests of its
  own files.
- The runtime of each writer is measured once the suite works, and reported with the
  plan's results.

## Calls I made (not decided by the user)

- the new module replaces `TestContentDefinedChunking` instead of living beside it.
- `DataGenerator` is extended rather than porting parquet-cpp's `hash(seed, i)` data.
- across writers only page boundaries are compared, and payload bytes are left to
  `de parity`. Today's `test_pages_match_parquet_cpp` compares payloads too, which is
  why parquet-java's nested columns fail it.
- DuckDB not cutting list and struct leaves counts as a failure, following the
  known-bugs-fail rule, rather than a skip.
- every type gets every row group layout and setting. My earlier suggestion of a
  narrower type set for row groups was dropped, since the batching makes it affordable
  and carry-over is the priority.
- the suite runs in a plain `pytest` run, with no marker.
- the first-cut row group sizes come from parquet-cpp's oracle.
- arrow-only types run on parquet-cpp and parquet-rs only. A type a writer writes as a
  different parquet type is skipped for it, and the read-back check in `Parquet.write`
  isn't relaxed.
- parquet-rs takes tables in-process through the Arrow C stream interface, and
  parquet-cpp's write calls go through a C++ program built against pyarrow. The
  arrow-level cases and write calls inside row groups then reach both arrow writers.
  This makes a C++ compiler a requirement for parquet-cpp's streaming, and changes
  what `--batch-size` means for parquet-cpp in `de bench`.
- the parameter grid and the defaults apply to the unedited oracles of a few types
  only. Updates always change the value, even for bool and other low-cardinality
  types. `zeros` with nulls is treated as an ordinary column.

## Left to the builder

- names of the new `de.pages` function and its result type, and of the matrix's data
  types;
- the exact table sizes, edit sizes and positions within the constraints above;
- how the matrix is cut back if the five minute target is missed: settings on edited
  files go first, carry-over coverage goes last;
- the stdin job format of `ParquetRewrite.java`;
- whether `ParityCheck` also uses the batched parquet-java writes;
- how the C++ program is built and cached, and how it gets its table (an Arrow IPC
  file is enough, since slices are tested in-process);
- which types the parameter grid and the defaults run on.

## Prior art

| Suite | Where | What it covers |
|---|---|---|
| parquet-cpp | `cpp/src/parquet/chunker_internal_test.cc` (`~/Workspace/marrow/integration/arrow`) | parameter validation, hash masks, a last chunk adding no page across write calls; 10 edit, empty table and array offset tests × 31 types with dictionary on and off, one nullability per type, v2 for 2 types; 4 multiple row group tests on 3 columns. Slips: `UpdateOnce` asserts that the edited row group is identical, and `Append`'s page count assertion is reversed |
| parquet-rs 58.4 / main | `parquet/src/column/chunker/cdc.rs` | a port of parquet-cpp's tests for 9 flat types, no v2; roundtrips of lists, deeply nested types and non-empty null list segments; main adds an all-null column writing data pages |
| parquet-rs, `cdc-strict-tests` | uncommitted in `~/Workspace/arrow-rs` | nested types in the edit tests; strict `assert_edited_pages`, `assert_updated_pages` and `assert_shifted_pages`; row group independence at 64Ki rows, 400 rows and the first page's length, nested with v1 and v2, and with 10007-row write calls |
| parquet-java | `TestContentDefinedChunker`, `TestColumnWriterChunking`, hadoop's `TestContentDefinedChunking` (`~/Workspace/parquet-java`) | chunker units; 8 columns × v1/v2 × 8 edits with exact deltas over page bytes; golden pyarrow files |

Everything in this table that can be seen in a written file maps to a check above. The
hash mask values and direct column writer calls are internal; the suite covers them
through their effect on the cuts (checks 9 and 10) and through write calls (check 1).
