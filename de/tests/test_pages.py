import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from de.formats import CdcParams, ParquetCpp, ParquetRs
from de.pages import Change, Layout, Persistence, changes, compare, persistence
from de.synthetic import DataGenerator

CDC = CdcParams(min_chunk_size=8 * 1024, max_chunk_size=32 * 1024, norm_level=0)


@pytest.fixture(scope="module")
def ints():
    return DataGenerator({"a": "int"}).generate_table(200_000)


def without_page_limits(writer, use_cdc=CDC, **kwargs):
    return writer(
        use_cdc=use_cdc,
        use_dictionary=False,
        data_page_size=2**30,
        max_rows_per_page=2**30,
        **kwargs,
    )


def write_ints(path, num_rows=100, **kwargs):
    table = pa.table({"x": list(range(num_rows))})
    pq.write_table(table, path, use_dictionary=False, compression="none", **kwargs)
    return path


def layout_of(*row_groups):
    """Single column layout from the data page lengths of each row group."""
    return Layout(
        [
            {
                "column": 0,
                "row_group": row_group,
                "page_type": "data_page_v1",
                "num_values": num_values,
            }
            for row_group, lengths in enumerate(row_groups)
            for num_values in lengths
        ]
    )


class TestLayout:
    @pytest.mark.parametrize("writer", [ParquetCpp, ParquetRs])
    @pytest.mark.parametrize("use_dictionary", [False, True])
    def test_pages_cover_column_chunks(self, tmp_path, writer, use_dictionary):
        data = DataGenerator({"a": "int", "b": "str", "c": ["int"]}).generate_table(
            5000
        )
        fmt = writer(
            use_cdc=CDC,
            compression="snappy",
            use_dictionary=use_dictionary,
            row_group_size=2000,
        )
        path = fmt.write("out", data, tmp_path)
        layout = Layout.read(path)
        metadata = pq.read_metadata(path)
        for rg in range(metadata.num_row_groups):
            for col in range(metadata.num_columns):
                chunk = metadata.row_group(rg).column(col)
                pages = [
                    p
                    for p in layout.pages
                    if p["row_group"] == rg and p["column"] == col
                ]
                assert pages[0]["offset"] == (
                    chunk.dictionary_page_offset
                    if chunk.has_dictionary_page
                    else chunk.data_page_offset
                )
                for page, following in zip(pages, pages[1:]):
                    end = (
                        page["offset"] + page["header_bytes"] + page["compressed_bytes"]
                    )
                    assert end == following["offset"]
                assert chunk.total_compressed_size == sum(
                    p["header_bytes"] + p["compressed_bytes"] for p in pages
                )
                data_pages = [p for p in pages if p["page_type"] != "dictionary"]
                assert sum(p["num_values"] for p in data_pages) == chunk.num_values
                assert (pages[0]["page_type"] == "dictionary") == (
                    chunk.has_dictionary_page
                )

    def test_known_cuts(self, tmp_path):
        path = write_ints(
            tmp_path / "x.parquet", row_group_size=50, max_rows_per_page=10
        )
        layout = Layout.read(path)
        assert layout.columns == [0]
        assert layout.cuts(0) == [10, 20, 30, 40, 50, 60, 70, 80, 90]
        assert layout.row_group_starts(0) == [50]
        assert {p["page_type"] for p in layout.pages} == {"data_page_v1"}
        assert {p["encoding"] for p in layout.pages} == {"plain"}

    def test_data_page_v2(self, tmp_path):
        path = write_ints(
            tmp_path / "x.parquet", max_rows_per_page=10, data_page_version="2.0"
        )
        layout = Layout.read(path)
        assert {p["page_type"] for p in layout.pages} == {"data_page_v2"}
        assert layout.cuts(0) == list(range(10, 100, 10))

    def test_multiple_columns(self, tmp_path):
        path = tmp_path / "x.parquet"
        pq.write_table(pa.table({"a": [1, 2], "b": ["x", "y"]}), path)
        layout = Layout.read(path)
        assert layout.columns == [0, 1]
        assert {p["path"] for p in layout.pages} == {"a", "b"}

    def test_empty_pages(self):
        layout = layout_of([10, 0, 5], [0, 3])
        assert layout.empty_pages(0) == 2
        assert layout.cuts(0) == [10, 15]
        assert layout.row_group_starts(0) == [15]


class TestCompare:
    def test_identical_files(self, tmp_path):
        path = write_ints(tmp_path / "x.parquet", max_rows_per_page=10)
        [diff] = compare(Layout.read(path), Layout.read(path))
        assert diff.common_cuts == 9
        assert (diff.left_only_cuts, diff.right_only_cuts) == (0, 0)
        assert diff.first_divergence is None
        assert diff.new_bytes == 0
        assert diff.new_payloads == 0

    def test_only_headers_differ(self, tmp_path):
        left = write_ints(tmp_path / "left.parquet", write_statistics=False)
        right = write_ints(tmp_path / "right.parquet")
        [diff] = compare(Layout.read(left), Layout.read(right))
        # the statistics are in the page headers, the levels and values are the same
        assert diff.new_bytes == diff.right_bytes
        assert diff.new_payloads == 0

    def test_different_page_limits(self, tmp_path):
        left = write_ints(
            tmp_path / "left.parquet", row_group_size=50, max_rows_per_page=10
        )
        right = write_ints(
            tmp_path / "right.parquet", row_group_size=50, max_rows_per_page=20
        )
        [diff] = compare(Layout.read(left), Layout.read(right))
        # left cuts every 10 rows, right at 20, 40, 50 (row group), 70, 90
        assert (diff.left_pages, diff.right_pages) == (10, 6)
        assert diff.common_cuts == 5
        assert (diff.left_only_cuts, diff.right_only_cuts) == (4, 0)
        assert diff.first_divergence == 10
        # the 10-row pages [40, 50) and [90, 100) are identical in both files
        assert 0 < diff.new_bytes < diff.right_bytes
        assert diff.new_payloads == 4

    def test_column_missing_on_the_left(self, tmp_path):
        left, right = tmp_path / "left.parquet", tmp_path / "right.parquet"
        pq.write_table(pa.table({"a": [1, 2]}), left)
        pq.write_table(pa.table({"a": [1, 2], "b": ["x", "y"]}), right)
        diffs = compare(Layout.read(left), Layout.read(right))
        assert [diff.left_pages for diff in diffs] == [1, 0]
        assert diffs[1].new_bytes == diffs[1].right_bytes


def layout_with_payloads(*row_groups):
    """Single column layout from the (num_values, payload) data pages of each row group."""
    return Layout(
        [
            {
                "column": 0,
                "row_group": row_group,
                "page_type": "data_page_v1",
                "num_values": num_values,
                "payload_hash": payload,
            }
            for row_group, pages in enumerate(row_groups)
            for num_values, payload in pages
        ]
    )


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
        found = changes(layout_with_payloads(BASE), edited, 0)
        assert [change.base for change in found] == [(10, 20), (30, 40)]

    def test_prepend_and_append(self):
        edited = layout_with_payloads([(13, "p"), *BASE[1:4], (12, "q")])
        found = changes(layout_with_payloads(BASE), edited, 0)
        # the edited file's levels after the prepended page are 3 levels later
        assert [(c.base, c.edited) for c in found] == [
            ((0, 10), (0, 13)),
            ((40, 50), (43, 55)),
        ]

    def test_pure_insertion(self):
        edited = layout_with_payloads([*BASE[:2], (3, "n"), *BASE[2:]])
        [change] = changes(layout_with_payloads(BASE), edited, 0)
        assert change == Change(0, base=(20, 20), edited=(20, 23), removed=0, added=1)

    def test_insertion_at_the_end(self):
        edited = layout_with_payloads([*BASE, (3, "n")])
        [change] = changes(layout_with_payloads(BASE), edited, 0)
        assert change == Change(0, base=(50, 50), edited=(50, 53), removed=0, added=1)

    def test_dictionary_pages_are_ignored(self):
        def with_dictionary(payload):
            page = {
                "column": 0,
                "row_group": 0,
                "page_type": "dictionary",
                "num_values": 5,
                "payload_hash": payload,
            }
            return Layout([page, *layout_with_payloads(BASE).pages])

        assert changes(with_dictionary("d1"), with_dictionary("d2"), 0) == []

    def test_constant_pages_change_only_at_the_end(self):
        base = layout_with_payloads([(10, "z")] * 4 + [(5, "w")])
        edited = layout_with_payloads([(10, "z")] * 4 + [(7, "v")])
        [change] = changes(base, edited, 0)
        assert (change.base, change.edited) == ((40, 45), (40, 47))

    def test_row_group_boundaries_dont_split_matches(self):
        edited = layout_with_payloads(BASE[:2], BASE[2:])
        assert changes(layout_with_payloads(BASE), edited, 0) == []


class TestPersistence:
    oracle = layout_of([10, 10, 10, 10])  # cdc cuts 10, 20, 30

    def test_kept_state(self):
        layout = layout_of([10, 5], [5, 10, 10])  # row group starts at 15
        assert persistence(layout, self.oracle) == [
            Persistence(column=0, recall=1.0, extra=0, aligned=1.0, resync=0.0, empty=0)
        ]

    def test_reset_state_resyncs_later(self):
        layout = layout_of([10, 5], [12, 3, 10])  # a fresh chunker cuts at 27
        [result] = persistence(layout, self.oracle)
        assert result.recall == 2 / 3
        assert result.extra == 1
        assert result.aligned == 25 / 40
        assert result.resync == 15

    def test_empty_page(self):
        layout = layout_of([10, 10, 0, 10, 10])
        [result] = persistence(layout, self.oracle)
        assert (result.recall, result.extra, result.aligned) == (1.0, 0, 1.0)
        assert result.empty == 1

    def test_arrow_rs_stream_matches_single_shot(self, tmp_path, ints):
        single = without_page_limits(ParquetRs, row_group_size=30_000)
        stream = without_page_limits(ParquetRs, row_group_size=30_000, batch_size=7000)
        assert (
            single.write("single", ints, tmp_path).read_bytes()
            == stream.write("stream", ints, tmp_path).read_bytes()
        )
