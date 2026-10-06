import os

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from de.formats import CdcParams, ParquetCpp, ParquetJava, ParquetRs
from de.pages import Layout, Persistence, compare, persistence
from de.synthetic import DataGenerator

CDC = CdcParams(min_chunk_size=8 * 1024, max_chunk_size=32 * 1024, norm_level=0)


@pytest.fixture(scope="module")
def ints():
    return DataGenerator({"a": "int"}).generate_table(200_000)


def without_page_limits(writer, **kwargs):
    return writer(
        use_cdc=CDC,
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

    def test_column_missing_on_the_left(self, tmp_path):
        left, right = tmp_path / "left.parquet", tmp_path / "right.parquet"
        pq.write_table(pa.table({"a": [1, 2]}), left)
        pq.write_table(pa.table({"a": [1, 2], "b": ["x", "y"]}), right)
        diffs = compare(Layout.read(left), Layout.read(right))
        assert [diff.left_pages for diff in diffs] == [1, 0]
        assert diffs[1].new_bytes == diffs[1].right_bytes


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

    def test_arrow_rs_keeps_state_across_row_groups(self, tmp_path, ints):
        oracle = without_page_limits(ParquetRs, row_group_size=len(ints))
        layout = without_page_limits(ParquetRs, row_group_size=30_000)
        [result] = persistence(
            Layout.read(layout.write("layout", ints, tmp_path)),
            Layout.read(oracle.write("oracle", ints, tmp_path)),
        )
        assert result == Persistence(
            column=0, recall=1.0, extra=0, aligned=1.0, resync=0.0, empty=0
        )

    def test_arrow_rs_stream_matches_single_shot(self, tmp_path, ints):
        single = without_page_limits(ParquetRs, row_group_size=30_000)
        stream = without_page_limits(ParquetRs, row_group_size=30_000, batch_size=7000)
        assert (
            single.write("single", ints, tmp_path).read_bytes()
            == stream.write("stream", ints, tmp_path).read_bytes()
        )

    @pytest.mark.parametrize(
        "writer",
        [
            ParquetRs,
            pytest.param(
                ParquetJava,
                marks=pytest.mark.skipif(
                    not os.environ.get("PARQUET_JAVA_CLASSPATH"),
                    reason="PARQUET_JAVA_CLASSPATH is not set",
                ),
            ),
        ],
    )
    def test_cdc_cuts_match_across_implementations(self, tmp_path, ints, writer):
        cpp = without_page_limits(ParquetCpp, row_group_size=len(ints))
        other = without_page_limits(writer, row_group_size=len(ints))
        cpp_cuts = Layout.read(cpp.write("cpp", ints, tmp_path)).cuts(0)
        other_cuts = Layout.read(other.write("other", ints, tmp_path)).cuts(0)
        assert len(cpp_cuts) > 10
        assert other_cuts == cpp_cuts
