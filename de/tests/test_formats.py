import gzip
import json
import sqlite3

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from de.formats import JsonLines, ParquetCpp, Sqlite


@pytest.fixture
def table():
    return pa.table({"a": [1, 2, 3], "b": ["x", "y", "z"]})


class TestParquetCpp:
    def test_suffix(self):
        assert ParquetCpp().suffix == "parquet"

    def test_kind_default(self):
        assert ParquetCpp().kind == "parquet"

    def test_kind_with_compression(self):
        assert ParquetCpp(compression="snappy").kind == "parquet-snappy"

    def test_kind_with_cdc(self):
        assert ParquetCpp(use_cdc=True).kind == "parquet-cdc"

    def test_kind_with_compression_and_cdc(self):
        assert ParquetCpp(compression="zstd", use_cdc=True).kind == "parquet-zstd-cdc"

    def test_derive_path_plain(self, tmp_path):
        assert (
            ParquetCpp().derive_path("data", tmp_path)
            == tmp_path / "parquet" / "data.parquet"
        )

    def test_derive_path_with_prefix(self, tmp_path):
        assert (
            ParquetCpp().derive_path("data", tmp_path, "pre")
            == tmp_path / "parquet" / "pre-data.parquet"
        )

    def test_derive_path_with_compression(self, tmp_path):
        assert (
            ParquetCpp(compression="snappy").derive_path("data", tmp_path)
            == tmp_path / "parquet-snappy" / "data.parquet"
        )

    def test_derive_path_with_cdc(self, tmp_path):
        assert (
            ParquetCpp(use_cdc=True).derive_path("data", tmp_path)
            == tmp_path / "parquet-cdc" / "data.parquet"
        )

    def test_derive_path_with_compression_and_cdc(self, tmp_path):
        assert (
            ParquetCpp(compression="zstd", use_cdc=True).derive_path("data", tmp_path)
            == tmp_path / "parquet-zstd-cdc" / "data.parquet"
        )

    def test_write_reads_back(self, tmp_path, table):
        path = ParquetCpp().write("out", table, tmp_path)
        assert pq.read_table(path).equals(table)

    def test_write_with_compression(self, tmp_path, table):
        path = ParquetCpp(compression="snappy").write("out", table, tmp_path)
        assert pq.read_metadata(path).row_group(0).column(0).compression == "SNAPPY"

    def test_write_with_row_group_size(self, tmp_path):
        big_table = pa.table({"x": list(range(100))})
        path = ParquetCpp(row_group_size=10).write("out", big_table, tmp_path)
        assert pq.read_metadata(path).num_row_groups == 10

    def test_write_with_data_page_size(self, tmp_path, table):
        path = ParquetCpp(data_page_size=512).write("out", table, tmp_path)
        assert pq.read_table(path).equals(table)

    def test_write_without_dictionary(self, tmp_path, table):
        path = ParquetCpp(use_dictionary=False).write("out", table, tmp_path)
        assert pq.read_table(path).equals(table)

    def test_write_from_path_preserves_data(self, tmp_path, table):
        src = tmp_path / "src.parquet"
        pq.write_table(table, src)
        dest = ParquetCpp().write("dst", src, tmp_path)
        assert pq.read_table(dest).equals(table)

    def test_write_from_path_with_row_group_size(self, tmp_path):
        big_table = pa.table({"x": list(range(100))})
        src = tmp_path / "src.parquet"
        pq.write_table(big_table, src)
        dest = ParquetCpp(row_group_size=20).write("dst", src, tmp_path)
        assert pq.read_metadata(dest).num_row_groups == 5


class TestJsonLines:
    def test_suffix(self):
        assert JsonLines().suffix == "jsonlines"

    def test_kind_default(self):
        assert JsonLines().kind == "jsonlines"

    def test_kind_with_compression(self):
        assert JsonLines(compression="gzip").kind == "jsonlines-gzip"

    def test_derive_path(self, tmp_path):
        assert (
            JsonLines().derive_path("data", tmp_path)
            == tmp_path / "jsonlines" / "data.jsonlines"
        )

    def test_write_reads_back(self, tmp_path, table):
        path = JsonLines().write("out", table, tmp_path)
        records = [json.loads(line) for line in path.read_text().strip().splitlines()]
        assert records == [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}, {"a": 3, "b": "z"}]

    def test_write_with_gzip(self, tmp_path, table):
        path = JsonLines(compression="gzip").write("out", table, tmp_path)
        with gzip.open(path, "rt") as f:
            lines = f.read().strip().splitlines()
        assert len(lines) == 3


class TestSqlite:
    def test_suffix(self):
        assert Sqlite().suffix == "sqlite"

    def test_kind(self):
        assert Sqlite().kind == "sqlite"

    def test_write_reads_back(self, tmp_path, table):
        path = Sqlite().write("out", table, tmp_path)
        con = sqlite3.connect(path)
        rows = con.execute("SELECT a, b FROM 'table' ORDER BY a").fetchall()
        con.close()
        assert rows == [(1, "x"), (2, "y"), (3, "z")]

    def test_write_overwrites_existing(self, tmp_path, table):
        path = Sqlite().write("out", table, tmp_path)
        new_table = pa.table({"a": [9], "b": ["q"]})
        Sqlite().write("out", new_table, tmp_path)
        con = sqlite3.connect(path)
        rows = con.execute("SELECT a, b FROM 'table'").fetchall()
        con.close()
        assert rows == [(9, "q")]
