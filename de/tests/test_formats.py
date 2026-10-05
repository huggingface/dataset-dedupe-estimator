from dataclasses import fields, replace
import gzip
import json
import os
import sqlite3
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from de.formats import (
    CdcParams,
    JsonLines,
    Parquet,
    ParquetCpp,
    ParquetJava,
    ParquetRs,
    Sqlite,
)
from de.synthetic import DataGenerator


@pytest.fixture
def table():
    return pa.table({"a": [1, 2, 3], "b": ["x", "y", "z"]})


class TestParquetCpp:
    def test_suffix(self):
        assert ParquetCpp(use_cdc=False).suffix == "parquet"

    def test_paramstem_default(self):
        assert ParquetCpp(use_cdc=False).paramstem == ""

    def test_paramstem_with_compression(self):
        assert ParquetCpp(use_cdc=False, compression="snappy").paramstem == "snappy"

    def test_paramstem_with_cdc(self):
        assert ParquetCpp(use_cdc=True).paramstem == "cdc"

    def test_paramstem_with_compression_and_cdc(self):
        assert ParquetCpp(use_cdc=True, compression="zstd").paramstem == "zstd cdc"

    def test_derive_path_plain(self, tmp_path):
        assert (
            ParquetCpp(use_cdc=False).derive_path("data", tmp_path)
            == tmp_path / "data.parquet"
        )

    def test_derive_path_with_compression(self, tmp_path):
        assert (
            ParquetCpp(use_cdc=False, compression="snappy").derive_path(
                "data", tmp_path
            )
            == tmp_path / "data-snappy.parquet"
        )

    def test_derive_path_with_cdc(self, tmp_path):
        assert (
            ParquetCpp(use_cdc=True).derive_path("data", tmp_path)
            == tmp_path / "data-cdc.parquet"
        )

    def test_derive_path_with_compression_and_cdc(self, tmp_path):
        assert (
            ParquetCpp(use_cdc=True, compression="zstd").derive_path("data", tmp_path)
            == tmp_path / "data-zstd-cdc.parquet"
        )

    def test_write_reads_back(self, tmp_path, table):
        path = ParquetCpp(use_cdc=False).write("out", table, tmp_path)
        assert pq.read_table(path).equals(table)

    def test_write_with_compression(self, tmp_path, table):
        path = ParquetCpp(use_cdc=False, compression="snappy").write(
            "out", table, tmp_path
        )
        assert pq.read_metadata(path).row_group(0).column(0).compression == "SNAPPY"

    def test_write_with_row_group_size(self, tmp_path):
        big_table = pa.table({"x": list(range(100))})
        path = ParquetCpp(use_cdc=False, row_group_size=10).write(
            "out", big_table, tmp_path
        )
        assert pq.read_metadata(path).num_row_groups == 10

    def test_write_with_data_page_size(self, tmp_path, table):
        path = ParquetCpp(use_cdc=False, data_page_size=512).write(
            "out", table, tmp_path
        )
        assert pq.read_table(path).equals(table)

    def test_write_without_dictionary(self, tmp_path, table):
        path = ParquetCpp(use_cdc=False, use_dictionary=False).write(
            "out", table, tmp_path
        )
        assert pq.read_table(path).equals(table)

    def test_write_from_path_preserves_data(self, tmp_path, table):
        src = tmp_path / "src.parquet"
        pq.write_table(table, src)
        dest = ParquetCpp(use_cdc=False).write("dst", src, tmp_path)
        assert pq.read_table(dest).equals(table)

    def test_write_from_path_with_row_group_size(self, tmp_path):
        big_table = pa.table({"x": list(range(100))})
        src = tmp_path / "src.parquet"
        pq.write_table(big_table, src)
        dest = ParquetCpp(use_cdc=False, row_group_size=20).write("dst", src, tmp_path)
        assert pq.read_metadata(dest).num_row_groups == 5


class TestParquetRs:
    def test_paramstem_with_row_group_size(self):
        fmt = ParquetRs(use_cdc=True, compression="zstd", row_group_size=2**16)
        assert fmt.paramstem == "zstd cdc rg=64Ki"

    def test_write_with_row_group_size(self, tmp_path):
        big_table = pa.table({"x": list(range(100))})
        path = ParquetRs(use_cdc=False, row_group_size=10).write(
            "out", big_table, tmp_path
        )
        assert pq.read_metadata(path).num_row_groups == 10
        assert pq.read_table(path).equals(big_table)

    def test_write_without_dictionary(self, tmp_path, table):
        path = ParquetRs(use_cdc=False, use_dictionary=False).write(
            "out", table, tmp_path
        )
        column = pq.read_metadata(path).row_group(0).column(0)
        assert not column.has_dictionary_page
        assert pq.read_table(path).equals(table)

    def test_table_source_leaves_no_temp_files(self, tmp_path, table, monkeypatch):
        tmp = tmp_path / "tmp"
        tmp.mkdir()
        monkeypatch.setattr(tempfile, "tempdir", str(tmp))
        ParquetRs(use_cdc=True).write("out", table, tmp_path)
        assert list(tmp.iterdir()) == []


CDC = CdcParams(min_chunk_size=8 * 1024, max_chunk_size=32 * 1024, norm_level=0)


def row_group_sizes(path):
    metadata = pq.read_metadata(path)
    return [metadata.row_group(i).num_rows for i in range(metadata.num_row_groups)]


class TestParquet:
    def test_paramstem_all_fields(self):
        fmt = ParquetCpp(
            use_cdc=CDC,
            compression="snappy",
            use_dictionary=False,
            row_group_size=2**16,
            data_page_size=2**30,
            max_rows_per_page=20480,
            batch_size=100_000,
        )
        assert fmt.paramstem == (
            "snappy cdc=8Ki-32Ki-n0 nodict rg=64Ki dp=1Gi rpp=20Ki stream=100000"
        )

    def test_paramstem_sizes_are_exact(self):
        assert ParquetRs(use_cdc=False, row_group_size=300_000).paramstem == (
            "rg=300000"
        )
        assert ParquetRs(use_cdc=False, row_group_size=299_999).paramstem == (
            "rg=299999"
        )

    def test_paramstem_norm_levels_differ(self, tmp_path):
        paths = {
            ParquetRs(use_cdc=replace(CDC, norm_level=level)).derive_path("x", tmp_path)
            for level in (-1, 0, 1)
        }
        assert len(paths) == 3

    def test_every_field_changes_paramstem(self):
        changed = {
            "use_cdc": True,
            "compression": "snappy",
            "use_dictionary": False,
            "row_group_size": 10,
            "data_page_size": 1024,
            "max_rows_per_page": 10,
            "batch_size": 10,
        }
        assert {f.name for f in fields(Parquet)} == changed.keys()
        base = ParquetCpp(use_cdc=False)
        for name, value in changed.items():
            assert replace(base, **{name: value}).paramstem != base.paramstem, name

    @pytest.mark.parametrize(
        ("writer", "batch_size", "expected"),
        [
            (ParquetCpp, None, [30, 30, 30, 10]),
            (ParquetCpp, 25, [25, 25, 25, 25]),
            (ParquetRs, None, [30, 30, 30, 10]),
            (ParquetRs, 25, [30, 30, 30, 10]),
        ],
    )
    def test_row_groups_per_mode(self, tmp_path, writer, batch_size, expected):
        big_table = pa.table({"x": list(range(100))})
        fmt = writer(use_cdc=False, row_group_size=30, batch_size=batch_size)
        path = fmt.write("out", big_table, tmp_path)
        assert row_group_sizes(path) == expected

    @pytest.mark.parametrize("writer", [ParquetCpp, ParquetRs])
    @pytest.mark.parametrize("batch_size", [None, 64])
    def test_table_and_path_sources_are_identical(self, tmp_path, writer, batch_size):
        data = DataGenerator({"a": "int", "b": "str"}).generate_table(500)
        src = tmp_path / "src.parquet"
        pq.write_table(data, src, row_group_size=7)
        fmt = writer(use_cdc=CDC, compression="snappy", batch_size=batch_size)
        from_table = fmt.write("table", data, tmp_path)
        from_path = fmt.write("path", src, tmp_path)
        assert from_table.read_bytes() == from_path.read_bytes()

    @pytest.mark.parametrize("writer", [ParquetCpp, ParquetRs])
    @pytest.mark.parametrize("batch_size", [None, 100])
    def test_nested_schema_round_trips(self, tmp_path, writer, batch_size):
        schema = {"a": "int", "b": "str", "c": ["int"], "d": {"x": "float"}}
        data = DataGenerator(schema).generate_table(1000)
        fmt = writer(use_cdc=CDC, use_dictionary=False, batch_size=batch_size)
        path = fmt.write("out", data, tmp_path)
        assert pq.read_table(path).equals(data)


needs_java = pytest.mark.skipif(
    not os.environ.get("PARQUET_JAVA_CLASSPATH"),
    reason="PARQUET_JAVA_CLASSPATH is not set",
)


@needs_java
class TestParquetJava:
    def test_row_groups(self, tmp_path):
        big_table = pa.table({"x": list(range(100))})
        path = ParquetJava(use_cdc=False, row_group_size=30).write(
            "out", big_table, tmp_path
        )
        assert row_group_sizes(path) == [30, 30, 30, 10]
        assert pq.read_metadata(path).created_by.startswith("parquet-mr")

    def test_table_and_path_sources_are_identical(self, tmp_path):
        data = DataGenerator({"a": "int", "b": "str"}).generate_table(500)
        src = tmp_path / "src.parquet"
        pq.write_table(data, src, row_group_size=7)
        fmt = ParquetJava(use_cdc=CDC, compression="snappy", row_group_size=100)
        from_table = fmt.write("table", data, tmp_path)
        from_path = fmt.write("path", src, tmp_path)
        assert from_table.read_bytes() == from_path.read_bytes()

    def test_nested_schema_round_trips(self, tmp_path):
        schema = {"a": "int", "b": "str", "c": ["int"], "d": {"x": "float"}}
        data = DataGenerator(schema).generate_table(1000)
        path = ParquetJava(use_cdc=CDC, use_dictionary=False).write(
            "out", data, tmp_path
        )
        assert pq.read_table(path).equals(data)

    def test_streaming_is_not_supported(self, tmp_path, table):
        with pytest.raises(ValueError, match="no write calls"):
            ParquetJava(use_cdc=False, batch_size=10).write("out", table, tmp_path)


class TestJsonLines:
    def test_suffix(self):
        assert JsonLines().suffix == "jsonlines"

    def test_paramstem_default(self):
        assert JsonLines().paramstem == ""

    def test_paramstem_with_compression(self):
        assert JsonLines(compression="gzip").paramstem == "gzip"

    def test_derive_path(self, tmp_path):
        assert JsonLines().derive_path("data", tmp_path) == tmp_path / "data.jsonlines"

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

    def test_paramstem_default(self):
        assert Sqlite().paramstem == ""

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
