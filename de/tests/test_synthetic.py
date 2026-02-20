import pyarrow as pa
import pytest

from de.synthetic import FakeDataGenerator


SCHEMA = {"a": "int", "b": "str"}


@pytest.fixture
def gen():
    return FakeDataGenerator(SCHEMA, seed=42)


@pytest.fixture
def table(gen):
    return gen.generate_table(100)


class TestDeterminism:
    @pytest.mark.parametrize(
        "dtype", ["int", "float", "str", "bool", ["int"], {"x": "int", "y": "str"}]
    )
    def test_same_seed_same_output(self, dtype):
        a = FakeDataGenerator({"f": dtype}, seed=0).generate_data(dtype, 20)
        b = FakeDataGenerator({"f": dtype}, seed=0).generate_data(dtype, 20)
        assert pa.array(a).equals(pa.array(b))

    @pytest.mark.parametrize("dtype", ["int", "float", "str", "bool"])
    def test_different_seed_different_output(self, dtype):
        a = FakeDataGenerator({"f": dtype}, seed=1).generate_data(dtype, 20)
        b = FakeDataGenerator({"f": dtype}, seed=2).generate_data(dtype, 20)
        assert not pa.array(a).equals(pa.array(b))


class TestGenerateData:
    def test_int(self, gen):
        data = gen.generate_data("int", 10)
        assert len(data) == 10

    def test_float(self, gen):
        data = gen.generate_data("float", 10)
        assert len(data) == 10

    def test_str(self, gen):
        data = gen.generate_data("str", 5)
        assert len(data) == 5
        assert all(isinstance(v, str) for v in data)

    def test_bool(self, gen):
        data = gen.generate_data("bool", 10)
        assert len(data) == 10

    def test_list(self, gen):
        data = gen.generate_data(["int"], 10)
        assert len(data) == 10

    def test_dict(self, gen):
        data = gen.generate_data({"x": "int", "y": "str"}, 5)
        assert len(data) == 5
        assert all(set(v.keys()) == {"x", "y"} for v in data)

    def test_unsupported_type_raises(self, gen):
        with pytest.raises(ValueError, match="Unsupported data type"):
            gen.generate_data("unknown", 5)


class TestGenerateTable:
    def test_row_count(self, gen):
        assert len(gen.generate_table(50)) == 50

    def test_schema_columns(self, gen):
        table = gen.generate_table(10)
        assert table.column_names == ["a", "b"]


class TestDeleteRows:
    def test_removes_edit_size_rows_per_point(self, gen, table):
        result = gen.delete_rows(table, edit_points=[0.5], edit_size=10)
        assert len(result) == len(table) - 10

    def test_multiple_edit_points(self, gen, table):
        result = gen.delete_rows(table, edit_points=[0.25, 0.75], edit_size=5)
        assert len(result) == len(table) - 10


class TestInsertRows:
    def test_adds_edit_size_rows_per_point(self, gen, table):
        result = gen.insert_rows(table, edit_points=[0.5], edit_size=10)
        assert len(result) == len(table) + 10

    def test_multiple_edit_points(self, gen, table):
        result = gen.insert_rows(table, edit_points=[0.25, 0.75], edit_size=5)
        assert len(result) == len(table) + 10


class TestAppendRows:
    def test_appends_correct_ratio(self, gen, table):
        result = gen.append_rows(table, ratio=0.1)
        assert len(result) == len(table) + int(0.1 * len(table))


class TestUpdateRows:
    def test_preserves_row_count(self, gen, table):
        result = gen.update_rows(table, edit_points=[0.5])
        assert len(result) == len(table)

    def test_modifies_columns(self, gen, table):
        result = gen.update_rows(table, edit_points=[0.5], edit_size=10)
        assert not result.column("a").equals(table.column("a"))

    def test_schema_preserved(self, gen, table):
        result = gen.update_rows(table, edit_points=[0.5])
        assert result.schema == table.schema


class TestGenerateSyntheticTables:
    def test_returns_original_and_variants(self, gen):
        original, variants = gen.generate_synthetic_tables(100, edit_points=[0.5])
        assert isinstance(original, pa.Table)
        assert set(variants.keys()) >= {"deleted", "inserted", "appended", "updated"}

    def test_original_row_count(self, gen):
        original, _ = gen.generate_synthetic_tables(
            100, edit_points=[0.5], edit_size=10
        )
        assert len(original) == 100

    def test_deleted_row_count(self, gen):
        _, variants = gen.generate_synthetic_tables(
            100, edit_points=[0.5], edit_size=10
        )
        assert len(variants["deleted"]) == 90

    def test_inserted_row_count(self, gen):
        _, variants = gen.generate_synthetic_tables(
            100, edit_points=[0.5], edit_size=10
        )
        assert len(variants["inserted"]) == 110

    def test_update_columns_produces_extra_variants(self, gen):
        _, variants = gen.generate_synthetic_tables(
            100, edit_points=[0.5], update_columns=["a"]
        )
        assert "updated_a" in variants

    def test_update_columns_list_type(self):
        gen = FakeDataGenerator({"a": "int", "b": "str", "c": ["float"]}, seed=42)
        _, variants = gen.generate_synthetic_tables(
            100, edit_points=[0.5], update_columns=["c"]
        )
        assert pa.types.is_list(variants["updated_c"].schema.field("c").type)

    def test_multiple_edit_points(self, gen):
        _, variants = gen.generate_synthetic_tables(
            100, edit_points=[0.25, 0.75], edit_size=5
        )
        assert len(variants["deleted"]) == 90
        assert len(variants["inserted"]) == 110
