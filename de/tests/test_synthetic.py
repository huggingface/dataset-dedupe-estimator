import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pytest

from de.synthetic import DataGenerator, generate_array


SCHEMA = {"a": "int", "b": "str"}


@pytest.fixture
def gen():
    return DataGenerator(SCHEMA, seed=42)


@pytest.fixture
def table(gen):
    return gen.generate_table(100)


class TestDeterminism:
    @pytest.mark.parametrize(
        "dtype",
        ["int", "int64", "float", "str", "bool", ["int"], {"x": "int", "y": "str"}],
    )
    def test_same_seed_same_output(self, dtype):
        a = generate_array(np.random.default_rng(0), dtype, 20)
        b = generate_array(np.random.default_rng(0), dtype, 20)
        assert a.equals(b)

    @pytest.mark.parametrize("dtype", ["int", "float", "str", "bool"])
    def test_different_seed_different_output(self, dtype):
        a = generate_array(np.random.default_rng(1), dtype, 20)
        b = generate_array(np.random.default_rng(2), dtype, 20)
        assert not a.equals(b)


class TestGenerateArray:
    def test_int(self):
        data = generate_array(np.random.default_rng(0), "int", 10)
        assert len(data) == 10

    def test_int64_spans_the_full_range(self):
        data = generate_array(np.random.default_rng(0), "int64", 10_000)
        assert data.type == pa.int64()
        assert pc.min(data).as_py() < -(2**62)
        assert pc.max(data).as_py() > 2**62

    def test_float(self):
        data = generate_array(np.random.default_rng(0), "float", 10)
        assert len(data) == 10

    def test_str(self):
        data = generate_array(np.random.default_rng(0), "str", 5)
        assert len(data) == 5
        assert all(isinstance(v.as_py(), str) for v in data)

    def test_bool(self):
        data = generate_array(np.random.default_rng(0), "bool", 10)
        assert len(data) == 10

    def test_list(self):
        data = generate_array(np.random.default_rng(0), ["int"], 10)
        assert len(data) == 10

    def test_dict(self):
        data = generate_array(np.random.default_rng(0), {"x": "int", "y": "str"}, 5)
        assert len(data) == 5
        assert data.type.get_field_index("x") >= 0
        assert data.type.get_field_index("y") >= 0

    def test_unsupported_type_raises(self):
        with pytest.raises(ValueError, match="Unsupported data type"):
            generate_array(np.random.default_rng(0), "unknown", 5)


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

    def test_updates_change_every_edited_value(self):
        gen = DataGenerator({"a": "bool"})
        table = gen.generate_table(1000)
        updated = gen.update_rows(table, [0.25, 0.5, 0.75], edit_size=1)
        before, after = table.column("a").to_pylist(), updated.column("a").to_pylist()
        changed = [i for i, (x, y) in enumerate(zip(before, after)) if x != y]
        assert changed == [250, 500, 750]

    def test_constant_column_cant_be_updated(self):
        gen = DataGenerator({"a": "zeros"})
        with pytest.raises(ValueError, match="constant"):
            gen.update_rows(gen.generate_table(100), [0.5])


NEW_TYPES = [
    ("int8", pa.int8()),
    ("int16", pa.int16()),
    ("int32", pa.int32()),
    ("uint8", pa.uint8()),
    ("uint16", pa.uint16()),
    ("uint32", pa.uint32()),
    ("uint64", pa.uint64()),
    ("float16", pa.float16()),
    ("float32", pa.float32()),
    ("decimal128", pa.decimal128(18, 6)),
    ("decimal256", pa.decimal256(40, 6)),
    ("date32", pa.date32()),
    ("time32", pa.time32("ms")),
    ("time64", pa.time64("ns")),
    ("timestamp", pa.timestamp("ns")),
    ("duration", pa.duration("ns")),
    ("binary", pa.binary()),
    ("largebinary", pa.large_binary()),
    ("largestring", pa.large_string()),
    ("fixed", pa.binary(16)),
    ("dictionary", pa.dictionary(pa.int32(), pa.string())),
    ("json", pa.json_(pa.string())),
    ("nulls", pa.int32()),
    ("nullsegments", pa.list_(pa.int64())),
]


class TestNewTypes:
    @pytest.mark.parametrize("dtype, arrow_type", NEW_TYPES)
    def test_type_and_length(self, dtype, arrow_type):
        data = generate_array(np.random.default_rng(0), dtype, 100)
        assert data.type == arrow_type
        assert len(data) == 100

    @pytest.mark.parametrize("dtype, arrow_type", NEW_TYPES)
    def test_empty(self, dtype, arrow_type):
        data = generate_array(np.random.default_rng(0), dtype, 0)
        assert data.type == arrow_type
        assert len(data) == 0

    def test_all_nulls(self):
        assert generate_array(np.random.default_rng(0), "nulls", 100).null_count == 100

    def test_dictionary_is_shared_by_every_generator(self):
        a = generate_array(np.random.default_rng(1), "dictionary", 100)
        b = generate_array(np.random.default_rng(2), "dictionary", 100)
        assert a.dictionary.equals(b.dictionary)

    def test_edits_keep_the_dictionary(self):
        table = DataGenerator({"d": "dictionary"}).generate_table(1000)
        inserted = DataGenerator({"d": "dictionary"}, seed=1).insert_rows(table, [0.5])
        assert (
            inserted.column("d")
            .chunk(0)
            .dictionary.equals(table.column("d").chunk(0).dictionary)
        )


def null_mask(data):
    return data.is_null().to_numpy(zero_copy_only=False)


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
        assert (lengths[null_mask(data)] == 0).all()

    def test_null_segments_keep_child_ranges(self):
        data = generate_array(
            np.random.default_rng(0), "nullsegments", 10_000, null_ratio=0.1
        )
        lengths = np.diff(data.offsets.to_numpy())
        assert (lengths[null_mask(data)] > 0).any()

    def test_zero_ratio_draws_no_masks(self):
        # the existing generated data stays the same: no rng draws for masks
        schema = {"a": "int", "b": ["str"]}
        a = DataGenerator(schema).generate_table(100)
        b = DataGenerator(schema, null_ratio=0.0).generate_table(100)
        assert a.equals(b)
        assert a.column("a").null_count == 0


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
        gen = DataGenerator({"a": "int", "b": "str", "c": ["float"]}, seed=42)
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
