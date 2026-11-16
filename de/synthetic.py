import numpy as np
import pyarrow as pa


def evenly_spaced(count: int) -> list[float]:
    """Relative positions of count evenly spaced edits."""
    return np.linspace(0.5 / count, 1 - 0.5 / count, count).tolist()


class DataGenerator:
    """Seeded tables of a schema, with null_ratio of nulls at every nesting level.

    The rows themselves are never null, their fields and everything below are.
    """

    def __init__(self, schema, seed=42, null_ratio=0.0):
        self.schema = schema
        self.seed = seed
        self.null_ratio = null_ratio
        self.rng = np.random.default_rng(seed)

    def generate_table(self, num_rows):
        arrays = [
            generate_array(self.rng, dtype, num_rows, self.null_ratio)
            for dtype in self.schema.values()
        ]
        return pa.Table.from_arrays(arrays, names=list(self.schema))

    def delete_rows(self, table, edit_points, edit_size=10):
        pieces = []
        for start, end in zip([0] + edit_points, edit_points + [1]):
            start_idx = int(start * len(table))
            end_idx = int(end * len(table))
            if end == 1:
                pieces.append(table.slice(start_idx, end_idx - start_idx))
            else:
                pieces.append(table.slice(start_idx, end_idx - start_idx - edit_size))
        return pa.concat_tables(pieces).combine_chunks()

    def insert_rows(self, table, edit_points, edit_size=10):
        pieces = []
        for start, end in zip([0] + edit_points, edit_points + [1]):
            start_idx = int(start * len(table))
            end_idx = int(end * len(table))
            pieces.append(table.slice(start_idx, end_idx - start_idx))
            if end != 1:
                pieces.append(self.generate_table(edit_size))
        return pa.concat_tables(pieces).combine_chunks()

    def append_rows(self, table, ratio):
        new_part = self.generate_table(int(ratio * len(table)))
        return pa.concat_tables([table, new_part]).combine_chunks()

    def update_rows(self, table, edit_points, edit_size=10):
        """Replace the row at each edit point with a generated row of other values."""
        edits = self.generate_table(len(edit_points) * max(edit_size, 64))
        unused = list(enumerate(edits.to_pylist()))
        pieces = []
        prev = 0
        for place in edit_points:
            idx = int(place * len(table))
            [original] = table.slice(idx, 1).to_pylist()
            different = [i for i, (_, row) in enumerate(unused) if row != original]
            if not different:
                raise ValueError("can't update a constant column")
            replacement, _ = unused.pop(different[0])
            pieces.append(table.slice(prev, idx - prev))
            pieces.append(edits.slice(replacement, 1))
            prev = idx + 1
        pieces.append(table.slice(prev))
        return pa.concat_tables(pieces).combine_chunks()

    def generate_synthetic_tables(
        self,
        size,
        edit_points=(0.5,),
        append_ratio=0.05,
        update_columns=None,
        edit_size=10,
    ):
        table = self.generate_table(size)
        deleted = self.delete_rows(table, edit_points, edit_size=edit_size)
        inserted = self.insert_rows(table, edit_points, edit_size=edit_size)
        appended = self.append_rows(table, append_ratio)
        updated = self.update_rows(table, edit_points, edit_size=edit_size)
        assert len(table) == size
        assert len(updated) == size
        assert len(deleted) == size - edit_size * len(edit_points)
        assert len(inserted) == size + edit_size * len(edit_points)

        result = {
            "deleted": deleted,
            "inserted": inserted,
            "appended": appended,
            "updated": updated,
        }
        if update_columns:
            updated = self.update_rows(table, edit_points, edit_size=edit_size)
            for column in update_columns:
                col_idx = table.schema.get_field_index(column)
                result[f"updated_{column}"] = table.set_column(
                    col_idx, column, updated.column(column)
                )

        return table, result


_CHARS = np.frombuffer(b"abcdefghijklmnopqrstuvwxyz ", dtype=np.uint8)


def _random_strings(rng, n, min_len, max_len):
    # Draw a random length for each string
    lengths = rng.integers(min_len, max_len, size=n)
    total = int(lengths.sum())
    # Sample all characters at once as uint8 indices into _CHARS (a-z + space)
    indices = rng.integers(0, len(_CHARS), size=total)
    flat = _CHARS[indices]
    # Build split offsets via cumsum so we can slice the flat array per string
    offsets = np.empty(n + 1, dtype=np.intp)
    offsets[0] = 0
    np.cumsum(lengths, out=offsets[1:])
    # tobytes()+decode is faster than joining numpy scalars one by one
    return [
        flat[offsets[i] : offsets[i + 1]].tobytes().decode("ascii") for i in range(n)
    ]


# the dictionary of the "dictionary" type, shared by every generator so that edited
# tables keep the dictionary of the original
DICTIONARY = pa.array(_random_strings(np.random.default_rng(0), 1000, 10, 100))

INTEGERS = {
    "int8": np.int8,
    "int16": np.int16,
    "int32": np.int32,
    "uint8": np.uint8,
    "uint16": np.uint16,
    "uint32": np.uint32,
    "uint64": np.uint64,
}

TEMPORALS = {
    "date32": (pa.date32(), 20_000),
    "time32": (pa.time32("ms"), 86_400_000),
    "time64": (pa.time64("ns"), 86_400 * 10**9),
    "timestamp": (pa.timestamp("ns"), 2**62),
    "duration": (pa.duration("ns"), 2**62),
}


def null_mask(rng, num_samples, null_ratio):
    """The nulls of an array, None without drawing from rng if there are none."""
    return rng.random(num_samples) < null_ratio if null_ratio > 0 else None


def with_nulls(array, mask):
    """The array with the values at mask set to null."""
    if isinstance(array, pa.DictionaryArray):
        indices = with_nulls(array.indices, mask)
        return pa.DictionaryArray.from_arrays(indices, array.dictionary)
    if isinstance(array, pa.ExtensionArray):
        return pa.ExtensionArray.from_storage(
            array.type, with_nulls(array.storage, mask)
        )
    validity = pa.array(~mask).buffers()[1]
    return pa.Array.from_buffers(
        array.type,
        len(array),
        [validity, *array.buffers()[1:]],
        null_count=int(mask.sum()),
    )


def generate_array(rng, dtype, num_samples, null_ratio=0.0):
    """An array of dtype, with null_ratio of nulls at every nesting level.

    The values are drawn before the nulls, and nulls only when null_ratio > 0, so
    the values don't depend on null_ratio.
    """
    if isinstance(dtype, dict):
        arrays = [
            generate_array(rng, field_type, num_samples, null_ratio)
            for field_type in dtype.values()
        ]
        mask = null_mask(rng, num_samples, null_ratio)
        return pa.StructArray.from_arrays(
            arrays, names=list(dtype), mask=None if mask is None else pa.array(mask)
        )
    elif dtype == "nullsegments" or (isinstance(dtype, list) and dtype):
        # null lists are empty, except with "nullsegments": lists of ints whose null
        # entries keep the child values they span, as the arrow format allows
        lengths = rng.integers(0, 5, size=num_samples)
        mask = null_mask(rng, num_samples, null_ratio)
        if mask is not None and dtype != "nullsegments":
            lengths[mask] = 0
        offsets = np.zeros(num_samples + 1, dtype=np.int32)
        np.cumsum(lengths, out=offsets[1:])
        child = "int" if dtype == "nullsegments" else dtype[0]
        values = generate_array(rng, child, int(offsets[-1]), null_ratio)
        return pa.ListArray.from_arrays(
            offsets, values, mask=None if mask is None else pa.array(mask)
        )
    elif dtype == "nulls":
        return pa.nulls(num_samples, pa.int32())
    array = generate_values(rng, dtype, num_samples)
    mask = null_mask(rng, num_samples, null_ratio)
    return array if mask is None else with_nulls(array, mask)


def generate_values(rng, dtype, num_samples):
    """A leaf array of dtype without nulls."""
    if dtype in INTEGERS:
        info = np.iinfo(INTEGERS[dtype])
        return pa.array(
            rng.integers(
                info.min,
                info.max,
                size=num_samples,
                dtype=INTEGERS[dtype],
                endpoint=True,
            )
        )
    elif dtype in ("float16", "float32"):
        values = rng.uniform(-1000, 1000, size=num_samples)
        return pa.array(values.astype(np.float16 if dtype == "float16" else np.float32))
    elif dtype in ("decimal128", "decimal256"):
        # unscaled little endian integers, sign extended to the decimal's width
        unscaled = rng.integers(-(10**17), 10**17, size=num_samples)
        words = 2 if dtype == "decimal128" else 4
        sign = unscaled >> 63
        data = np.column_stack([unscaled] + [sign] * (words - 1))
        type = pa.decimal128(18, 6) if dtype == "decimal128" else pa.decimal256(40, 6)
        return pa.Array.from_buffers(
            type, num_samples, [None, pa.py_buffer(data.tobytes())]
        )
    elif dtype in TEMPORALS:
        type, high = TEMPORALS[dtype]
        values = rng.integers(0, high, size=num_samples)
        return pa.array(values.astype(f"int{type.bit_width}"), type=type)
    elif dtype in ("binary", "largebinary"):
        strings = pa.array(_random_strings(rng, num_samples, 10, 100))
        return strings.cast(pa.binary() if dtype == "binary" else pa.large_binary())
    elif dtype == "largestring":
        return pa.array(_random_strings(rng, num_samples, 10, 100), pa.large_string())
    elif dtype == "fixed":
        data = pa.py_buffer(rng.bytes(16 * num_samples))
        return pa.Array.from_buffers(pa.binary(16), num_samples, [None, data])
    elif dtype == "dictionary":
        indices = rng.integers(0, len(DICTIONARY), size=num_samples).astype(np.int32)
        return pa.DictionaryArray.from_arrays(pa.array(indices), DICTIONARY)
    elif dtype == "json":
        values = rng.integers(0, 1_000_000, size=num_samples)
        storage = pa.array([f'{{"v": {v}}}' for v in values])
        return pa.ExtensionArray.from_storage(pa.json_(), storage)
    elif dtype in ("int", int):
        return pa.array(rng.integers(0, 1_000_000, size=num_samples))
    elif dtype in ("int64",):
        # the full value range, every byte is random so it doesn't compress
        info = np.iinfo(np.int64)
        return pa.array(
            rng.integers(info.min, info.max, size=num_samples, endpoint=True)
        )
    elif dtype in ("zeros",):
        # constant, so content defined chunking can only cut at the max chunk size
        return pa.array(np.zeros(num_samples, dtype=np.int64))
    elif dtype in ("float", float):
        return pa.array(rng.uniform(0, 1_000_000, size=num_samples).round(3))
    elif dtype in ("str", str):
        return pa.array(_random_strings(rng, num_samples, 10, 100))
    elif dtype in ("largestr",):
        return pa.array(_random_strings(rng, num_samples, 100, 1000))
    elif dtype in ("bool", bool):
        return pa.array(rng.integers(0, 2, size=num_samples).astype(bool))
    else:
        raise ValueError("Unsupported data type: {}".format(dtype))
