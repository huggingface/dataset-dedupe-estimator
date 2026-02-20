import numpy as np
import pyarrow as pa


class DataGenerator:
    def __init__(self, schema, seed=42):
        self.schema = schema
        self.seed = seed
        self.rng = np.random.default_rng(seed)

    def generate_table(self, num_rows):
        array = generate_array(self.rng, self.schema, num_rows)
        return pa.Table.from_struct_array(array)

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
        edits = self.generate_table(len(edit_points) * edit_size)
        pieces = []
        prev = 0
        for i, place in enumerate(edit_points):
            idx = int(place * len(table))
            pieces.append(table.slice(prev, idx - prev))
            pieces.append(edits.slice((i + 1) * edit_size - 1, 1))
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


def generate_array(rng, dtype, num_samples):
    if dtype in ("int", int):
        return pa.array(rng.integers(0, 1_000_000, size=num_samples))
    elif dtype in ("float", float):
        return pa.array(rng.uniform(0, 1_000_000, size=num_samples).round(3))
    elif dtype in ("str", str):
        return pa.array(_random_strings(rng, num_samples, 10, 100))
    elif dtype in ("largestr",):
        return pa.array(_random_strings(rng, num_samples, 100, 1000))
    elif dtype in ("bool", bool):
        return pa.array(rng.integers(0, 2, size=num_samples).astype(bool))
    elif isinstance(dtype, dict):
        arrays = [
            generate_array(rng, field_type, num_samples)
            for field_type in dtype.values()
        ]
        return pa.StructArray.from_arrays(arrays, names=list(dtype.keys()))
    elif isinstance(dtype, list) and dtype:
        offsets = np.zeros(num_samples + 1, dtype=np.int32)
        np.cumsum(rng.integers(0, 5, size=num_samples), out=offsets[1:])
        values = generate_array(rng, dtype[0], int(offsets[-1]))
        return pa.ListArray.from_arrays(offsets, values)
    else:
        raise ValueError("Unsupported data type: {}".format(dtype))
