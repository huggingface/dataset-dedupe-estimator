from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from faker import Faker
from PIL import Image
import sqlite3

from .estimate import estimate_de

seed = 42
fake = Faker()
fake.random.seed(seed)


def generate_data(dtype, num_samples=1000):
    if dtype in ("int", int):
        return np.random.randint(0, 1_000_000, size=num_samples).tolist()
    elif dtype in ("float", float):
        return np.random.uniform(0, 1_000_000, size=num_samples).round(3).tolist()
    elif dtype in ("str", str):
        num_chars = np.random.randint(10, 200, size=num_samples)
        return [fake.text(max_nb_chars=n_chars) for n_chars in num_chars]
    elif dtype in ("largestr",):
        num_chars = np.random.randint(100, 1000, size=num_samples)
        return [fake.text(max_nb_chars=n_chars) for n_chars in num_chars]
    elif dtype == ("bool", bool):
        return np.random.choice([True, False], size=num_samples).tolist()
    elif isinstance(dtype, dict):
        columns = [
            generate_data(field_type, num_samples) for field_type in dtype.values()
        ]
        return [dict(zip(dtype.keys(), row)) for row in zip(*columns)]
    elif isinstance(dtype, list) and dtype:
        lengths = np.random.randint(0, 5, size=num_samples)
        values = generate_data(dtype[0], lengths.sum())
        return [
            values[i : i + length]
            for i, length in zip(np.cumsum([0] + lengths), lengths)
        ]
    else:
        raise ValueError("Unsupported data type: {}".format(dtype))


def generate_table(schema, num_samples=1000):
    data = generate_data(schema, num_samples)
    arr = pa.array(data)
    table = pa.Table.from_struct_array(arr)
    return table


def delete_rows(table, edit_points, n=10):
    pieces = []
    for start, end in zip([0] + edit_points, edit_points + [1]):
        start_idx = int(start * len(table))
        end_idx = int(end * len(table))
        if end == 1:
            pieces.append(table.slice(start_idx, end_idx - start_idx))
        else:
            pieces.append(table.slice(start_idx, end_idx - start_idx - n))
    return pa.concat_tables(pieces).combine_chunks()


def insert_rows(table, schema, edit_points, n=10):
    pieces = []
    for start, end in zip([0] + edit_points, edit_points + [1]):
        start_idx = int(start * len(table))
        end_idx = int(end * len(table))
        pieces.append(table.slice(start_idx, end_idx - start_idx))
        if end != 1:
            pieces.append(generate_table(schema, n))
    return pa.concat_tables(pieces).combine_chunks()


def append_rows(table, schema, ratio):
    new_part = generate_table(schema, int(ratio * len(table)))
    return pa.concat_tables([table, new_part]).combine_chunks()


def update_rows(table, schema, edit_points, columns):
    df = table.to_pandas()
    for place in edit_points:
        idx = int(place * len(df))
        for column in columns:
            df.at[idx, column] = generate_data(schema[column], 1)[0]
    return pa.Table.from_pandas(df)


def generate_synthetic_tables(
    schema,
    size,
    edit_points=(0.5,),
    append_ratio=0.05,
    update_columns=None,
    edit_size=10,
):
    fields = list(schema.keys())
    table = generate_table(schema, size)
    deleted = delete_rows(table, edit_points, n=edit_size)
    inserted = insert_rows(table, schema, edit_points, n=edit_size)
    appended = append_rows(table, schema, append_ratio)
    updated = update_rows(table, schema, edit_points, columns=fields)
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
    for key, fields in (update_columns or {}).items():
        result[f"updated_{key}"] = update_rows(
            table, schema, edit_points, columns=fields
        )

    return table, result


def write_parquet(path, table, **kwargs):
    pq.write_table(table, path, **kwargs)
    readback = pq.read_table(path)
    assert table.equals(readback)


def write_and_compare_parquet(
    directory, original, alts, prefix, postfix, **parquet_options
):
    results = []
    for compression in ["none", "zstd", "snappy"]:
        if compression == "none":
            parquet_options["compression"] = None
        else:
            parquet_options["compression"] = compression
        a = directory / f"{prefix}-{compression}-original-{postfix}.parquet"
        write_parquet(a, original, **parquet_options)
        for name, table in alts.items():
            b = directory / f"{prefix}-{compression}-{name}-{postfix}.parquet"
            write_parquet(b, table, **parquet_options)
            result = estimate_de([a, b])
            results.append(
                {"kind": postfix, "edit": name, "compression": compression, **result}
            )
    return results


def write_and_compare_json(directory, original, alts, prefix):
    results = []
    original_df = original.to_pandas()
    for compression in ["none", "zstd"]:
        comp = None if compression == "none" else compression
        a = directory / f"{prefix}-{compression}-original.jsonlines"
        original_df.to_json(a, orient="records", lines=True, compression=comp)
        for name, table in alts.items():
            b = directory / f"{prefix}-{compression}-{name}.jsonlines"
            table.to_pandas().to_json(b, orient="records", lines=True, compression=comp)
            result = estimate_de([a, b])
            results.append(
                {"kind": "json", "edit": name, "compression": compression, **result}
            )
    return results


def write_and_compare_sqlite(directory, original, alts, prefix):
    results = []
    original_df = original.to_pandas()
    for compression in ["none"]:
        comp = None if compression == "none" else compression
        a = directory / f"{prefix}-{compression}-original.sqlite"
        con = sqlite3.connect(a)
        original_df.to_sql("table", con, if_exists="replace", index=False)
        for name, table in alts.items():
            b = directory / f"{prefix}-{compression}-{name}.sqlite"
            con = sqlite3.connect(b)
            table.to_pandas().to_sql("table", con, if_exists="replace", index=False)
            result = estimate_de([a, b])
            results.append(
                {"kind": "sqlite", "edit": name, "compression": compression, **result}
            )
    return results


def convert_dedupe_images_to_png(directory):
    directory = Path(directory)

    for ppm in directory.iterdir():
        if ppm.suffix == ".ppm":
            png = ppm.with_suffix(".png")
            with Image.open(ppm) as img:
                img.save(png, "PNG")
            ppm.unlink()
