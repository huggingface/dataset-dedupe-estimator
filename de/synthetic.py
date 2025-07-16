from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from faker import Faker
import sqlite3
from rich.console import Console
from rich.table import Table
from humanize import naturalsize

from .estimate import estimate_de


class DataGenerator:
    def __init__(self, schema, seed=42):
        self.schema = schema
        self.seed = seed

    def generate_table(self, num_rows):
        raise NotImplementedError("Subclasses should implement this method.")

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

    def update_rows(self, table, edit_points, columns, edit_size=10):
        df = table.to_pandas()
        edits = self.generate_table(len(edit_points) * edit_size)
        edits_df = edits.to_pandas()
        for i, place in enumerate(edit_points):
            idx = int(place * len(df))
            for column in columns:
                for j in range(edit_size):
                    edit_idx = i * edit_size + j
                    df.at[idx, column] = edits_df.at[edit_idx, column]
        return pa.Table.from_pandas(df)

    def generate_synthetic_tables(
        self,
        size,
        edit_points=(0.5,),
        append_ratio=0.05,
        update_columns=None,
        edit_size=10,
    ):
        fields = list(self.schema.keys())
        table = self.generate_table(size)
        deleted = self.delete_rows(table, edit_points, edit_size=edit_size)
        inserted = self.insert_rows(table, edit_points, edit_size=edit_size)
        appended = self.append_rows(table, append_ratio)
        updated = self.update_rows(table, edit_points, columns=fields)
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
            result[f"updated_{key}"] = self.update_rows(
                table, edit_points, columns=fields
            )

        return table, result


class FakeDataGenerator(DataGenerator):
    def __init__(self, schema, seed=42):
        super().__init__(schema, seed)
        self.fake = Faker()
        self.fake.random.seed(seed)

    def generate_data(self, dtype, num_samples):
        if dtype in ("int", int):
            return np.random.randint(0, 1_000_000, size=num_samples).tolist()
        elif dtype in ("float", float):
            return np.random.uniform(0, 1_000_000, size=num_samples).round(3).tolist()
        elif dtype in ("str", str):
            num_chars = np.random.randint(10, 200, size=num_samples)
            return [self.fake.text(max_nb_chars=n_chars) for n_chars in num_chars]
        elif dtype in ("largestr",):
            num_chars = np.random.randint(100, 1000, size=num_samples)
            return [self.fake.text(max_nb_chars=n_chars) for n_chars in num_chars]
        elif dtype in ("bool", bool):
            return np.random.choice([True, False], size=num_samples).tolist()
        elif isinstance(dtype, dict):
            columns = [
                self.generate_data(field_type, num_samples)
                for field_type in dtype.values()
            ]
            return [dict(zip(dtype.keys(), row)) for row in zip(*columns)]
        elif isinstance(dtype, list) and dtype:
            lengths = np.random.randint(0, 5, size=num_samples)
            values = self.generate_data(dtype[0], lengths.sum())
            return [
                values[i : i + length]
                for i, length in zip(np.cumsum([0] + lengths), lengths)
            ]
        else:
            raise ValueError("Unsupported data type: {}".format(dtype))

    def generate_table(self, num_rows):
        data = self.generate_data(self.schema, num_rows)
        arr = pa.array(data)
        table = pa.Table.from_struct_array(arr)
        return table


# # TODO(kszucs)
# class WikiDataSampler(DataGenerator):
#     pass


def write_parquet(path, table, **kwargs):
    if isinstance(table, tuple):
        table, options = table
        kwargs.update(options)

    pq.write_table(table, path, **kwargs)
    readback = pq.read_table(path)
    assert table.equals(readback)


def write_and_compare_parquet(
    directory, original, alts, prefix, postfix, **parquet_options
):
    directory = Path(directory)
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
                {
                    "path": b,
                    "kind": postfix,
                    "edit": name,
                    "compression": compression,
                    **result,
                }
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


def pretty_print_stats(results):
    has_xtool = "transmitted_xtool_bytes" in results[0]

    # dump the results to the console as a rich formatted table
    results = sorted(results, key=lambda x: (x["edit"], x["compression"], x["kind"]))

    console = Console()
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Title")
    table.add_column("Compression", justify="left")
    table.add_column("Parquet CDC", justify="left")
    table.add_column("Total Size", justify="right")
    table.add_column("Chunk Size", justify="right")
    table.add_column("Compressed Chunk Size", justify="right")
    table.add_column("Dedup Ratio", justify="right")
    table.add_column("Compressed Dedup Ratio", justify="right")
    if has_xtool:
        table.add_column("Transmitted XTool Bytes", justify="right")

    prev_group = None
    for row in results:
        group = (row["edit"], row["compression"])
        if group != prev_group:
            table.add_section()
            prev_group = group

        values = [
            row["edit"],
            row["compression"],
            row["kind"],
            naturalsize(row["total_len"], binary=True),
            naturalsize(row["chunk_bytes"], binary=True),
            naturalsize(row["compressed_chunk_bytes"], binary=True),
            "{:.0%}".format(row["chunk_bytes"] / row["total_len"]),
            "{:.0%}".format(row["compressed_chunk_bytes"] / row["total_len"]),
        ]
        if has_xtool:
            values.append(naturalsize(row["transmitted_xtool_bytes"], binary=True))
        table.add_row(*values)

    console.print(table)
