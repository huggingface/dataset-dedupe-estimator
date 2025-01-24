import json
import subprocess
import functools
import os
from pathlib import Path
import sys
import tempfile

import click
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from faker import Faker
from tqdm.contrib.concurrent import process_map
from rich.console import Console
from rich.table import Table
import humanize
import plotly.graph_objects as go
from PIL import Image

import de

seed = 42
fake = Faker()
fake.random.seed(seed)


def pyarrow_has_cdc():
    # check that pyarrow is compoiled with cdc support
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir = Path(temp_dir)
        table = pa.table({"a": [1, 2, 3, 4, 5]})
        try:
            pq.write_table(
                table, temp_dir / "test.parquet", content_defined_chunking=True
            )
        except TypeError:
            return False
    return True


def convert_dedupe_images_to_png(directory):
    directory = Path(directory)

    for ppm in directory.iterdir():
        if ppm.suffix == ".ppm":
            png = ppm.with_suffix(".png")
            with Image.open(ppm) as img:
                img.save(png, "PNG")
            ppm.unlink()


def generate_data(dtype, num_samples=1000):
    if dtype in ("int", int):
        return np.random.randint(0, 1_000_000, size=num_samples).tolist()
    elif dtype in ("float", float):
        return np.random.uniform(0, 1_000_000, size=num_samples).round(3).tolist()
    elif dtype in ("str", str):
        return [fake.word() for _ in range(num_samples)]
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
    return pa.Table.from_struct_array(pa.array(data))


def delete_rows(table, alter_points, n=10):
    pieces = []
    for start, end in zip([0] + alter_points, alter_points + [1]):
        start_idx = int(start * len(table))
        end_idx = int(end * len(table))
        if end == 1:
            pieces.append(table.slice(start_idx, end_idx - start_idx))
        else:
            pieces.append(table.slice(start_idx, end_idx - start_idx - n))
    return pa.concat_tables(pieces)


def insert_rows(table, schema, alter_points, n=10):
    pieces = []
    for start, end in zip([0] + alter_points, alter_points + [1]):
        start_idx = int(start * len(table))
        end_idx = int(end * len(table))
        pieces.append(table.slice(start_idx, end_idx - start_idx))
        if end != 1:
            pieces.append(generate_table(schema, n))
    return pa.concat_tables(pieces)


def append_rows(table, schema, ratio):
    new_part = generate_table(schema, int(ratio * len(table)))
    return pa.concat_tables([table, new_part])


def update_rows(table, schema, alter_points, columns):
    df = table.to_pandas()
    for place in alter_points:
        idx = int(place * len(df))
        for column in columns:
            df.at[idx, column] = generate_data(schema[column], 1)[0]
    return pa.Table.from_pandas(df)


def write_parquet(path, table, **kwargs):
    # cdc = (0xffff00000000000, 1024 * 1024 / 4, 2 * 1024 * 1024)
    pq.write_table(table, path, **kwargs)
    readback = pq.read_table(path)
    assert table.equals(readback)


def rewrite_to_parquet(src_path, dest_path, block_size=1024 * 1024, **kwargs):
    """
    Reads a Parquet file in blocks and writes them out to another file.

    :param src_path: Path to the source Parquet file.
    :param dest_path: Path to the destination Parquet file.
    :param block_size: Size of the blocks to read and write in bytes.
    """
    src_path = Path(src_path)
    dest_path = Path(dest_path)

    with pq.ParquetFile(src_path) as src:
        schema = src.schema.to_arrow_schema()
        writer = pq.ParquetWriter(dest_path, schema, **kwargs)
        for batch in src.iter_batches(batch_size=block_size):
            writer.write(batch, row_group_size=1024 * 1024)
        writer.close()

    src = pq.ParquetFile(src_path)
    dst = pq.ParquetFile(dest_path)
    src_metadata = src.metadata
    dst_metadata = dst.metadata

    assert src_metadata.num_rows == dst_metadata.num_rows
    assert (
        src_metadata.schema.to_arrow_schema() == dst_metadata.schema.to_arrow_schema()
    )


def rewrite_to_jsonlines(src, dest, **kwargs):
    table = pq.read_table(src)
    table.to_pandas().to_json(dest, orient="records", lines=True, **kwargs)


def generate_alterated_tables(
    schema, size, alter_points=(0.5,), append_ratio=0.05, update_columns=None
):
    n = 10
    fields = list(schema.keys())
    table = generate_table(schema, size)
    deleted = delete_rows(table, alter_points, n=n)
    inserted = insert_rows(table, schema, alter_points, n=n)
    appended = append_rows(table, schema, append_ratio)
    updated = update_rows(table, schema, alter_points, columns=fields)
    assert len(table) == size
    assert len(updated) == size
    assert len(deleted) == size - n * len(alter_points)
    assert len(inserted) == size + n * len(alter_points)

    result = {
        "deleted": deleted,
        "inserted": inserted,
        "appended": appended,
        "updated": updated,
    }
    for key, fields in (update_columns or {}).items():
        result[f"updated_{key}"] = update_rows(
            table, schema, alter_points, columns=fields
        )

    return table, result


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
            result = de.estimate([a, b])
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
            result = de.estimate([a, b])
            results.append(
                {"kind": "json", "edit": name, "compression": compression, **result}
            )
    return results


def checkout_file_revisions(file_path, target_dir) -> list[str]:
    """
    Returns a list of short commit hashes for all revisions of the given file.
    """
    file_path = Path(file_path)
    target_dir = Path(target_dir)

    cwd = Path.cwd()
    try:
        os.chdir(file_path.parent)
        git_dir = Path(
            subprocess.check_output(
                ["git", "rev-parse", "--show-toplevel"], text=True
            ).strip()
        )
    finally:
        os.chdir(cwd)

    git_file = file_path.relative_to(git_dir)
    git_cmd = ["git", "-C", str(git_dir)]
    try:
        command = git_cmd + [
            "log",
            "--pretty=format:%h",
            "--follow",
            "--diff-filter=d",
            "--",
            str(git_file),
        ]
        output = subprocess.check_output(command, text=True)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Failed to retrieve revisions for {git_file}") from e

    revisions = output.strip().split("\n")
    print(f"{git_file} has {len(revisions)} revisions")
    for rev in revisions:
        print("Checking out", rev)
        command = git_cmd + [
            f"--work-tree={target_dir}",
            "checkout",
            rev,
            "--",
            str(git_file),
        ]
        try:
            subprocess.run(command, check=True)
        except subprocess.CalledProcessError as e:
            raise RuntimeError(
                f"Failed to checkout {file_path} at revision {rev}"
            ) from e
        # rename the file to include the commit hash
        new_file = target_dir / f"{file_path.stem}-{rev}{file_path.suffix}"
        os.rename(target_dir / git_file, new_file)


def pretty_print_stats(results):
    # dump the results to the console as a rich formatted table
    console = Console()
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Title")
    table.add_column("Total Size", justify="right")
    table.add_column("Chunk Size", justify="right")
    table.add_column("Compressed Chunk Size", justify="right")
    table.add_column("Deduplication Ratio", justify="right")
    table.add_column("Compressed Deduplication Ratio", justify="right")
    for i, row in enumerate(results):
        table.add_row(
            row["title"],
            humanize.naturalsize(row["total_len"], binary=True),
            humanize.naturalsize(row["chunk_bytes"], binary=True),
            humanize.naturalsize(row["compressed_chunk_bytes"], binary=True),
            "{:.0%}".format(row["chunk_bytes"] / results[i]["total_len"]),
            "{:.0%}".format(row["compressed_chunk_bytes"] / results[i]["total_len"]),
        )
    console.print(table)


@click.group()
def cli():
    if not pyarrow_has_cdc():
        click.echo("PyArrow is not compiled with CDC support.", err=True)
        sys.exit(1)


@cli.command()
@click.argument("schema", default='{"a": "int", "b": "str", "c": ["int"]}', type=str)
@click.option(
    "--target-dir",
    "-d",
    help="Directory to store the files at",
    type=click.Path(file_okay=False, writable=True),
    required=True,
    default="synthetic",
)
@click.option(
    "--size", "-s", default=1, help="Number of millions or records to generate"
)
@click.option(
    "--num-edits", "-e", default=10, help="Number of changes to make in the data"
)
@click.option("--use-dictionary", is_flag=True, help="Use parquet dictionary encoding")
def synthetic(schema, size, num_edits, target_dir, use_dictionary):
    directory = Path(target_dir)
    directory.mkdir(exist_ok=True)

    alter_points = np.linspace(0.5 / num_edits, 1 - 0.5 / num_edits, num_edits)
    schema = json.loads(schema)
    original, tables = generate_alterated_tables(
        schema,
        size=size * 2**20,
        alter_points=list(alter_points),
        append_ratio=0.05,
        update_columns={k: [k] for k in schema.keys()},
    )

    prefix = f"s{size}c{len(schema)}e{num_edits}"
    results = write_and_compare_parquet(
        directory,
        original,
        tables,
        prefix=prefix,
        postfix="nocdc",
        content_defined_chunking=False,
        use_dictionary=use_dictionary,
    )
    results += write_and_compare_parquet(
        directory,
        original,
        tables,
        prefix=prefix,
        postfix="cdc",
        content_defined_chunking=True,
        use_dictionary=use_dictionary,
        data_page_size=100 * 1024 * 1024,
    )
    results += write_and_compare_json(directory, original, tables, prefix=prefix)
    convert_dedupe_images_to_png(directory)

    for row in results:
        row["title"] = (
            f"{row['edit'].capitalize()} / {row['compression']} / {row['kind']}"
        )
    results = sorted(results, key=lambda x: x["title"])
    pretty_print_stats(results)


@cli.command()
@click.argument("files", nargs=-1, type=click.Path(exists=True))
@click.option(
    "--target-dir",
    "-d",
    help="Directory to store the revisions",
    type=click.Path(file_okay=False, writable=True),
    required=True,
)
def revisions(files, target_dir):
    """Checkout all revisions of the given files and calculate the deduplication ratio."""
    target_dir = Path("revisions") if target_dir is None else Path(target_dir)
    target_dir.mkdir(exist_ok=True)
    for file_path in files:
        checkout_file_revisions(file_path, target_dir=target_dir)


@cli.command()
@click.argument("directory", type=click.Path(exists=True, file_okay=False))
@click.option("--with-json", is_flag=True, help="Also calculate JSONLines stats")
@click.option("--skip-rewrite", is_flag=True, help="Skip file rewriting")
@click.option("--skip-json-rewrite", is_flag=True, help="Skip JSON rewrite")
@click.option("--skip-parquet-rewrite", is_flag=True, help="Skip Parquet rewrite")
@click.option(
    "--max-processes",
    "-p",
    default=None,
    type=int,
    help="Maximum number of processes to use",
)
def stats(
    directory,
    with_json,
    skip_rewrite,
    skip_json_rewrite,
    skip_parquet_rewrite,
    max_processes,
):
    # go over all the parquet files in the directory, read them, generate a cdc
    # enabled version and compare the deduplication ratios of all the files
    # written without and with CDC
    files = [
        path for path in Path(directory).rglob("*.parquet") if "cdc" not in path.name
    ]
    json_files = [path.with_name(path.stem + ".jsonlines") for path in files]
    cdc_zstd_files = [path.with_name(path.stem + "-zstd-cdc.parquet") for path in files]
    cdc_snappy_files = [
        path.with_name(path.stem + "-snappy-cdc.parquet") for path in files
    ]

    if with_json and not (skip_rewrite or skip_json_rewrite):
        print("Writing JSONLines files")
        process_map(rewrite_to_jsonlines, files, json_files)

    # TODO(kszucs): measure with max_data_page_size = 100 * 1024 * 1024
    if not (skip_rewrite or skip_parquet_rewrite):
        print("Writing CDC Parquet files")
        process_map(
            functools.partial(
                rewrite_to_parquet, compression="zstd", content_defined_chunking=True
            ),
            files,
            cdc_zstd_files,
            max_workers=max_processes,
        )
        process_map(
            functools.partial(
                rewrite_to_parquet, compression="snappy", content_defined_chunking=True
            ),
            files,
            cdc_snappy_files,
            max_workers=max_processes,
        )

    column_titles = ["Total Bytes", "Chunk Bytes", "Compressed Chunk Bytes"]
    if with_json:
        stats = [json_files, files, cdc_snappy_files, cdc_zstd_files]
        titles = ["JSONLines", "Parquet", "CDC Snappy", "CDC ZSTD"]
    else:
        stats = [files, cdc_snappy_files, cdc_zstd_files]
        titles = ["Parquet", "CDC Snappy", "CDC ZSTD"]

    results = []
    for i, paths in enumerate(stats):
        title = titles[i]
        print(f"Estimating deduplication for {title}")
        results.append({"title": title, **de.estimate(paths)})
    pretty_print_stats(results)

    # plot the results using plotly if available
    y_keys = ["total_len", "chunk_bytes", "compressed_chunk_bytes"]
    fig = go.Figure(
        data=[
            go.Bar(
                name=results[i]["title"],
                x=column_titles,
                y=[results[i][k] for k in y_keys],
            )
            for i in range(len(results))
        ]
    )
    fig.show()


@cli.command()
@click.argument("files", nargs=-1, type=click.Path(exists=True))
def dedup(files):
    de.estimate(files)


@cli.command()
@click.argument("template", type=click.Path(exists=True))
def render_readme(template):
    # open the README file and render it using jinja2
    from jinja2 import Template

    readme = Path(template)
    content = Template(readme.read_text()).render()
    readme.with_suffix("").write_text(content)
