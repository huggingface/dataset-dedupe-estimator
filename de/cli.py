import json

import functools

from pathlib import Path
import sys
import tempfile

import click
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm.contrib.concurrent import process_map
from rich.console import Console
from rich.table import Table
import humanize
import plotly.graph_objects as go


from .estimate import estimate_de, estimate_xtool
from .fileutils import (
    rewrite_to_parquet,
    rewrite_to_jsonlines,
    checkout_file_revisions,
    get_page_chunk_sizes,
)
from .synthetic import (
    generate_alterated_tables,
    write_and_compare_parquet,
    write_and_compare_json,
    convert_dedupe_images_to_png,
)


def pyarrow_has_cdc():
    # check that pyarrow is compoiled with cdc support
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir = Path(temp_dir)
        table = pa.table({"id": [1, 2, 3, 4, 5]})
        try:
            pq.write_table(table, temp_dir / "test.parquet", use_content_defined_chunking=True)
        except TypeError:
            return False
    return True


def pretty_print_stats(results):
    # dump the results to the console as a rich formatted table
    console = Console()
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Title")
    table.add_column("Total Size", justify="right")
    table.add_column("Chunk Size", justify="right")
    table.add_column("Compressed Chunk Size", justify="right")
    table.add_column("Dedup Ratio", justify="right")
    table.add_column("Compressed Dedup Ratio", justify="right")
    table.add_column("Transmitted XTool Bytes", justify="right")
    for i, row in enumerate(results):
        table.add_row(
            row["title"],
            humanize.naturalsize(row["total_len"], binary=True),
            humanize.naturalsize(row["chunk_bytes"], binary=True),
            humanize.naturalsize(row["compressed_chunk_bytes"], binary=True),
            "{:.0%}".format(row["chunk_bytes"] / results[i]["total_len"]),
            "{:.0%}".format(row["compressed_chunk_bytes"] / results[i]["total_len"]),
            humanize.naturalsize(row["transmitted_xtool_bytes"], binary=True)
            if "transmitted_xtool_bytes" in row
            else "",
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
    """Generate synthetic data and compare the deduplication ratios.
    de synthetic -s 1 -e 1 '{"a": "int"}'
    de synthetic -s 1 -e 2 '{"a": "int"}'
    de synthetic -s 4 -e 1 '{"a": "int"}'
    de synthetic -s 4 -e 2 '{"a": "int"}'
    de synthetic -s 1 -e 1 '{"a": "int", "b": "str", "c": ["int"]}'
    de synthetic -s 1 -e 2 '{"a": "int", "b": "str", "c": ["int"]}'
    de synthetic -s 4 -e 1 '{"a": "int", "b": "str", "c": ["int"]}'
    de synthetic -s 4 -e 2 '{"a": "int", "b": "str", "c": ["int"]}'
    de render-readme README.md.jinja2
    """
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
        cdc=False,
        use_dictionary=use_dictionary,
    )
    results += write_and_compare_parquet(
        directory,
        original,
        tables,
        prefix=prefix,
        postfix="cdc",
        cdc=True,
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
@click.option("--skip-zstd", is_flag=True, help="Skip ZSTD rewrite")
@click.option("--skip-snappy", is_flag=True, help="Skip Snappy rewrite")
@click.option("--skip-rewrite", is_flag=True, help="Skip file rewriting")
@click.option("--skip-json-rewrite", is_flag=True, help="Skip JSON rewrite")
@click.option("--skip-parquet-rewrite", is_flag=True, help="Skip Parquet rewrite")
@click.option(
    "--disable-dictionary", is_flag=True, help="Disallow parquet dictionary encoding"
)
@click.option(
    "--cdc-min-size", default=256, help="Minimum CDC chunk size in KiB", type=int
)
@click.option(
    "--cdc-max-size", default=1024, help="Maximum CDC chunk size in KiB", type=int
)
@click.option(
    "--data-page-size", default=1024 * 1024, help="Parquet data page size in bytes", type=int
)
@click.option("--cdc-norm-factor", default=0, help="CDC normalization factor", type=int)
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
    skip_zstd,
    skip_snappy,
    skip_rewrite,
    skip_json_rewrite,
    skip_parquet_rewrite,
    disable_dictionary,
    cdc_min_size,
    cdc_max_size,
    cdc_norm_factor,
    data_page_size,
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

    kwargs = {
        "use_content_defined_chunking": {
            "min_chunk_size": cdc_min_size * 1024, 
            "max_chunk_size": cdc_max_size * 1024, 
            "norm_factor": cdc_norm_factor
        },
        "use_dictionary": not disable_dictionary,
        "data_page_size": data_page_size,
    }
    if not (skip_rewrite or skip_parquet_rewrite or skip_zstd):
        print("Writing CDC Parquet files with ZSTD compression")
        if max_processes == 1:
            for src_path, dst_path in zip(files, cdc_zstd_files):
                rewrite_to_parquet(src_path, dst_path, compression="snappy", **kwargs)
        else:
            process_map(
                functools.partial(rewrite_to_parquet, compression="zstd", **kwargs),
                files,
                cdc_zstd_files,
                max_workers=max_processes,
            )
    if not (skip_rewrite or skip_parquet_rewrite or skip_snappy):
        print("Writing CDC Parquet files with Snappy compression")
        if max_processes == 1:
            for src_path, dst_path in zip(files, cdc_snappy_files):
                rewrite_to_parquet(src_path, dst_path, compression="snappy", **kwargs)
        else:
            process_map(
                functools.partial(rewrite_to_parquet, compression="snappy", **kwargs),
                files,
                cdc_snappy_files,
                max_workers=max_processes,
            )

    column_titles = [
        "Total Bytes",
        "Chunk Bytes",
        "Compressed Chunk Bytes",
        "Transmitted XTool Bytes",
    ]
    inputs = {}
    if with_json:
        inputs["JSONLines"] = json_files
    inputs["Parquet"] = files
    if not skip_zstd:
        inputs["CDC ZSTD"] = cdc_zstd_files
    if not skip_snappy:
        inputs["CDC Snappy"] = cdc_snappy_files

    results = []
    for title, paths in inputs.items():
        print(f"Estimating deduplication for {title}")
        results.append({"title": title, **estimate_de(paths), **estimate_xtool(paths)})
    pretty_print_stats(results)

    # plot the results using plotly with bars grouped by metric
    y_keys = [
        "total_len",
        "chunk_bytes",
        "compressed_chunk_bytes",
        "transmitted_xtool_bytes",
    ]
    fig = go.Figure(
        data=[
            go.Bar(
                name=column_title,
                x=[r["title"] for r in results],
                y=[r[y_keys[i]] for r in results],
            )
            for i, column_title in enumerate(column_titles)
        ]
    )
    fig.update_layout(barmode="group", yaxis=dict(tickformat=".2s", title="Bytes"))
    fig.show()


@cli.command()
@click.argument("files", nargs=-1, type=click.Path(exists=True))
def dedup(files):
    estimate_de(files)


@cli.command()
@click.argument("files", nargs=-1, type=click.Path(exists=True))
def rewrite(files):
    for path in files:
        path = Path(path)
        out = path.with_name(path.stem + "-dedup.parquet")
        rewrite_to_parquet(path, out, cdc=True)


@cli.command()
@click.argument("template", type=click.Path(exists=True))
def render_readme(template):
    # open the README file and render it using jinja2
    from jinja2 import Template

    readme = Path(template)
    content = Template(readme.read_text()).render()
    readme.with_suffix("").write_text(content)


@cli.command()
@click.argument("patterns", nargs=-1, type=str)
def page_chunks(patterns):
    paths = []
    for pattern in patterns:
        if "*" in pattern:
            paths.extend(Path().rglob(pattern))
        else:
            paths.append(Path(pattern))

    uncompressed_bytes, num_values = zip(*get_page_chunk_sizes(paths))

    fig = go.Figure()

    fig.add_trace(
        go.Histogram(
            x=uncompressed_bytes,
            nbinsx=100,
            name="Uncompressed Page Sizes",
            marker_color="blue",
            opacity=0.75,
        )
    )

    fig.update_layout(
        title="Distribution of Uncompressed Page Sizes",
        xaxis_title="Value",
        yaxis_title="Frequency",
        barmode="overlay",
    )

    fig.update_xaxes(tickformat=".2s")
    fig.show()
