import json
from pathlib import Path
import sys

import click
import numpy as np
import pyarrow.parquet as pq

from humanize import naturalsize
import plotly.io as pio
import plotly.graph_objects as go

from . import display
from .bench import (
    DEFAULT_WRITERS,
    PROFILES,
    WRITERS,
    compare_runs,
    load_sources,
    matrix,
    prepare_sources,
    run as run_bench,
)
from .estimate import estimate
from .fileutils import checkout_file_revisions
from .pages import Layout, compare
from .formats import FileFormat, ParquetCpp, ParquetRs, JsonLines, Sqlite, CdcParams
from .estimate import compare_formats_tables, compare_formats
from .synthetic import DataGenerator


pio.renderers.default = "browser"  # Opens in a new browser tab


@click.group()
@click.option("--plot", is_flag=True, help="Show plots after each command")
@click.pass_context
def cli(ctx, plot):
    ctx.ensure_object(dict)
    ctx.obj["plot"] = plot


@cli.command()
@click.pass_context
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
@click.option(
    "--edit-size", default=10, help="Number of rows to change in each edit", type=int
)
@click.option("--with-json", is_flag=True, help="Also calculate JSONLines stats")
@click.option("--with-sqlite", is_flag=True, help="Also calculate SQLite stats")
@click.option("--use-dictionary", is_flag=True, help="Use parquet dictionary encoding")
@click.option(
    "--row-group-size", default=None, help="Parquet row group size in rows", type=int
)
@click.option(
    "--cdc-min-size", default=256, help="Minimum CDC chunk size in KiB", type=int
)
@click.option(
    "--cdc-max-size", default=1024, help="Maximum CDC chunk size in KiB", type=int
)
@click.option("--cdc-norm-level", default=0, help="CDC normalization level", type=int)
@click.option(
    "--no-sanity-check", is_flag=True, help="Skip sanity check after writing files"
)
def synthetic(
    ctx,
    schema,
    size,
    num_edits,
    edit_size,
    target_dir,
    use_dictionary,
    row_group_size,
    with_json,
    with_sqlite,
    cdc_min_size,
    cdc_max_size,
    cdc_norm_level,
    no_sanity_check,
):
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

    edit_points = np.linspace(0.5 / num_edits, 1 - 0.5 / num_edits, num_edits)
    schema = json.loads(schema)

    gen = DataGenerator(schema, seed=42)
    original, tables = gen.generate_synthetic_tables(
        size=size * 2**20,
        edit_size=edit_size,
        edit_points=list(edit_points),
        append_ratio=0.05,
        update_columns=list(schema.keys()),
    )

    cdc_params = CdcParams(
        min_chunk_size=cdc_min_size * 1024,
        max_chunk_size=cdc_max_size * 1024,
        norm_level=cdc_norm_level,
    )
    formats: list[FileFormat] = [
        writer(
            use_cdc=use_cdc,
            compression=compression,
            use_dictionary=use_dictionary,
            row_group_size=row_group_size,
        )
        for writer in (ParquetCpp, ParquetRs)
        for use_cdc in (False, cdc_params)
        for compression in ("snappy", "zstd")
    ]
    if with_json:
        formats += [JsonLines(), JsonLines("zstd")]
    if with_sqlite:
        formats.append(Sqlite())

    prefix = f"s{size}c{len(schema)}e{num_edits}"
    variants = {
        f"{prefix}-{name}": {"original": original, name: edit_table}
        for name, edit_table in tables.items()
    }
    results = compare_formats_tables(
        formats, variants, directory, sanity_check=not no_sanity_check
    )

    display.print_table(results)
    if ctx.obj["plot"]:
        display.plot_bars(results)


@cli.command()
@click.argument("files", nargs=-1, type=click.Path(exists=True))
@click.option(
    "--target-dir",
    "-d",
    help="Directory to store the revisions",
    type=click.Path(file_okay=False, writable=True),
    required=True,
)
@click.option("--from-rev", default=None, help="Start of revision range (exclusive)")
@click.option(
    "--until-rev",
    default="HEAD",
    help="End of revision range (inclusive, default: HEAD)",
)
def revisions(files, target_dir, from_rev, until_rev):
    """Checkout all revisions of the given files and calculate the deduplication ratio."""
    target_dir = Path("revisions") if target_dir is None else Path(target_dir)
    target_dir.mkdir(exist_ok=True)
    for file_path in files:
        checkout_file_revisions(
            file_path, target_dir=target_dir, from_rev=from_rev, until_rev=until_rev
        )


@cli.command()
@click.pass_context
@click.argument("directory", type=click.Path(exists=True, file_okay=False))
@click.option("--with-json", is_flag=True, help="Also calculate JSONLines stats")
@click.option("--with-sqlite", is_flag=True, help="Also calculate SQLite stats")
@click.option(
    "--cdc-min-size", default=256, help="Minimum CDC chunk size in KiB", type=int
)
@click.option(
    "--cdc-max-size", default=1024, help="Maximum CDC chunk size in KiB", type=int
)
@click.option("--cdc-norm-level", default=0, help="CDC normalization level", type=int)
@click.option(
    "--data-page-size", default=None, help="Parquet data page size in bytes", type=int
)
@click.option("--row-group-size", default=None, help="Parquet row group size", type=int)
@click.option(
    "--no-sanity-check", is_flag=True, help="Skip sanity check after writing files"
)
def stats(
    ctx,
    directory,
    with_json,
    with_sqlite,
    cdc_min_size,
    cdc_max_size,
    cdc_norm_level,
    data_page_size,
    row_group_size,
    no_sanity_check,
):
    """Compare deduplication ratios across formats for parquet files in a directory."""
    directory = Path(directory)
    files = sorted(directory.glob("*.parquet"))

    tables = {"combined": {p.stem: p for p in files}}

    cdc_params = CdcParams(
        min_chunk_size=cdc_min_size * 1024,
        max_chunk_size=cdc_max_size * 1024,
        norm_level=cdc_norm_level,
    )
    formats: list[FileFormat] = [
        writer(
            use_cdc=use_cdc,
            compression="snappy",
            data_page_size=data_page_size,
            row_group_size=row_group_size,
            batch_size=2**20,
        )
        for writer in (ParquetCpp, ParquetRs)
        for use_cdc in (False, cdc_params)
    ]
    if with_json:
        formats.insert(0, JsonLines())
    if with_sqlite:
        formats.insert(0, Sqlite())

    results = compare_formats_tables(
        formats,
        tables,
        directory,
        sanity_check=not no_sanity_check,
    )
    display.print_table(results)
    if ctx.obj["plot"]:
        display.plot_bars(results)


@cli.command()
@click.argument("files", nargs=-1, type=click.Path(exists=True))
def dedup(files):
    result = estimate(files)
    print(
        f"Deduplication ratio: {result['dedup_ratio']:.2%} ({naturalsize(result['chunk_bytes'])} / {naturalsize(result['total_len'])})"
    )
    print(
        f"Xet deduplication ratio: {result['xet_dedup_ratio']:.2%} ({naturalsize(result['xet_bytes'])} / {naturalsize(result['total_len'])})"
    )


@cli.command()
@click.argument("files", nargs=-1, type=click.Path(exists=True))
def rewrite(files):
    fmt = ParquetCpp(use_cdc=True, batch_size=2**20)
    for path in files:
        path = Path(path)
        fmt.write(path.stem + "-dedup", path, path.parent)


@cli.command()
@click.argument("template", type=click.Path(exists=True))
def render_readme(template):
    # open the README file and render it using jinja2
    from jinja2 import Template

    readme = Path(template)
    content = Template(readme.read_text()).render()
    readme.with_suffix("").write_text(content)


@cli.command()
@click.pass_context
@click.argument("patterns", nargs=-1, type=str)
def page_chunks(ctx, patterns):
    paths = []
    for pattern in patterns:
        if "*" in pattern:
            paths.extend(Path().rglob(pattern))
        else:
            paths.append(Path(pattern))

    uncompressed_bytes = [
        page["uncompressed_bytes"]
        for path in paths
        for page in Layout.read(path).pages
        if page["page_type"] != "dictionary"
    ]

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
    if ctx.obj["plot"]:
        fig.show()


@cli.command
@click.pass_context
@click.argument("file", type=Path)
@click.argument("directory", type=Path)
@click.option(
    "--row-group-size",
    is_flag=True,
    help="Calculate the impact of row group size on deduplication ratio",
)
@click.option(
    "--data-page-size",
    is_flag=True,
    help="Calculate the impact of data page size on deduplication ratio",
)
def param_impact(ctx, file, directory, row_group_size, data_page_size):
    if row_group_size:
        param_name = "row_group_size"
        param_default = 2**20
        param_values = [2**i for i in range(11, 23)]
    elif data_page_size:
        param_name = "data_page_size"
        param_default = 2**20
        param_values = [2**i for i in range(12, 23)]
    else:
        print("Please specify either --row-group-size or --data-page-size")
        sys.exit(1)

    directory.mkdir(exist_ok=True)
    table = pq.read_table(file)

    baseline_fmt = ParquetCpp(use_cdc=True, **{param_name: param_default})
    contenders = {
        f"{param_name}={v}": ParquetCpp(use_cdc=True, **{param_name: v})
        for v in param_values
    }
    results = compare_formats(baseline_fmt, contenders, table, directory)

    display.print_table(results)
    if ctx.obj["plot"]:
        display.plot_bars(results)


@cli.command()
@click.argument("run_dir", type=click.Path(file_okay=False, path_type=Path))
@click.option(
    "--sources",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    default=None,
    help="Source files of another run to rewrite, defaults to RUN_DIR/sources",
)
@click.option("--schema", default='{"a": "int"}', help="Schema of the generated data")
@click.option("--num-rows", default=16 * 2**20, help="Rows to generate", type=int)
@click.option(
    "--num-edits", default=10, help="Evenly spaced edits to make in the data", type=int
)
@click.option(
    "--edit-point",
    "edit_points",
    multiple=True,
    type=float,
    help="Relative position of an edit, repeatable, overrides --num-edits",
)
@click.option(
    "--edit-size", default=10, help="Rows to insert or delete per edit", type=int
)
@click.option("--compression", default="snappy", help="Parquet compression")
@click.option(
    "--row-group-size",
    "row_group_sizes",
    multiple=True,
    type=int,
    default=(2**20, 2**18, 2**16),
    help="Row group size in rows, repeatable",
)
@click.option(
    "--batch-size",
    "batch_sizes",
    multiple=True,
    type=int,
    default=(100_000,),
    help="Streaming batch size in rows, repeatable",
)
@click.option(
    "--writer",
    "writers",
    multiple=True,
    type=click.Choice(list(WRITERS)),
    default=DEFAULT_WRITERS,
    help="Writer implementation, repeatable",
)
@click.option(
    "--cdc-min-size", default=256, help="Minimum CDC chunk size in KiB", type=int
)
@click.option(
    "--cdc-max-size", default=1024, help="Maximum CDC chunk size in KiB", type=int
)
@click.option("--cdc-norm-level", default=0, help="CDC normalization level", type=int)
@click.option(
    "--profile",
    "profiles",
    multiple=True,
    type=click.Choice(PROFILES),
    default=PROFILES,
    help="Profile to run, repeatable",
)
@click.option(
    "--max-workers",
    default=None,
    type=int,
    help="Files written and estimated concurrently",
)
@click.option(
    "--no-sanity-check", is_flag=True, help="Skip sanity check after writing files"
)
def bench(
    run_dir,
    sources,
    schema,
    num_rows,
    num_edits,
    edit_points,
    edit_size,
    compression,
    row_group_sizes,
    batch_sizes,
    writers,
    cdc_min_size,
    cdc_max_size,
    cdc_norm_level,
    profiles,
    max_workers,
    no_sanity_check,
):
    """Measure how parquet writers keep CDC page cuts across row groups.

    Rewrites the same generated data with every writer, row group size and
    streaming batch size, saves the results to RUN_DIR/results.json and shows
    them. Run it once per environment, e.g. with a pyarrow build of another
    Arrow branch, then compare the runs:

    \b
    de bench runs/stock
    de bench runs/cdc-branch --sources runs/stock/sources --writer parquet-cpp
    de compare runs/stock runs/cdc-branch
    """
    if sources is None:
        sources = run_dir / "sources"
        points = (
            list(edit_points)
            or np.linspace(0.5 / num_edits, 1 - 0.5 / num_edits, num_edits).tolist()
        )
        prepare_sources(sources, json.loads(schema), num_rows, points, edit_size)
    manifest = load_sources(sources)
    cdc = CdcParams(
        min_chunk_size=cdc_min_size * 1024,
        max_chunk_size=cdc_max_size * 1024,
        norm_level=cdc_norm_level,
    )
    formats = matrix(
        [WRITERS[name] for name in writers],
        manifest["num_rows"],
        cdc,
        compression,
        list(row_group_sizes),
        list(batch_sizes),
        profiles,
    )
    run_bench(
        run_dir,
        sources,
        formats,
        sanity_check=not no_sanity_check,
        max_workers=max_workers,
    )
    display.print_report(compare_runs([run_dir]))


@cli.command("compare")
@click.argument(
    "run_dirs",
    nargs=-1,
    required=True,
    type=click.Path(exists=True, file_okay=False, path_type=Path),
)
def compare_command(run_dirs):
    """Compare saved benchmark runs, the first run is the baseline."""
    display.print_report(compare_runs(list(run_dirs)))


@cli.command()
@click.argument("file", type=click.Path(exists=True, dir_okay=False, path_type=Path))
def pages(file):
    """Show the exact page layout of a parquet file."""
    display.print_pages(Layout.read(file))


@cli.command("page-diff")
@click.argument("left", type=click.Path(exists=True, dir_okay=False, path_type=Path))
@click.argument("right", type=click.Path(exists=True, dir_okay=False, path_type=Path))
def page_diff(left, right):
    """Compare the page boundaries and page bytes of two parquet files."""
    display.print_page_diffs(compare(Layout.read(left), Layout.read(right)))
