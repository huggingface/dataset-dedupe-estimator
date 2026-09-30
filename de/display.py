from humanize import naturalsize
from rich.bar import Bar
from rich.console import Console
from rich.table import Table
from rich.text import Text
import plotly.graph_objects as go

from .pages import Layout, PageDiff


def _bar_color(r, numfiles):
    ideal = 1 / numfiles

    fracs = [0.067, 0.25, 0.5, 0.75, 0.933]
    thresholds = [ideal + (1.0 - ideal) * f for f in fracs]

    colors = ["bright_green", "green", "yellow", "orange3", "red", "bright_red"]
    for threshold, color in zip(thresholds, colors):
        if r <= threshold:
            return color
    return colors[-1]


def ratio_cell(ratio: float, numfiles: int, width: int = 20, style=None) -> Table:
    color = _bar_color(ratio, numfiles)
    bar = Bar(1.0, 0, ratio, width=width, color=color, bgcolor="grey23")
    grid = Table.grid(padding=(0, 1, 0, 0))
    grid.add_column(justify="right", min_width=4, no_wrap=True)
    grid.add_column(min_width=width)
    grid.add_row(Text("{:.0%}".format(ratio), style=style), bar)
    return grid


def print_table(results: list) -> None:
    """Display results as a Rich formatted table."""
    has_xet = results[0].xet_bytes is not None

    results = sorted(results, key=lambda r: (r.group, r.dedup_ratio))

    best_ratio: dict[str, float] = {}
    for row in results:
        g = row.group
        r = round(row.dedup_ratio, 2)
        if g not in best_ratio or r < best_ratio[g]:
            best_ratio[g] = r

    console = Console()
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Group", justify="left")
    table.add_column("Format", justify="left")
    table.add_column("Params", justify="left")
    table.add_column("Total Size", justify="right")
    table.add_column("Chunk Size", justify="right")
    table.add_column("Deduped Ratio ↓", justify="left")
    if has_xet:
        table.add_column("Xet Size", justify="right")
        table.add_column("Xet Deduped Ratio", justify="left")

    prev_group = None
    for row in results:
        group = row.group
        is_first_in_group = group != prev_group
        if is_first_in_group:
            table.add_section()
            prev_group = group

        ratio = round(row.dedup_ratio, 2)
        is_best = ratio == best_ratio[group]
        style = "bold green" if is_best else ""

        values = [
            row.group if is_first_in_group else "",
            Text(row.format.name, style=style),
            Text(row.format.paramstem, style=style),
            Text(naturalsize(row.total_len, binary=True), style=style),
            Text(naturalsize(row.chunk_bytes, binary=True), style=style),
            ratio_cell(ratio, row.numfiles, style=style),
        ]
        if has_xet:
            xet_ratio = round(row.xet_dedup_ratio, 2)
            values.extend(
                [
                    Text(naturalsize(row.xet_bytes, binary=True), style=style),
                    ratio_cell(xet_ratio, row.numfiles, style=style),
                ]
            )

        table.add_row(*values)

    console.print(table)


def plot_bars(results: list, output_html: str | None = None) -> None:
    """Display deduplication ratios as horizontal grouped bars: format on y, params as series."""
    groups = sorted(set(r.group for r in results))
    params_list = sorted(set(r.format.paramstem for r in results))
    multi_group = len(groups) > 1

    def fmt_key(r):
        return f"{r.format.name} ({r.group})" if multi_group else r.format.name

    # Sort formats by best ratio; reversed so best appears at top
    format_best: dict[str, float] = {}
    for r in results:
        k = fmt_key(r)
        if k not in format_best or r.dedup_ratio < format_best[k]:
            format_best[k] = r.dedup_ratio
    sorted_formats = sorted(format_best, key=lambda f: format_best[f], reverse=True)

    by_key = {(fmt_key(r), r.format.paramstem): r.dedup_ratio for r in results}

    fig = go.Figure()
    for params in params_list:
        ratios = [by_key.get((fmt, params)) for fmt in sorted_formats]
        fig.add_trace(
            go.Bar(
                name=params or "default",
                y=sorted_formats,
                x=ratios,
                orientation="h",
                text=[f"{v:.1%}" if v is not None else "" for v in ratios],
                textposition="outside",
            )
        )

    fig.update_layout(
        barmode="group",
        xaxis=dict(
            tickformat=".0%",
            title="Dedup ratio (lower = better)",
            range=[0, 1.0],
        ),
        template="plotly_white",
        showlegend=len(params_list) > 1 or multi_group,
        height=max(300, len(sorted_formats) * 80 + 100),
    )
    if output_html:
        fig.write_html(output_html, include_plotlyjs="cdn")
    fig.show()


def _table(*columns: str) -> Table:
    """A table with right justified columns, except those prefixed with '<'."""
    table = Table(show_header=True, header_style="bold magenta")
    for column in columns:
        left = column.startswith("<")
        table.add_column(column.lstrip("<"), justify="left" if left else "right")
    return table


def print_pages(layout: Layout) -> None:
    """Display every page of a parquet file, data pages with their level range."""
    table = _table(
        "Row Group",
        "<Column",
        "<Type",
        "<Encoding",
        "First Level",
        "Values",
        "Header",
        "Compressed",
        "Uncompressed",
        "<Page Hash",
    )
    levels = {column: 0 for column in layout.columns}
    previous = None
    for page in layout.pages:
        chunk = (page["row_group"], page["column"])
        if chunk != previous:
            table.add_section()
            previous = chunk
        is_data = page["page_type"] != "dictionary"
        first = f"{levels[page['column']]:,}" if is_data else ""
        if is_data:
            levels[page["column"]] += page["num_values"]
        table.add_row(
            str(page["row_group"]),
            page["path"],
            page["page_type"],
            page["encoding"],
            first,
            f"{page['num_values']:,}",
            f"{page['header_bytes']:,}",
            f"{page['compressed_bytes']:,}",
            f"{page['uncompressed_bytes']:,}",
            f"{page['page_hash']:016x}",
        )

    summary = _table("<Column", "Levels", "Data Pages", "Cuts", "Row Groups", "Empty")
    for column in layout.columns:
        spans = layout.spans(column)
        summary.add_row(
            next(p["path"] for p in layout.pages if p["column"] == column),
            f"{spans[-1].end:,}",
            f"{len(spans):,}",
            f"{len(layout.cuts(column)):,}",
            f"{len(layout.row_group_starts(column)) + 1:,}",
            f"{layout.empty_pages(column):,}",
        )
    console = Console()
    console.print(table)
    console.print(summary)


def print_page_diffs(diffs: list[PageDiff]) -> None:
    """Display the page boundary agreement of two files per column."""
    table = _table(
        "<Column",
        "Left Pages",
        "Right Pages",
        "Common Cuts",
        "Left Only",
        "Right Only",
        "First Divergence",
        "New Bytes",
    )
    for diff in diffs:
        table.add_row(
            diff.path,
            f"{diff.left_pages:,}",
            f"{diff.right_pages:,}",
            f"{diff.common_cuts:,}",
            f"{diff.left_only_cuts:,}",
            f"{diff.right_only_cuts:,}",
            "" if diff.first_divergence is None else f"{diff.first_divergence:,}",
            f"{diff.new_bytes / diff.right_bytes:.1%}",
        )
    Console().print(table)


def _print_rows(
    console: Console, title: str, rows: list[dict], section: tuple, columns: list
) -> None:
    """Print rows as a table of (header, cell) columns, one section per section key."""
    table = _table(*(header for header, _ in columns))
    previous = None
    for row in rows:
        key = tuple(row[name] for name in section)
        if key != previous:
            table.add_section()
            previous = key
        table.add_row(*(cell(row) for _, cell in columns))
    console.print(title)
    console.print(table)


def print_report(report: dict) -> None:
    """Display the page cut, deduplication and cross implementation results of runs."""
    console = Console()
    runs = _table("<Run", "<pyarrow", "<Arrow Git Id", "<Written By")
    for run in report["runs"]:
        runs.add_row(
            run["label"],
            run["pyarrow"],
            run["arrow_git_id"][:12],
            "\n".join(run["created_by"].values()),
        )
    console.print(runs)

    # the run columns are only interesting when comparing runs
    multiple = len(report["runs"]) > 1
    run = [("<Run", lambda row: row["run"])] if multiple else []
    versus = [("<vs First Run", lambda row: row["versus"])] if multiple else []

    _print_rows(
        console,
        "Page cuts against each writer's single row group oracle (PLAIN, no page "
        "limits), ideally recall 100%, extra 0, aligned 100%, resync 0",
        report["persistence"],
        ("layout",),
        [
            ("<Layout", lambda row: row["layout"]),
            ("<Writer", lambda row: row["writer"]),
            *run,
            ("Col", lambda row: str(row["column"])),
            ("Recall", lambda row: f"{row['recall']:.1%}"),
            ("Extra", lambda row: f"{row['extra']:,}"),
            ("Aligned", lambda row: f"{row['aligned']:.1%}"),
            ("Resync", lambda row: f"{row['resync']:,.0f}"),
            ("Empty", lambda row: f"{row['empty']:,}"),
            *versus,
        ],
    )
    _print_rows(
        console,
        "Deduplication of the original with its edited version (lower is better, "
        "50% is ideal), new bytes are the edited file's page bytes not in the "
        "original, empty pages are the edited file's data pages without values",
        report["dedup"],
        ("profile", "edit", "layout"),
        [
            ("<Profile", lambda row: row["profile"]),
            ("<Edit", lambda row: row["edit"]),
            ("<Layout", lambda row: row["layout"]),
            ("<Writer", lambda row: row["writer"]),
            *run,
            ("Total Size", lambda row: naturalsize(row["total_len"], binary=True)),
            ("Dedup Ratio", lambda row: f"{row['dedup_ratio']:.1%}"),
            ("Xet Dedup Ratio", lambda row: f"{row['xet_dedup_ratio']:.1%}"),
            ("New Bytes", lambda row: f"{row['new_bytes']:.1%}"),
            ("Empty Pages", lambda row: f"{row['empty_pages']:,}"),
        ],
    )
    _print_rows(
        console,
        "Page cuts of the same data written by different implementations",
        report["cross_impl"],
        ("profile", "layout"),
        [
            ("<Profile", lambda row: row["profile"]),
            ("<Layout", lambda row: row["layout"]),
            ("<Pair", lambda row: row["pair"]),
            *run,
            ("Common", lambda row: f"{row['common']:,}"),
            ("Left Only", lambda row: f"{row['left_only']:,}"),
            ("Right Only", lambda row: f"{row['right_only']:,}"),
        ],
    )
