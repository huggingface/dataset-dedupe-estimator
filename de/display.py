from humanize import naturalsize
from rich.bar import Bar
from rich.console import Console
from rich.table import Table
from rich.text import Text
import plotly.graph_objects as go


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
    table.add_column("Deduped Ratio", justify="left")
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
    format_best = {}
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
