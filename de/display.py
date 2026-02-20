from pydoc import text
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


def print_table(results: list[dict]) -> None:
    """Display results as a Rich formatted table."""
    has_xet = "xet_bytes" in results[0]

    results = sorted(results, key=lambda r: (r["variant"], r["dedup_ratio"]))

    best_ratio = {}
    for row in results:
        g = row["variant"]
        r = round(row["dedup_ratio"], 2)
        if g not in best_ratio or r < best_ratio[g]:
            best_ratio[g] = r

    console = Console()
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Variant", justify="left")
    table.add_column("Format", justify="left")
    table.add_column("Params", justify="left")
    table.add_column("Total Size", justify="right")
    table.add_column("Chunk Size", justify="right")
    table.add_column("Deduped Size Ratio", justify="left")
    if has_xet:
        table.add_column("Xet Bytes", justify="right")
        table.add_column("Xet Deduped Size Ratio", justify="left")

    prev_group = None
    for row in results:
        group = row["variant"]
        is_first_in_group = group != prev_group
        if is_first_in_group:
            table.add_section()
            prev_group = group

        ratio = round(row["dedup_ratio"], 2)
        is_best = ratio == best_ratio[group]
        style = "bold green" if is_best else None

        values = [
            row["variant"] if is_first_in_group else "",
            Text(row["format"], style=style),
            Text(row["params"], style=style),
            Text(naturalsize(row["total_len"], binary=True), style=style),
            Text(naturalsize(row["chunk_bytes"], binary=True), style=style),
            ratio_cell(ratio, row["numfiles"], style=style),
        ]
        if has_xet:
            xet_ratio = round(row["xet_dedup_ratio"], 2)
            values.extend(
                [
                    Text(naturalsize(row["xet_bytes"], binary=True), style=style),
                    ratio_cell(xet_ratio, row["numfiles"], style=style),
                ]
            )

        table.add_row(*values)

    console.print(table)


def plot_bars(results: list[dict]) -> None:
    """Display deduplication ratios per format, sorted best to worst."""
    multi_variant = len(set(r["variant"] for r in results)) > 1

    def label(r):
        base = f"{r['format']} {r['params']}".strip()
        return f"{r['variant']} / {base}" if multi_variant else base

    sorted_results = sorted(results, key=lambda r: r["chunk_bytes"] / r["total_len"])
    ratios = [r["chunk_bytes"] / r["total_len"] for r in sorted_results]
    x_labels = [label(r) for r in sorted_results]

    fig = go.Figure(
        go.Bar(
            x=x_labels,
            y=ratios,
            text=[f"{v:.1%}" for v in ratios],
            textposition="outside",
        )
    )

    # Zoom into the meaningful range: just below the best result
    y_min = max(0, min(ratios) - 0.1)
    fig.update_layout(
        yaxis=dict(
            tickformat=".0%",
            title="Chunk ratio (lower = better dedup)",
            range=[y_min, 1.08],
        ),
        xaxis_title="Format",
        template="plotly_white",
        showlegend=False,
    )
    fig.show()


def plot_lines(
    x_values,
    y_series: dict[str, list],
    x_label: str,
    y_label: str = "Deduplication Ratio",
    output_html: str | None = None,
) -> None:
    """Display y_series as line+marker traces against x_values."""
    markers = ["circle", "square", "diamond", "cross"]
    fig = go.Figure()
    for i, (name, y_values) in enumerate(y_series.items()):
        fig.add_trace(
            go.Scatter(
                x=x_values,
                y=y_values,
                mode="lines+markers",
                name=name,
                marker=dict(symbol=markers[i % len(markers)]),
            )
        )
    fig.update_layout(
        title=f"{y_label} vs {x_label}",
        xaxis=dict(title=x_label, type="log", dtick=1, tickformat=".2s"),
        yaxis=dict(title=y_label, tickformat=".2%"),
        legend=dict(title="Metric"),
        template="plotly_white",
    )
    if output_html:
        fig.write_html(output_html, include_plotlyjs="cdn")
    fig.show()
