from dataclasses import dataclass
from functools import partial
from operator import itemgetter
from typing import Any, Callable, Generic, Hashable, Iterable, Sequence, TypeVar

from humanize import naturalsize
from rich.bar import Bar
from rich.console import Console, JustifyMethod, RenderableType
from rich.table import Table
from rich.text import Text
import plotly.graph_objects as go

from .pages import Layout, PageDiff
from .parity import CASES, PAGE_VERSIONS, Parity, ParityReport

Row = TypeVar("Row")

_count = "{:,}".format
_percent = "{:.1%}".format
_size = partial(naturalsize, binary=True)


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
            Text(_size(row.total_len), style=style),
            Text(_size(row.chunk_bytes), style=style),
            ratio_cell(ratio, row.numfiles, style=style),
        ]
        if has_xet:
            xet_ratio = round(row.xet_dedup_ratio, 2)
            values.extend(
                [
                    Text(_size(row.xet_bytes), style=style),
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
                text=[_percent(v) if v is not None else "" for v in ratios],
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


@dataclass(frozen=True)
class Column(Generic[Row]):
    """A table column: its header, a row's value and how the value is shown.

    A row without a value (None) shows `missing` instead of failing to format it.
    """

    header: str
    value: Callable[[Row], Any]
    format: Callable[[Any], RenderableType] = str
    justify: JustifyMethod = "right"
    missing: RenderableType = ""

    @classmethod
    def text(cls, header: str, value: Callable[[Row], Any]) -> "Column[Row]":
        """A left justified column of the value as text."""
        return cls(header, value, justify="left")

    def cell(self, row: Row) -> RenderableType:
        value = self.value(row)
        return self.missing if value is None else self.format(value)


@dataclass(frozen=True)
class Listing(Generic[Row]):
    """A titled table with a row per item, consecutive rows with a different section
    key are separated."""

    title: str
    columns: Sequence[Column[Row]]
    section: Callable[[Row], Hashable] = lambda row: None

    def table(self, rows: Iterable[Row]) -> Table:
        table = Table(show_header=True, header_style="bold magenta")
        for column in self.columns:
            table.add_column(column.header, justify=column.justify)
        previous = None
        for row in rows:
            key = self.section(row)
            # a no-op before the first row
            if key != previous:
                table.add_section()
                previous = key
            table.add_row(*(column.cell(row) for column in self.columns))
        return table

    def print(self, console: Console, rows: Iterable[Row]) -> None:
        if self.title:
            console.print(self.title)
        console.print(self.table(rows))


def print_pages(layout: Layout, console: Console) -> None:
    """Display every page of a parquet file, data pages with their level range."""
    pages = []
    for column in layout.columns:
        starts = (span.start for span in layout.spans(column))
        for page in layout.column_pages(column):
            is_data = page["page_type"] != "dictionary"
            pages.append(dict(page, first_level=next(starts) if is_data else None))
    # file order: row group by row group, column by column
    pages.sort(key=itemgetter("row_group", "column"))
    Listing[dict](
        "Pages in file order, data pages with the level they start at",
        [
            Column("Row Group", itemgetter("row_group")),
            Column.text("Column", itemgetter("path")),
            Column.text("Type", itemgetter("page_type")),
            Column.text("Encoding", itemgetter("encoding")),
            Column("First Level", itemgetter("first_level"), _count),
            Column("Values", itemgetter("num_values"), _count),
            Column("Header", itemgetter("header_bytes"), _count),
            Column("Compressed", itemgetter("compressed_bytes"), _count),
            Column("Uncompressed", itemgetter("uncompressed_bytes"), _count),
            Column(
                "Page Hash", itemgetter("page_hash"), "{:016x}".format, justify="left"
            ),
        ],
        section=itemgetter("row_group", "column"),
    ).print(console, pages)
    Listing[int](
        "",
        [
            Column.text(
                "Column", lambda column: layout.column_pages(column)[0]["path"]
            ),
            Column("Levels", lambda column: layout.spans(column)[-1].end, _count),
            Column("Data Pages", lambda column: len(layout.spans(column)), _count),
            Column("Cuts", lambda column: len(layout.cuts(column)), _count),
            Column(
                "Row Groups",
                lambda column: len(layout.row_group_starts(column)) + 1,
                _count,
            ),
            Column("Empty", layout.empty_pages, _count),
        ],
    ).print(console, layout.columns)


def print_page_diffs(diffs: list[PageDiff], console: Console) -> None:
    """Display the page boundary agreement of two files per column."""
    Listing[PageDiff](
        "Page boundaries and page bytes of the right file compared to the left one",
        [
            Column.text("Column", lambda diff: diff.path),
            Column("Left Pages", lambda diff: diff.left_pages, _count),
            Column("Right Pages", lambda diff: diff.right_pages, _count),
            Column("Common Cuts", lambda diff: diff.common_cuts, _count),
            Column("Left Only", lambda diff: diff.left_only_cuts, _count),
            Column("Right Only", lambda diff: diff.right_only_cuts, _count),
            Column("First Divergence", lambda diff: diff.first_divergence, _count),
            Column(
                "New Bytes", lambda diff: diff.new_bytes / diff.right_bytes, _percent
            ),
            Column("New Payloads", lambda diff: diff.new_payloads, _count),
        ],
    ).print(console, diffs)


def _parity_cell(result: Parity) -> Text:
    if result.error is not None:
        return Text("error", style="bold red")
    empty = f", {_count(result.empty_pages)} empty" if result.empty_pages else ""
    if result.different_cuts:
        return Text(f"{_count(result.different_cuts)} cuts{empty}", style="red")
    if result.different_pages:
        pages = f"{_count(result.different_pages)}/{_count(result.pages)}"
        return Text(f"{pages}{empty}", style="yellow")
    return Text("✓", style="green")


def print_parity(report: ParityReport, console: Console) -> None:
    """Display each writer's page parity with parquet-cpp per page version, datatype
    and case."""
    Listing[str](
        "",
        [
            Column.text("Writer", str),
            Column.text("Created By", report.created_by.get),
        ],
    ).print(console, report.created_by)
    console.print(
        "Pages against parquet-cpp's: ✓ identical, N cuts in only one of the files, "
        "N/M of the M pages unmatched by a diff of the pages (row group, values and "
        "bytes, headers aside), K empty data pages, n/a when the writer can't "
        "express the case or write the type"
    )
    for writer in report.writers:
        for page_version in PAGE_VERSIONS:
            Listing[str](
                f"{writer}, {page_version} data pages",
                [
                    Column.text("Datatype", str),
                    *(
                        Column(
                            case,
                            partial(report.get, writer, page_version, case),
                            _parity_cell,
                            justify="left",
                            missing=Text("n/a", style="dim"),
                        )
                        for case in CASES
                    ),
                ],
            ).print(console, report.datatypes)
    for (writer, line), failed in report.errors().items():
        where = " ".join(failed[0].cell) if len(failed) == 1 else f"{len(failed)} cells"
        # assembled as plain text, errors may contain brackets rich would parse
        console.print(Text.assemble((f"{writer} {where}", "red"), f": {line}"))


def print_report(report: dict, console: Console) -> None:
    """Display the page cut, deduplication and cross implementation results of runs."""
    Listing[dict](
        "",
        [
            Column.text("Run", itemgetter("label")),
            Column.text("pyarrow", itemgetter("pyarrow")),
            Column.text("Arrow Git Id", lambda run: run["arrow_git_id"][:12]),
            Column.text(
                "Written By", lambda run: "\n".join(run["created_by"].values())
            ),
        ],
    ).print(console, report["runs"])

    # the run columns are only interesting when comparing runs
    multiple = len(report["runs"]) > 1
    run: list[Column[dict]] = (
        [Column.text("Run", itemgetter("run"))] if multiple else []
    )
    versus: list[Column[dict]] = (
        [Column.text("vs First Run", itemgetter("versus"))] if multiple else []
    )

    Listing[dict](
        "Page cuts against each writer's single row group oracle (PLAIN, no page "
        "limits), ideally recall 100%, extra 0, aligned 100%, resync 0",
        [
            Column.text("Layout", itemgetter("layout")),
            Column.text("Writer", itemgetter("writer")),
            *run,
            Column("Col", itemgetter("column")),
            Column("Recall", itemgetter("recall"), _percent),
            Column("Extra", itemgetter("extra"), _count),
            Column("Aligned", itemgetter("aligned"), _percent),
            Column("Resync", itemgetter("resync"), "{:,.0f}".format),
            Column("Empty", itemgetter("empty"), _count),
            *versus,
        ],
        section=itemgetter("layout"),
    ).print(console, report["persistence"])
    Listing[dict](
        "Deduplication of the original with its edited version (lower is better, "
        "50% is ideal), new bytes are the edited file's page bytes not in the "
        "original, empty pages are the edited file's data pages without values",
        [
            Column.text("Profile", itemgetter("profile")),
            Column.text("Edit", itemgetter("edit")),
            Column.text("Layout", itemgetter("layout")),
            Column.text("Writer", itemgetter("writer")),
            *run,
            Column("Total Size", itemgetter("total_len"), _size),
            Column("Dedup Ratio", itemgetter("dedup_ratio"), _percent),
            Column("Xet Dedup Ratio", itemgetter("xet_dedup_ratio"), _percent),
            Column("New Bytes", itemgetter("new_bytes"), _percent),
            Column("Empty Pages", itemgetter("empty_pages"), _count),
        ],
        section=itemgetter("profile", "edit", "layout"),
    ).print(console, report["dedup"])
    Listing[dict](
        "Page cuts of the same data written by different implementations",
        [
            Column.text("Profile", itemgetter("profile")),
            Column.text("Layout", itemgetter("layout")),
            Column.text("Pair", itemgetter("pair")),
            *run,
            Column("Common", itemgetter("common"), _count),
            Column("Left Only", itemgetter("left_only"), _count),
            Column("Right Only", itemgetter("right_only"), _count),
        ],
        section=itemgetter("profile", "layout"),
    ).print(console, report["cross_impl"])
