"""Exact page boundaries of parquet files and comparisons between them.

Positions are level offsets: the running sum of the data pages' `num_values` of a
leaf column across the whole file. For flat columns a level is a row, for nested
columns levels are the coordinate shared by every writer (pages may split records).
"""

from bisect import bisect_left, bisect_right
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
from typing import Optional

from .core import parquet_pages


@dataclass(frozen=True)
class Span:
    """A data page's position in level space."""

    start: int
    end: int
    row_group: int


@dataclass(frozen=True)
class Layout:
    """All pages of a parquet file in file order, as returned by `de.core.parquet_pages`."""

    pages: list[dict]

    @classmethod
    def read(cls, path: str | Path) -> "Layout":
        return cls(parquet_pages(str(path)))

    @cached_property
    def _column_pages(self) -> dict[int, list[dict]]:
        pages: dict[int, list[dict]] = {}
        for page in self.pages:
            pages.setdefault(page["column"], []).append(page)
        return dict(sorted(pages.items()))

    @cached_property
    def _spans(self) -> dict[int, list[Span]]:
        spans: dict[int, list[Span]] = {}
        for column, pages in self._column_pages.items():
            start = 0
            spans[column] = []
            for page in pages:
                if page["page_type"] != "dictionary":
                    end = start + page["num_values"]
                    spans[column].append(Span(start, end, page["row_group"]))
                    start = end
        return spans

    @property
    def columns(self) -> list[int]:
        return list(self._column_pages)

    def column_pages(self, column: int) -> list[dict]:
        """The column's dictionary and data pages in file order, none if it's missing."""
        return self._column_pages.get(column, [])

    def spans(self, column: int) -> list[Span]:
        """Level ranges of the column's data pages, empty pages included."""
        return self._spans.get(column, [])

    def cuts(self, column: int) -> list[int]:
        """Level offsets where non-empty data pages start, excluding 0."""
        return sorted({s.start for s in self.spans(column) if s.end > s.start} - {0})

    def empty_pages(self, column: int) -> int:
        """Number of data pages without values."""
        return sum(1 for s in self.spans(column) if s.end == s.start)

    def row_group_starts(self, column: int) -> list[int]:
        """Level offsets where row groups start, excluding 0."""
        starts, previous = [], 0
        for span in self.spans(column):
            if span.row_group != previous:
                starts.append(span.start)
                previous = span.row_group
        return starts


@dataclass(frozen=True)
class PageDiff:
    """Page boundary agreement of one column between a left and a right file."""

    column: int
    path: str
    left_pages: int
    right_pages: int
    common_cuts: int
    left_only_cuts: int
    right_only_cuts: int
    # first level where the cut sets differ, None if they are identical
    first_divergence: Optional[int]
    # page bytes (headers included) of the right file
    right_bytes: int
    # bytes of the right file's pages not found byte-identical in the left file
    new_bytes: int


def compare(left: Layout, right: Layout) -> list[PageDiff]:
    """Compare the page cuts and page bytes of two files with the same columns."""
    diffs = []
    for column in right.columns:
        left_cuts, right_cuts = set(left.cuts(column)), set(right.cuts(column))
        different = left_cuts ^ right_cuts
        left_hashes = {p["page_hash"] for p in left.column_pages(column)}
        right_pages = right.column_pages(column)
        sizes = [p["header_bytes"] + p["compressed_bytes"] for p in right_pages]
        new = sum(
            size
            for page, size in zip(right_pages, sizes)
            if page["page_hash"] not in left_hashes
        )
        diffs.append(
            PageDiff(
                column=column,
                path=right_pages[0]["path"],
                left_pages=len(left.spans(column)),
                right_pages=len(right.spans(column)),
                common_cuts=len(left_cuts & right_cuts),
                left_only_cuts=len(left_cuts - right_cuts),
                right_only_cuts=len(right_cuts - left_cuts),
                first_divergence=min(different) if different else None,
                right_bytes=sum(sizes),
                new_bytes=new,
            )
        )
    return diffs


@dataclass(frozen=True)
class Persistence:
    """How well a file's CDC page cuts survive its row group boundaries.

    Measured against an oracle: the same data and writer settings with a single
    row group and page size limits disabled, so its cuts K are exactly the CDC
    chunk boundaries. A writer keeping its chunker state across row groups G
    reproduces K and adds only G, scoring recall=1, extra=0, aligned=1, resync=0.
    """

    column: int
    # share of the oracle cuts (except those on row group starts) reproduced
    recall: float
    # cuts that are neither oracle cuts nor row group starts
    extra: int
    # share of levels in pages that span exactly one segment between K ∪ G anchors
    aligned: float
    # mean levels from a row group start to the first aligned page after it
    resync: float
    # data pages without values
    empty: int


def persistence(layout: Layout, oracle: Layout) -> list[Persistence]:
    results = []
    for column in layout.columns:
        spans = layout.spans(column)
        num_levels = spans[-1].end
        assert oracle.spans(column)[-1].end == num_levels, "different data"

        oracle_cuts = set(oracle.cuts(column))
        row_group_starts = layout.row_group_starts(column)
        cuts = set(layout.cuts(column))
        expected = oracle_cuts - set(row_group_starts)
        anchors = sorted(oracle_cuts | set(row_group_starts) | {0, num_levels})

        def is_aligned(span: Span) -> bool:
            i = bisect_right(anchors, span.start) - 1
            return anchors[i] == span.start and anchors[i + 1] == span.end

        aligned = [s for s in spans if s.end > s.start and is_aligned(s)]
        aligned_starts = [s.start for s in aligned]
        resyncs = []
        for g, end in zip(row_group_starts, row_group_starts[1:] + [num_levels]):
            i = bisect_left(aligned_starts, g)
            first = aligned_starts[i] if i < len(aligned) else end
            resyncs.append(min(first, end) - g)

        results.append(
            Persistence(
                column=column,
                recall=len(expected & cuts) / len(expected) if expected else 1.0,
                extra=len(cuts - oracle_cuts - set(row_group_starts)),
                aligned=sum(s.end - s.start for s in aligned) / num_levels,
                resync=sum(resyncs) / len(resyncs) if resyncs else 0.0,
                empty=layout.empty_pages(column),
            )
        )
    return results
