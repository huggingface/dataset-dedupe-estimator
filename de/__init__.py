import glob

import humanize

from .core import chunks
from .display import print_table
from .formats import ParquetCpp
from .estimate import estimate as _estimate, compare_formats_tables, compare_formats

__all__ = [
    "chunks",
    "compare_formats_tables",
    "compare_formats",
    "estimate",
    "visualize",
]


def estimate(*patterns):
    """Estimate the deduplication size of the given paths."""
    paths = sum([glob.glob(pattern) for pattern in patterns], [])
    result = _estimate(paths)
    print(f"Total size: {humanize.naturalsize(result['total_len'])}")
    print(f"Chunk size: {humanize.naturalsize(result['chunk_bytes'])}")
    print(f"Transmitted size (xet): {humanize.naturalsize(result['xet_bytes'])}")


_without_cdc_markdown_header = """
#### Parquet Deduplication for {name}

| Compression | Vanilla Parquet |
|-------------|-----------------|
"""

_with_cdc_markdown_header = """
#### Parquet Deduplication for {name}
    
| Compression | Vanilla Parquet | CDC Parquet |
|-------------|-----------------|-------------|
"""


def visualize(
    original,
    tables,
    directory=".",
    prefix="temp",
    with_cdc=False,
    compressions=("none", "snappy"),
    **parquet_options,
):
    formats = [ParquetCpp(c, use_cdc=False, **parquet_options) for c in compressions]
    if with_cdc:
        formats += [
            ParquetCpp(c, use_cdc=True, **parquet_options) for c in compressions
        ]
        header = _with_cdc_markdown_header
    else:
        header = _without_cdc_markdown_header
    results = compare_formats_tables(
        formats, {"original": original, **tables}, directory, prefix
    )

    for name in tables.keys():
        markdown_table = header.format(name=name.capitalize())
        for compression in compressions:
            nocdc_kind = ParquetCpp(compression, use_cdc=False).kind
            row = f"| {compression.capitalize()} "
            path = f"{prefix}-{nocdc_kind}-{name.lower()}.parquet.png"
            row += f"| ![Vanilla Parquet {compression}]({path}) "
            if with_cdc:
                cdc_kind = ParquetCpp(compression, use_cdc=True).kind
                path = f"{prefix}-{cdc_kind}-{name.lower()}.parquet.png"
                row += f"| ![CDC Parquet {compression}]({path}) "
            markdown_table += row + "|\n"
        from IPython.display import display, Markdown

        display(Markdown(markdown_table))

    print_table(results)


def visualize_multidoc_diff(file_paths):
    """
    Visualize a multi-document diff as vertical strips.

    Each document is a vertical strip, and chunks are aligned horizontally.
    Each document has its own color; shared chunks are colored by the doc's
    color but faded if not unique.
    """
    data = chunks(file_paths)

    # Distinct colors for up to 12 docs, can extend if needed
    colors = [
        "#1f77b4",
        "#ff7f0e",
        "#2ca02c",
        "#d62728",
        "#9467bd",
        "#8c564b",
        "#e377c2",
        "#7f7f7f",
        "#bcbd22",
        "#17becf",
        "#a55194",
        "#393b79",
    ]
    # Collect all document indices
    doc_indices = set()
    max_size = 0
    for hash, chunk in data:
        doc_indices.update(chunk["seen_in"])
        max_size = max(max_size, chunk["size"])
    doc_indices = sorted(doc_indices)
    num_docs = max(doc_indices) + 1 if doc_indices else 0

    # Build HTML for each document strip
    html = '<div style="display: flex; flex-direction: row; align-items: flex-end; gap: 8px;">'
    for doc_idx in range(num_docs):
        color = colors[doc_idx % len(colors)]
        strip_html = '<div style="display: flex; flex-direction: column; margin: 0 2px; align-items: stretch;">'
        for hash, chunk in data:
            if doc_idx in chunk["seen_in"]:
                # If chunk is in this doc, use this doc's color
                opacity = 1.0 if len(chunk["seen_in"]) == 1 else 0.5
            else:
                # Not present in this doc
                opacity = 0.15

            # normalize height according to max_size
            height_px = (chunk["size"] / max_size) * 10
            # 1px is the minimum height, 10px is the maximum height
            height_px = min(max(1, height_px), 10)
            strip_html += (
                f'<div title="chunk {hash} in doc {doc_idx}" '
                f'style="background-color: {color}; opacity: {opacity}; height: {height_px}px; width: 36px; border-bottom: 0px solid #fff;"></div>'
            )
        html += strip_html + "</div>"

    html += "</div>"
    html += '<p style="font-size: 15px;">Each vertical strip is a document. Chunks are aligned horizontally. Each doc has its own color; faded color means shared chunk, gray means not present.</p>'

    from IPython.display import display, HTML

    display(HTML(html))
