import glob
from dataclasses import dataclass

from IPython.display import display, Markdown, HTML
import humanize
import pyarrow as pa

from .core import chunks
from .estimate import estimate_de, estimate_xtool
from .synthetic import write_and_compare_parquet, pretty_print_stats

__all__ = [
    "chunks",
    "estimate",
    "estimate_de",
    "estimate_xtool",
    "visualize",
]


def estimate(*patterns, xtool=False):
    """Estimate the deduplication size of the given paths."""
    paths = sum([glob.glob(pattern) for pattern in patterns], [])

    de_result = estimate_de(paths)
    print(f"Total size: {humanize.naturalsize(de_result['total_len'])}")
    print(f"Chunk size: {humanize.naturalsize(de_result['chunk_bytes'])}")

    if xtool:
        xtool_result = estimate_xtool(paths)
        print(
            f"Transmitted size (xtool): {humanize.naturalsize(xtool_result['transmitted_xtool_bytes'])}"
        )


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
    results = write_and_compare_parquet(
        directory,
        original,
        tables,
        prefix=prefix,
        postfix="nocdc",
        compressions=compressions,
        use_content_defined_chunking=False,
        **parquet_options,
    )
    if with_cdc:
        results += write_and_compare_parquet(
            directory,
            original,
            tables,
            prefix=prefix,
            postfix="cdc",
            compressions=compressions,
            use_content_defined_chunking=True,
            **parquet_options,
        )
        header = _with_cdc_markdown_header
    else:
        header = _without_cdc_markdown_header

    for name in tables.keys():
        markdown_table = header.format(name=name.capitalize())
        for compression in compressions:
            row = f"| {compression.capitalize()} "
            path = f"{prefix}-{compression}-{name.lower()}-nocdc.parquet.png"
            row += f"| ![Vanilla Parquet {compression}]({path}) "
            if with_cdc:
                path = f"{prefix}-{compression}-{name.lower()}-cdc.parquet.png"
                row += f"| ![CDC Parquet {compression}]({path}) "
            markdown_table += row + "|\n"
        display(Markdown(markdown_table))

    pretty_print_stats(results)


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

    display(HTML(html))
