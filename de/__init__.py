import glob
from pathlib import Path

from IPython.display import display, Markdown
import humanize

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


def estimate(*patterns):
    """Estimate the deduplication size of the given paths."""
    paths = sum([glob.glob(pattern) for pattern in patterns], [])

    de_result = estimate_de(paths)
    xtool_result = estimate_xtool(paths)

    print(f"Total size: {humanize.naturalsize(de_result['total_len'])}")
    print(f"Chunk size: {humanize.naturalsize(de_result['chunk_bytes'])}")
    print(
        f"Compressed chunk size: {humanize.naturalsize(de_result['compressed_chunk_bytes'])}"
    )
    print(
        f"Transmitted size (xtool): {humanize.naturalsize(xtool_result['transmitted_xtool_bytes'])}"
    )


_markdown_header = """
#### Parquet Deduplication for {name}
    
| Variant | No Compression | Zstd Compression  | Snappy Compression |
|---------|----------------|-------------------|--------------------|
"""


def visualize(
    original,
    tables,
    directory=".",
    prefix="temp",
    with_content_defined_chunking=False,
    **parquet_options,
):
    results = write_and_compare_parquet(
        Path(directory),
        original,
        tables,
        prefix=prefix,
        postfix="nocdc",
        use_content_defined_chunking=False,
        **parquet_options,
    )
    if with_content_defined_chunking:
        results += write_and_compare_parquet(
            Path(directory),
            original,
            tables,
            prefix=prefix,
            postfix="cdc",
            use_content_defined_chunking=True,
            **parquet_options,
        )

    for name in tables.keys():
        markdown_table = _markdown_header.format(name=name.capitalize())

        row_vanilla = "| Vanilla Parquet "
        for compression in ["none", "zstd", "snappy"]:
            heatmap_path_nocdc = (
                f"{prefix}-{compression}-{name.lower()}-nocdc.parquet.png"
            )
            row_vanilla += f"| ![{name} Vanilla]({heatmap_path_nocdc}) "
        markdown_table += row_vanilla + "|\n"

        if with_content_defined_chunking:
            row_cdc = "| CDC Parquet "
            for compression in ["none", "zstd", "snappy"]:
                heatmap_path_cdc = (
                    f"{prefix}-{compression}-{name.lower()}-cdc.parquet.png"
                )
                row_cdc += f"| ![{name} CDC]({heatmap_path_cdc}) "
            markdown_table += row_cdc + "|\n"

        markdown_table += "\n"
        display(Markdown(markdown_table))

    pretty_print_stats(results)
