from collections import defaultdict
from pathlib import Path
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed

import pyarrow as pa
from tqdm import tqdm

from .core import estimate, estimate_xet as _estimate_xet
from .formats import FileFormat


def estimate_de(paths):
    string_paths = list(map(str, paths))
    total_bytes, chunk_bytes, compressed_chunk_bytes = estimate(string_paths)
    return {
        "total_len": total_bytes,
        "chunk_bytes": chunk_bytes,
        "compressed_chunk_bytes": compressed_chunk_bytes,
    }


def estimate_xet(paths):
    xet_bytes = _estimate_xet(list(map(str, paths)))
    return {"xet_bytes": xet_bytes}


def compare_formats_tables(
    formats: list[FileFormat],
    tables: dict[str, dict[str, Path | pa.Table]],
    directory: Path | str,
    metrics: tuple[Callable, ...] = (estimate_de, estimate_xet),
    max_workers: int | None = None,
    sanity_check: bool = True,
) -> list[dict]:
    """For each format and variant, write/rewrite files and estimate deduplication.

    tables maps variant name -> {name: Path | pa.Table}.
    Path values: rewrite all files, estimate across the group (one record per
    (format, variant)). pa.Table values: compare first (original) against second
    (edit), one record per (format, variant).
    """
    directory = Path(directory)

    def compute_metrics(variant, fmt, out_paths):
        record = {
            "format": fmt.name,
            "params": fmt.paramstem,
            "variant": variant,
            "numfiles": len(out_paths),
        }
        for fn in metrics:
            record.update(fn([out_paths[k] for k in sorted(out_paths)]))
        record["dedup_ratio"] = record["chunk_bytes"] / record["total_len"]
        if "xet_bytes" in record:
            record["xet_dedup_ratio"] = record["xet_bytes"] / record["total_len"]
        return record

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all file writes
        futures = {}
        for variant, data in tables.items():
            for fmt in formats:
                prefix = directory / variant / fmt.name
                prefix.mkdir(parents=True, exist_ok=True)
                for name, value in data.items():
                    f = executor.submit(
                        fmt.write, name, value, prefix, sanity_check=sanity_check
                    )
                    futures[f] = (variant, fmt, name)

        # Collect results as they complete, grouping paths by (fmt, variant)
        groups: defaultdict[tuple, dict] = defaultdict(dict)
        for future in tqdm(as_completed(futures), total=len(futures)):
            variant, fmt, name = futures[future]
            groups[(variant, fmt)][name] = future.result()

        # Submit metric computation for each group
        metric_futures = []
        for (variant, fmt), out_paths in groups.items():
            metric_futures.append(
                executor.submit(compute_metrics, variant, fmt, out_paths)
            )

        # Collect metric results as they complete
        records = []
        for future in tqdm(as_completed(metric_futures), total=len(metric_futures)):
            records.append(future.result())

    return records


def compare_formats(
    baseline: FileFormat,
    formats: list[FileFormat],
    table: pa.Table,
    directory: Path | str,
    prefix: str = "",
    metrics: tuple[Callable, ...] = (estimate_de, estimate_xet),
) -> list[dict]:
    """Write a table in the baseline format and each variant format, comparing
    each variant against the baseline. One record per format variant."""
    directory = Path(directory)
    baseline_path = baseline.write(prefix, table, directory)

    results = []
    for fmt in formats:
        path = fmt.write(prefix, table, directory)
        record = {
            "format": fmt.name,
            "params": fmt.paramstem,
        }
        for fn in metrics:
            record.update(fn([baseline_path, path]))
        results.append(record)

    return results
