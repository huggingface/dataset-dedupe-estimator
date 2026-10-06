from dataclasses import dataclass
from pathlib import Path
from typing import Sequence
from concurrent.futures import Executor, ThreadPoolExecutor, as_completed

import pyarrow as pa
from tqdm import tqdm

from .core import estimate as _estimate_de, estimate_xet as _estimate_xet
from .formats import FileFormat


@dataclass
class EstimationResult:
    format: FileFormat
    numfiles: int
    total_len: int
    chunk_bytes: int
    compressed_chunk_bytes: int
    dedup_ratio: float
    xet_bytes: int
    xet_dedup_ratio: float
    group: str = ""


def estimate(paths, plot=True):
    """Estimate the deduplication of the files, plot writes PNGs of their chunks."""
    string_paths = list(map(str, paths))
    total_bytes, chunk_bytes, compressed_chunk_bytes = _estimate_de(string_paths, plot)
    xet_bytes = _estimate_xet(string_paths)
    return {
        "numfiles": len(string_paths),
        "total_len": total_bytes,
        "chunk_bytes": chunk_bytes,
        "compressed_chunk_bytes": compressed_chunk_bytes,
        "dedup_ratio": chunk_bytes / total_bytes,
        "xet_bytes": xet_bytes,
        "xet_dedup_ratio": xet_bytes / total_bytes,
    }


def write_formats_tables(
    formats: Sequence[FileFormat],
    tables: dict[str, dict[str, Path | pa.Table]],
    directory: Path,
    executor: Executor,
    sanity_check: bool = True,
) -> dict[tuple[str, FileFormat], list[Path]]:
    """Write every group's files with every format to directory/group/format name.

    tables maps group name -> {name: Path | pa.Table}; the written paths are
    returned per (group, format) in input order.
    """
    writes = {}
    for table_name, table in tables.items():
        for fmt in formats:
            prefix = directory / table_name / fmt.name
            prefix.mkdir(parents=True, exist_ok=True)
            writes[(table_name, fmt)] = [
                executor.submit(
                    fmt.write, name, value, prefix, sanity_check=sanity_check
                )
                for name, value in table.items()
            ]
    pending = [future for futures in writes.values() for future in futures]
    for _ in tqdm(as_completed(pending), total=len(pending)):
        pass
    return {key: [f.result() for f in futures] for key, futures in writes.items()}


def compare_formats_tables(
    formats: Sequence[FileFormat],
    tables: dict[str, dict[str, Path | pa.Table]],
    directory: Path | str,
    max_workers: int | None = None,
    sanity_check: bool = True,
) -> list[EstimationResult]:
    """For each format and group, write/rewrite files and estimate deduplication.

    tables maps group name -> {name: Path | pa.Table}. Every file of a group is
    estimated together, one record per (format, group).
    """
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        groups = write_formats_tables(
            formats, tables, Path(directory), executor, sanity_check
        )
        estimates = executor.map(estimate, groups.values())
        return [
            EstimationResult(format=fmt, group=table_name, **data)
            for (table_name, fmt), data in zip(groups, estimates)
        ]


def compare_formats(
    baseline: FileFormat,
    contenders: dict[str, FileFormat],
    table: pa.Table,
    directory: Path | str,
) -> list[EstimationResult]:
    """Write a table in the baseline format and each contender format, comparing
    each contender against the baseline. One record per contender."""
    directory = Path(directory)

    with ThreadPoolExecutor() as executor:
        # Submit all writes
        baseline_future = executor.submit(baseline.write, "baseline", table, directory)
        futures = {
            executor.submit(fmt.write, name, table, directory): name
            for name, fmt in contenders.items()
        }

        # Collect write results
        baseline_path = baseline_future.result()
        contender_paths = {}
        for future in tqdm(as_completed(futures), total=len(futures)):
            contender_paths[futures[future]] = future.result()

        # Estimate in parallel
        names = list(contender_paths.keys())
        path_pairs = ([baseline_path, contender_paths[name]] for name in names)
        estimates = executor.map(estimate, path_pairs)

    return [
        EstimationResult(format=contenders[name], group="param-impact", **data)
        for name, data in zip(names, estimates)
    ]
