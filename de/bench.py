"""Benchmark how parquet writers keep content defined chunks across row groups.

Every writer implementation rewrites the same source files with several row group
layouts, both with a single write call and streamed in batches. Page cuts are
measured against a single row group oracle (see `de.pages.persistence`) and the
deduplication of inserted and deleted rows is estimated. A run is saved as
`results.json` next to its files, so runs made in different environments (e.g.
with different pyarrow builds) can be joined with `compare_runs`.
"""

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import platform
from typing import Any, Optional, Sequence

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from .estimate import compare_formats_tables
from .formats import CdcParams, Parquet, ParquetCpp, ParquetJava, ParquetRs, exact_size
from .pages import Layout, PageDiff, compare, persistence
from .synthetic import DataGenerator

WRITERS: dict[str, type[Parquet]] = {
    "parquet-cpp": ParquetCpp,
    "parquet-rs": ParquetRs,
    "parquet-java": ParquetJava,
}
# parquet-java needs PARQUET_JAVA_CLASSPATH, so it is opt-in
DEFAULT_WRITERS = ("parquet-cpp", "parquet-rs")
EDITS = ("inserted", "deleted")
SEED = 42
# pyarrow caps row groups at 64Mi rows, the oracle must fit into a single one
MAX_ROW_GROUP_SIZE = 64 * 2**20
# large enough to never cut: pages end at CDC chunk and row group boundaries only
NO_PAGE_LIMITS: dict[str, Any] = dict(data_page_size=2**30, max_rows_per_page=2**30)
# a multiple of the 1024 values mini-batches arrow-rs checks its row limit after,
# so both implementations cut at the same rows
ALIGNED_PAGE_LIMITS: dict[str, Any] = dict(max_rows_per_page=20 * 1024)
PROFILES = ("cut", "aligned", "default", "dict", "nocdc")


def _sha256(path: Path) -> str:
    with open(path, "rb") as f:
        return hashlib.file_digest(f, "sha256").hexdigest()


def load_sources(directory: Path) -> dict:
    """Load a sources manifest, verifying that the files didn't change."""
    manifest = json.loads((directory / "manifest.json").read_text())
    for entry in manifest["files"].values():
        path = directory / entry["path"]
        if _sha256(path) != entry["sha256"]:
            raise ValueError(f"{path} doesn't match its manifest")
    return manifest


def prepare_sources(
    directory: Path,
    schema: dict,
    num_rows: int,
    edit_points: list[float],
    edit_size: int,
) -> dict:
    """Generate the original, inserted and deleted tables once as plain parquet files.

    The edited versions have edit_size rows inserted or deleted at each edit point,
    a relative position in the original. Existing sources are reused if they were
    generated with the same parameters.
    """
    params = dict(
        schema=schema,
        num_rows=num_rows,
        edit_points=edit_points,
        edit_size=edit_size,
        seed=SEED,
    )
    if (directory / "manifest.json").exists():
        manifest = load_sources(directory)
        if {key: manifest.get(key) for key in params} != params:
            raise ValueError(f"{directory} has sources with different parameters")
        return manifest

    gen = DataGenerator(schema, seed=SEED)
    original = gen.generate_table(num_rows)
    tables = {
        "original": original,
        "inserted": gen.insert_rows(original, edit_points, edit_size=edit_size),
        "deleted": gen.delete_rows(original, edit_points, edit_size=edit_size),
    }
    directory.mkdir(parents=True, exist_ok=True)
    files = {}
    for name, table in tables.items():
        path = directory / f"{name}.parquet"
        pq.write_table(
            table,
            path,
            compression="none",
            use_dictionary=False,
            row_group_size=2**20,
        )
        files[name] = dict(path=path.name, num_rows=len(table), sha256=_sha256(path))
    manifest = dict(
        params,
        edit_rows=[int(point * num_rows) for point in edit_points],
        numpy=np.__version__,
        pyarrow=pa.__version__,
        files=files,
    )
    (directory / "manifest.json").write_text(json.dumps(manifest, indent=2))
    return manifest


def matrix(
    writers: list[type[Parquet]],
    num_rows: int,
    cdc: CdcParams,
    compression: str,
    row_group_sizes: list[int],
    batch_sizes: list[int],
    profiles: Sequence[str] = PROFILES,
) -> dict[str, list[Parquet]]:
    """Formats to write, per selected profile.

    cut      PLAIN without page limits, pages end at CDC and row group boundaries;
             the single row group layout is each writer's oracle
    aligned  PLAIN with a row limit both implementations cut at identically
    default  PLAIN with the writers' default page limits
    dict     dictionary encoding with default page limits
    nocdc    CDC disabled, the deduplication baseline

    Layouts are a single write call per row group size, plus streaming in each
    batch size with the largest row group size.
    """
    if "cut" in profiles or "aligned" in profiles:
        assert num_rows <= MAX_ROW_GROUP_SIZE, "the oracle needs a single row group"
    largest = max(row_group_sizes)
    plain: dict[str, Any] = dict(
        use_cdc=cdc, compression=compression, use_dictionary=False
    )

    def layouts(writer: type[Parquet], **params) -> list[Parquet]:
        streamed = batch_sizes if writer.streaming else []
        return [writer(row_group_size=size, **params) for size in row_group_sizes] + [
            writer(row_group_size=largest, batch_size=size, **params)
            for size in streamed
        ]

    formats: dict[str, list[Parquet]] = defaultdict(list)
    for writer in writers:
        formats["cut"] += [writer(row_group_size=num_rows, **plain, **NO_PAGE_LIMITS)]
        formats["cut"] += layouts(writer, **plain, **NO_PAGE_LIMITS)
        formats["aligned"] += [
            writer(row_group_size=size, **plain, **ALIGNED_PAGE_LIMITS)
            for size in (num_rows, largest)
        ]
        formats["default"] += layouts(writer, **plain)
        formats["dict"] += [
            writer(row_group_size=largest, use_cdc=cdc, compression=compression)
        ]
        formats["nocdc"] += [
            writer(row_group_size=largest, **dict(plain, use_cdc=False))
        ]
    # drop duplicates, e.g. when num_rows is one of the row group sizes
    return {profile: list(dict.fromkeys(formats[profile])) for profile in profiles}


def layout_label(fmt: Parquet) -> str:
    assert fmt.row_group_size is not None
    label = f"rg={exact_size(fmt.row_group_size)}"
    if fmt.batch_size is not None:
        label += f" stream={exact_size(fmt.batch_size)}"
    return label


def _cell(
    profile: str, fmt: Parquet, edit: str, paths: list[Path], run_dir: Path
) -> dict:
    return dict(
        profile=profile,
        writer=fmt.name,
        params=fmt.paramstem,
        layout=layout_label(fmt),
        edit=edit,
        format=asdict(fmt),
        files=[str(path.relative_to(run_dir)) for path in paths],
    )


def run(
    run_dir: Path,
    sources_dir: Path,
    profiles: dict[str, list[Parquet]],
    sanity_check: bool = True,
    max_workers: Optional[int] = None,
) -> dict:
    """Write every profile's formats, measure them and save results.json.

    max_workers bounds the files written and estimated concurrently, every single
    write call holds a whole source table in memory.
    """
    manifest = load_sources(sources_dir)
    sources = {name: sources_dir / f["path"] for name, f in manifest["files"].items()}
    num_rows = manifest["num_rows"]
    cells = []

    # page cut profiles only rewrite the original
    for profile in ("cut", "aligned"):
        formats = profiles.get(profile, [])
        directories = [run_dir / profile / "original" / fmt.name for fmt in formats]
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(
                    fmt.write, "original", sources["original"], directory, sanity_check
                )
                for fmt, directory in zip(formats, directories)
            ]
            layouts = {fmt: Layout.read(f.result()) for fmt, f in zip(formats, futures)}
        oracles = {
            fmt.name: layout
            for fmt, layout in layouts.items()
            if fmt.row_group_size == num_rows and fmt.batch_size is None
        }
        for fmt, future in zip(formats, futures):
            cell = _cell(profile, fmt, "original", [future.result()], run_dir)
            if profile == "cut":
                persisted = persistence(layouts[fmt], oracles[fmt.name])
                cell["persistence"] = [asdict(result) for result in persisted]
            cells.append(cell)

    # deduplication profiles rewrite the original and each edited version
    groups: dict[str, dict[str, Path | pa.Table]] = {
        edit: {"original": sources["original"], edit: sources[edit]} for edit in EDITS
    }
    for profile in ("default", "dict", "nocdc"):
        estimates = compare_formats_tables(
            profiles.get(profile, []),
            groups,
            run_dir / profile,
            max_workers=max_workers,
            sanity_check=sanity_check,
        )
        for result in estimates:
            assert isinstance(result.format, Parquet)
            original, edited = result.paths
            cell = _cell(profile, result.format, result.group, result.paths, run_dir)
            cell["dedup"] = dict(
                total_len=result.total_len,
                chunk_bytes=result.chunk_bytes,
                dedup_ratio=result.dedup_ratio,
                xet_bytes=result.xet_bytes,
                xet_dedup_ratio=result.xet_dedup_ratio,
            )
            edited_layout = Layout.read(edited)
            diffs = compare(Layout.read(original), edited_layout)
            cell["pages"] = [asdict(diff) for diff in diffs]
            cell["empty_pages"] = sum(
                edited_layout.empty_pages(column) for column in edited_layout.columns
            )
            cells.append(cell)

    created_by: dict[str, str] = {}
    for cell in cells:
        if cell["writer"] not in created_by:
            metadata = pq.read_metadata(run_dir / cell["files"][0])
            created_by[cell["writer"]] = metadata.created_by
    results_json = dict(
        environment=dict(
            python=platform.python_version(),
            platform=platform.platform(),
            numpy=np.__version__,
            pyarrow=pa.__version__,
            arrow_git_id=pa.cpp_build_info.git_id,
            arrow_git_description=pa.cpp_build_info.git_description,
            created_by=created_by,
        ),
        sources=dict(manifest, directory=str(sources_dir.resolve())),
        cells=cells,
    )
    (run_dir / "results.json").write_text(json.dumps(results_json, indent=2))
    return results_json


def load_run(run_dir: Path) -> dict:
    return json.loads((run_dir / "results.json").read_text())


def _versus(diffs: list[PageDiff]) -> str:
    """Summarize how a file differs from the same cell of the first run."""
    differ = sum(diff.left_only_cuts + diff.right_only_cuts for diff in diffs)
    if differ:
        return f"{differ} cuts differ"
    if any(diff.new_bytes for diff in diffs):
        return "same cuts"
    return "identical"


def compare_runs(run_dirs: list[Path]) -> dict:
    """Join the cells of saved runs into report rows, grouped into sections.

    Rows of a section share the profile, edit and layout; within a section rows
    are ordered by writer, then by run.
    """
    runs = [load_run(run_dir) for run_dir in run_dirs]
    digests = [
        {name: f["sha256"] for name, f in run["sources"]["files"].items()}
        for run in runs
    ]
    if any(digest != digests[0] for digest in digests):
        raise ValueError("the runs were made from different source files")
    labels = [str(run_dir) for run_dir in run_dirs]

    # only cells with the same parameters are compared between runs
    sections: dict[tuple, dict[tuple, dict[int, dict]]] = {}
    for index, run in enumerate(runs):
        for cell in run["cells"]:
            section = sections.setdefault(
                (cell["profile"], cell["edit"], cell["layout"]), {}
            )
            section.setdefault((cell["writer"], cell["params"]), {})[index] = cell

    persistence_rows, dedup_rows = [], []
    for (profile, edit, layout), by_writer in sections.items():
        for (writer, _), by_run in by_writer.items():
            first = None
            for index, cell in sorted(by_run.items()):
                row = dict(
                    profile=profile,
                    edit=edit,
                    layout=layout,
                    writer=writer,
                    run=labels[index],
                )
                if "persistence" in cell:
                    current = Layout.read(run_dirs[index] / cell["files"][-1])
                    versus = _versus(compare(first, current)) if first else ""
                    first = first or current
                    for result in cell["persistence"]:
                        persistence_rows.append(dict(row, **result, versus=versus))
                if "dedup" in cell:
                    pages = cell["pages"]
                    new_bytes = sum(diff["new_bytes"] for diff in pages) / sum(
                        diff["right_bytes"] for diff in pages
                    )
                    dedup_rows.append(
                        dict(
                            row,
                            **cell["dedup"],
                            new_bytes=new_bytes,
                            empty_pages=cell["empty_pages"],
                        )
                    )

    # the same data written by different implementations with a single write call:
    # each run's first writer is compared to the other writers of the same run, or
    # of the last run having them, e.g. a run with a pyarrow build of another
    # branch to the arrow-rs files of a previous run
    cross_impl_rows = []
    for (profile, edit, layout), by_writer in sections.items():
        if profile not in ("cut", "aligned"):
            continue
        by_params: dict[str, dict[str, dict[int, dict]]] = defaultdict(dict)
        for (writer, params), by_run in by_writer.items():
            if next(iter(by_run.values()))["format"]["batch_size"] is None:
                by_params[params][writer] = by_run
        for writers in by_params.values():
            (left, left_runs), *others = writers.items()
            for index, left_cell in sorted(left_runs.items()):
                left_layout = Layout.read(run_dirs[index] / left_cell["files"][0])
                for right, right_runs in others:
                    right_index = index if index in right_runs else max(right_runs)
                    right_cell = right_runs[right_index]
                    right_layout = Layout.read(
                        run_dirs[right_index] / right_cell["files"][0]
                    )
                    diffs = compare(left_layout, right_layout)
                    pair = f"{left} vs {right}"
                    if right_index != index:
                        pair += f" of {labels[right_index]}"
                    cross_impl_rows.append(
                        dict(
                            profile=profile,
                            layout=layout,
                            pair=pair,
                            run=labels[index],
                            common=sum(diff.common_cuts for diff in diffs),
                            left_only=sum(diff.left_only_cuts for diff in diffs),
                            right_only=sum(diff.right_only_cuts for diff in diffs),
                        )
                    )

    return dict(
        runs=[
            dict(label=label, **run["environment"]) for label, run in zip(labels, runs)
        ],
        persistence=persistence_rows,
        dedup=dedup_rows,
        cross_impl=cross_impl_rows,
    )
