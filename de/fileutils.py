import json
import subprocess
import os
from pathlib import Path

import pyarrow.parquet as pq


def rewrite_to_parquet(src_path, dest_path, block_size=1024 * 1024, **kwargs):
    """
    Reads a Parquet file in blocks and writes them out to another file.

    :param src_path: Path to the source Parquet file.
    :param dest_path: Path to the destination Parquet file.
    :param block_size: Size of the blocks to read and write in bytes.
    """
    src_path = Path(src_path)
    dest_path = Path(dest_path)

    with pq.ParquetFile(src_path) as src:
        schema = src.schema.to_arrow_schema()
        writer = pq.ParquetWriter(dest_path, schema, **kwargs)
        for batch in src.iter_batches(batch_size=block_size):
            writer.write(batch, row_group_size=1024 * 1024)
        writer.close()

    src = pq.ParquetFile(src_path)
    dst = pq.ParquetFile(dest_path)
    src_metadata = src.metadata
    dst_metadata = dst.metadata

    assert src_metadata.num_rows == dst_metadata.num_rows
    assert (
        src_metadata.schema.to_arrow_schema() == dst_metadata.schema.to_arrow_schema()
    )


def rewrite_to_jsonlines(src, dest, **kwargs):
    table = pq.read_table(src)
    table.to_pandas().to_json(dest, orient="records", lines=True, **kwargs)


def rewrite_to_sqlite(src, dest, **kwargs):
    """
    Reads a Parquet file and writes it out to a SQLite database.

    :param src: Path to the source Parquet file.
    :param dest: Path to the destination SQLite database.
    """
    table = pq.read_table(src)
    table.to_pandas().to_sql(dest.stem, dest, if_exists="replace", **kwargs)


def checkout_file_revisions(file_path, target_dir) -> list[str]:
    """
    Returns a list of short commit hashes for all revisions of the given file.
    """
    file_path = Path(file_path)
    target_dir = Path(target_dir)

    cwd = Path.cwd()
    try:
        os.chdir(file_path.parent)
        git_dir = Path(
            subprocess.check_output(
                ["git", "rev-parse", "--show-toplevel"], text=True
            ).strip()
        )
    finally:
        os.chdir(cwd)

    git_file = file_path.relative_to(git_dir)
    git_cmd = ["git", "-C", str(git_dir)]
    try:
        command = git_cmd + [
            "log",
            "--pretty=format:%h",
            "--follow",
            "--diff-filter=d",
            "--",
            str(git_file),
        ]
        output = subprocess.check_output(command, text=True)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Failed to retrieve revisions for {git_file}") from e

    revisions = output.strip().split("\n")
    print(f"{git_file} has {len(revisions)} revisions")
    for rev in revisions:
        print("Checking out", rev)
        command = git_cmd + [
            f"--work-tree={target_dir}",
            "checkout",
            rev,
            "--",
            str(git_file),
        ]
        try:
            subprocess.run(command, check=True)
        except subprocess.CalledProcessError as e:
            raise RuntimeError(
                f"Failed to checkout {file_path} at revision {rev}"
            ) from e
        # rename the file to include the commit hash
        new_file = target_dir / f"{file_path.stem}-{rev}{file_path.suffix}"
        os.rename(target_dir / git_file, new_file)


def get_page_chunk_sizes(paths):
    # get the result of parquet-layout command
    for path in paths:
        output = subprocess.check_output(["parquet-layout", path], text=True)
        meta = json.loads(output)
        for row_group in meta["row_groups"]:
            for column in row_group["columns"]:
                for page in column["pages"]:
                    if page["page_type"].startswith("data"):
                        yield page["uncompressed_bytes"], page["num_values"]
