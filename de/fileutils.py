import json
import subprocess
import os
from pathlib import Path


def checkout_file_revisions(
    file_path, target_dir, from_rev=None, until_rev="HEAD"
) -> None:
    """
    Checks out all revisions of the given file in the range (from_rev, until_rev].
    from_rev is exclusive (not included); until_rev is inclusive (defaults to HEAD).
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
        revision_range = f"{from_rev}..{until_rev}" if from_rev else until_rev
        command = git_cmd + [
            "log",
            "--pretty=format:%h",
            "--follow",
            "--diff-filter=d",
            revision_range,
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
