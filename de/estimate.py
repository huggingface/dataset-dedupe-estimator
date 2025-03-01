import subprocess
import tempfile
import os

from .core import estimate


def estimate_de(paths):
    string_paths = list(map(str, paths))
    total_bytes, chunk_bytes, compressed_chunk_bytes = estimate(string_paths)
    return {
        "total_len": total_bytes,
        "chunk_bytes": chunk_bytes,
        "compressed_chunk_bytes": compressed_chunk_bytes,
    }


def estimate_xtool(paths):
    with tempfile.NamedTemporaryFile(suffix=".json") as tmp:
        env = os.environ.copy()
        env["DEFAULT_MIN_N_CHUNKS_PER_RANGE"] = "1"
        cmd = [
            "xtool",
            "--repo-type",
            "dataset",
            "--repo-id",
            "kszucs/pq",
            "--token",
            os.environ["XTOOL_TOKEN"],
            "dedup",
            "-o",
            tmp.name,
            *map(str, paths),
        ]
        result = subprocess.run(
            cmd, check=True, capture_output=True, text=True, env=env
        )
        # with open(tmp.name) as f:
        #    stats = json.load(f)
        # segments = {path: int(stat["len"]) for path, stat in stats.items()}

    # stderr looks like:
    # 'Dedupping 26 files...\nUsing lz4 compression\n\n\nClean results:\nTransmitted 3180990288 bytes in total.\n'
    transmitted = int(result.stderr.splitlines()[-1].split()[1])
    return {"transmitted_xtool_bytes": transmitted}
