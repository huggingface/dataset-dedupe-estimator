from . import core


def estimate(paths):
    string_paths = list(map(str, paths))
    total_bytes, chunk_bytes, compressed_chunk_bytes = core.estimate(string_paths)
    return {
        "total_len": total_bytes,
        "chunk_bytes": chunk_bytes,
        "compressed_chunk_bytes": compressed_chunk_bytes,
    }
