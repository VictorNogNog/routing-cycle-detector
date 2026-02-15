"""Partitioning orchestration for two-pass solving."""

import zlib
from pathlib import Path

from routing_cycle_detector.partition.cache import LRUFileCache
from routing_cycle_detector.partition.types import MAX_OPEN_HANDLES, PartitionStats


def partition_to_buckets(
    input_path: str,
    num_buckets: int,
    tmp_dir: str,
) -> tuple[list[Path], PartitionStats]:
    """
    Partition input file into buckets based on (claim_id, status_code) hash.

    Uses an LRU cache to limit the number of open file handles.
    """
    tmp_path = Path(tmp_dir)
    tmp_path.mkdir(parents=True, exist_ok=True)

    bucket_mask = num_buckets - 1
    cache = LRUFileCache(MAX_OPEN_HANDLES, tmp_path)
    stats = PartitionStats()

    try:
        with open(input_path, "rb") as handle:
            for line in handle:
                stats.lines_read += 1
                line = line.rstrip(b"\n\r")
                if not line:
                    stats.empty_lines += 1
                    continue

                parts = line.split(b"|", 3)
                if len(parts) < 4:
                    stats.malformed_lines += 1
                    continue

                claim_bytes = parts[2]
                status_bytes = parts[3]

                # Stable hash for bucket assignment.
                # Bitwise AND gives hash % num_buckets for power-of-two bucket counts.
                bucket_idx = zlib.crc32(claim_bytes + b"|" + status_bytes) & bucket_mask

                cache.write(bucket_idx, line + b"\n")
                stats.lines_written += 1

    finally:
        cache.close_all()

    # Return only non-empty bucket files.
    non_empty = [
        tmp_path / f"bucket_{i:04d}.bin"
        for i in range(num_buckets)
        if (tmp_path / f"bucket_{i:04d}.bin").exists()
        and (tmp_path / f"bucket_{i:04d}.bin").stat().st_size > 0
    ]
    return non_empty, stats
