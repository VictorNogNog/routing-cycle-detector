import zlib
from collections import OrderedDict
from pathlib import Path

# 1MB buffer for efficient I/O
BUFFER_SIZE = 1024 * 1024

# Maximum number of file handles to keep open at once (LRU cache limit)
MAX_OPEN_HANDLES = 128


class LRUFileCache:
    """
    LRU cache for file handles to prevent file descriptor exhaustion.

    Keeps at most MAX_OPEN_HANDLES files open, evicting least-recently-used
    handles when the limit is exceeded.
    """

    def __init__(self, max_handles: int, tmp_dir: Path):
        self._max_handles = max_handles
        self._tmp_dir = tmp_dir
        self._cache: OrderedDict[int, object] = OrderedDict()

    def _get_path(self, bucket_idx: int) -> Path:
        return self._tmp_dir / f"bucket_{bucket_idx:04d}.bin"

    def write(self, bucket_idx: int, data: bytes) -> None:
        """Write data to the specified bucket, opening handle if needed."""
        if bucket_idx in self._cache:
            self._cache.move_to_end(bucket_idx)
            handle = self._cache[bucket_idx]
        else:
            while len(self._cache) >= self._max_handles:
                _, old_handle = self._cache.popitem(last=False)
                old_handle.close()

            handle = open(self._get_path(bucket_idx), "ab", buffering=BUFFER_SIZE)
            self._cache[bucket_idx] = handle

        handle.write(data)

    def close_all(self) -> None:
        """Close all open file handles."""
        for handle in self._cache.values():
            handle.close()
        self._cache.clear()


def partition_to_buckets(
    input_path: str,
    num_buckets: int,
    tmp_dir: str,
) -> list[Path]:
    """
    Partition input file into buckets based on (claim_id, status_code) hash.

    Uses an LRU cache to limit the number of open file handles.

    Args:
        input_path: Path to the input file (pipe-delimited).
        num_buckets: Number of buckets (should be power of 2 for fast modulo).
        tmp_dir: Directory to store bucket files.

    Returns:
        List of paths to non-empty bucket files.
    """
    tmp_path = Path(tmp_dir)
    tmp_path.mkdir(parents=True, exist_ok=True)

    bucket_mask = num_buckets - 1
    cache = LRUFileCache(MAX_OPEN_HANDLES, tmp_path)

    try:
        with open(input_path, "rb") as f:
            for line in f:
                line = line.rstrip(b"\n\r")
                if not line:
                    continue

                parts = line.split(b"|", 3)
                if len(parts) < 4:
                    continue

                claim_bytes = parts[2]
                status_bytes = parts[3]

                # Stable hash for bucket assignment
                bucket_idx = zlib.crc32(claim_bytes + b"|" + status_bytes) & bucket_mask

                # Write raw line bytes with newline
                cache.write(bucket_idx, line + b"\n")

    finally:
        cache.close_all()

    # Return only non-empty bucket files
    non_empty = [
        tmp_path / f"bucket_{i:04d}.bin"
        for i in range(num_buckets)
        if (tmp_path / f"bucket_{i:04d}.bin").exists()
        and (tmp_path / f"bucket_{i:04d}.bin").stat().st_size > 0
    ]
    return non_empty
