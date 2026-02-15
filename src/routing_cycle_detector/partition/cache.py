"""File-handle cache used by partitioning."""

from collections import OrderedDict
from pathlib import Path
from typing import BinaryIO

from routing_cycle_detector.partition.types import BUFFER_SIZE


class LRUFileCache:
    """LRU cache for file handles to prevent file descriptor exhaustion."""

    def __init__(self, max_handles: int, tmp_dir: Path):
        self._max_handles = max_handles
        self._tmp_dir = tmp_dir
        self._cache: OrderedDict[int, BinaryIO] = OrderedDict()

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

            handle = open(self._get_path(bucket_idx), "ab", buffering=BUFFER_SIZE)  # noqa: SIM115
            self._cache[bucket_idx] = handle

        handle.write(data)

    def close_all(self) -> None:
        """Close all open file handles."""
        for handle in self._cache.values():
            handle.close()
        self._cache.clear()
