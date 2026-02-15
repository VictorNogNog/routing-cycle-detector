"""Shared constants and metadata structures for partitioning."""

from dataclasses import dataclass

# 1MB buffer for efficient I/O.
BUFFER_SIZE = 1024 * 1024

# Maximum number of file handles to keep open at once (LRU cache limit).
MAX_OPEN_HANDLES = 128


@dataclass
class PartitionStats:
    """Statistics from partition_to_buckets operation."""

    lines_read: int = 0
    empty_lines: int = 0
    malformed_lines: int = 0
    lines_written: int = 0
