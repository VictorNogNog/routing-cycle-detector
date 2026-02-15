"""Parsing utilities for bucket records."""

from collections.abc import Iterable, Iterator

from routing_cycle_detector.graph.types import BucketRecord


def parse_bucket_line(raw_line: bytes) -> BucketRecord | None:
    """
    Parse one raw bucket line into a record tuple.

    Returns None for empty or malformed lines.
    """
    line = raw_line.rstrip(b"\n\r")
    if not line:
        return None

    parts = line.split(b"|", 3)
    if len(parts) < 4:
        return None

    source, dest, claim_id, status = parts
    return source, dest, claim_id, status


def iter_bucket_records(lines: Iterable[bytes]) -> Iterator[BucketRecord]:
    """Yield parsed records from raw lines, skipping invalid records."""
    for raw_line in lines:
        parsed = parse_bucket_line(raw_line)
        if parsed is not None:
            yield parsed


def read_bucket_records(bucket_path: str) -> Iterator[BucketRecord]:
    """Read and parse all records from a bucket file."""
    with open(bucket_path, "rb") as handle:
        yield from iter_bucket_records(handle)
