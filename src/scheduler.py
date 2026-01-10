"""Orchestration: Coordinate partitioning and parallel cycle detection."""

import sys
import tempfile
import shutil
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from pathlib import Path

from .partition import partition_to_buckets
from .graph import process_bucket

# Chunksize for ProcessPoolExecutor.map to reduce IPC overhead
PROCESS_POOL_CHUNKSIZE = 16


def _is_gil_enabled() -> bool:
    """Check if GIL is enabled."""
    try:
        return sys._is_gil_enabled()
    except AttributeError:
        return True


def _get_executor_class():
    """
    Select the appropriate executor based on GIL status.

    If GIL is disabled (free-threading), use ThreadPoolExecutor.
    If GIL is enabled, fall back to ProcessPoolExecutor.
    """
    if _is_gil_enabled():
        return ProcessPoolExecutor
    else:
        return ThreadPoolExecutor


def solve(
    input_path: str,
    buckets: int = 1024,
    workers: int | None = None,
    verbose: bool = False,
) -> tuple[str, str, int] | None:
    """
    Find the longest cycle in the routing data.

    Two-pass algorithm:
    1. Partition data into buckets by (claim_id, status_code) hash
    2. Process each bucket in parallel to find cycles
    3. Reduce to find the global maximum

    Args:
        input_path: Path to the input file.
        buckets: Number of buckets for partitioning (power of 2).
        workers: Number of parallel workers (None = auto).
        verbose: Print progress information to stderr.

    Returns:
        Tuple of (claim_id, status_code, cycle_length) or None if no cycles.
    """
    input_path = str(Path(input_path).resolve())

    # Validate buckets is power of 2
    if buckets & (buckets - 1) != 0:
        raise ValueError(f"buckets must be a power of 2, got {buckets}")

    # Select executor based on GIL status
    ExecutorClass = _get_executor_class()
    use_process_pool = ExecutorClass is ProcessPoolExecutor

    if verbose:
        gil_status = "enabled" if _is_gil_enabled() else "disabled"
        print(f"GIL status: {gil_status}", file=sys.stderr)
        print(f"Using executor: {ExecutorClass.__name__}", file=sys.stderr)

    # Create temporary directory for buckets
    tmp_dir = tempfile.mkdtemp(prefix="routing_cycles_")

    try:
        # Pass 1: Partition to buckets
        if verbose:
            print(f"Pass 1: Partitioning to {buckets} buckets...", file=sys.stderr)

        bucket_paths = partition_to_buckets(input_path, buckets, tmp_dir)

        if verbose:
            print(f"  Created {len(bucket_paths)} non-empty buckets", file=sys.stderr)

        if not bucket_paths:
            return None

        # Pass 2: Process buckets in parallel using executor.map
        if verbose:
            print("Pass 2: Processing buckets...", file=sys.stderr)

        # Convert Path objects to strings for pickling (ProcessPoolExecutor)
        bucket_path_strs = [str(p) for p in bucket_paths]

        # Best result: (claim_id_bytes, status_bytes, cycle_len)
        best_result: tuple[bytes, bytes, int] | None = None

        with ExecutorClass(max_workers=workers) as executor:
            if use_process_pool:
                # Use chunksize to reduce IPC overhead
                results = executor.map(
                    process_bucket,
                    bucket_path_strs,
                    chunksize=PROCESS_POOL_CHUNKSIZE,
                )
            else:
                # ThreadPoolExecutor - chunksize not as critical
                results = executor.map(process_bucket, bucket_path_strs)

            for result in results:
                if result is not None:
                    if best_result is None or result[2] > best_result[2]:
                        best_result = result
                        if verbose:
                            claim_id = result[0].decode("utf-8")
                            status = result[1].decode("utf-8")
                            print(f"  New best: {claim_id},{status},{result[2]}", file=sys.stderr)

        if verbose:
            print("Pass 2: Complete.", file=sys.stderr)

        # Decode bytes to strings for final result
        if best_result is not None:
            return (
                best_result[0].decode("utf-8"),
                best_result[1].decode("utf-8"),
                best_result[2],
            )
        return None

    finally:
        # Cleanup temporary directory
        shutil.rmtree(tmp_dir, ignore_errors=True)


def main_solve(input_path: str, buckets: int = 1024, verbose: bool = False) -> None:
    """
    Main entry point that prints result to stdout.

    Args:
        input_path: Path to the input file.
        buckets: Number of buckets for partitioning.
        verbose: Print progress information to stderr.
    """
    result = solve(input_path, buckets=buckets, verbose=verbose)

    if result:
        claim_id, status_code, cycle_length = result
        print(f"{claim_id},{status_code},{cycle_length}")
    else:
        print(0)
