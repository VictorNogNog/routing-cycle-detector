import logging
import os
import shutil
import sys
import tempfile
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from pathlib import Path

from .graph import process_bucket
from .partition import partition_to_buckets

logger = logging.getLogger(__name__)

# Each process recieves 16 buckets to process
PROCESS_POOL_CHUNKSIZE = 16

# Environment variable to override executor selection
RC_EXECUTOR_ENV = "RC_EXECUTOR"


def _is_gil_enabled() -> bool:
    """Check if GIL is enabled."""
    try:
        return sys._is_gil_enabled()
    except AttributeError:
        return True


def _get_executor_class():
    """
    Select the appropriate executor.

    Priority:
    1. RC_EXECUTOR env var override ("threads" or "processes")
    2. Auto-select based on GIL status (disabled -> threads, enabled -> processes)
    """
    executor_override = os.environ.get(RC_EXECUTOR_ENV, "").lower()

    if executor_override == "threads":
        return ThreadPoolExecutor
    elif executor_override == "processes":
        return ProcessPoolExecutor
    else:
        if _is_gil_enabled():
            return ProcessPoolExecutor
        else:
            return ThreadPoolExecutor


def solve(
    input_path: str,
    buckets: int = 1024,
    workers: int | None = None,
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

    gil_status = "enabled" if _is_gil_enabled() else "disabled"
    logger.debug("GIL status: %s", gil_status)
    logger.debug("Using executor: %s", ExecutorClass.__name__)

    # Create temporary directory for buckets
    tmp_dir = tempfile.mkdtemp(prefix="routing_cycles_")

    try:
        # Pass 1: Partition to buckets
        logger.debug("Pass 1: Partitioning to %d buckets...", buckets)

        bucket_paths = partition_to_buckets(input_path, buckets, tmp_dir)

        logger.debug("  Created %d non-empty buckets", len(bucket_paths))

        if not bucket_paths:
            return None

        # Pass 2: Process buckets in parallel using executor.map
        logger.debug("Pass 2: Processing buckets...")

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
                        claim_id = result[0].decode("utf-8")
                        status = result[1].decode("utf-8")
                        logger.debug("  New best: %s,%s,%d", claim_id, status, result[2])

        logger.debug("Pass 2: Complete.")

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


def main_solve(input_path: str, buckets: int = 1024) -> None:
    """
    Main entry point that prints result to stdout.

    Args:
        input_path: Path to the input file.
        buckets: Number of buckets for partitioning.
    """
    result = solve(input_path, buckets=buckets)

    if result:
        claim_id, status_code, cycle_length = result
        print(f"{claim_id},{status_code},{cycle_length}")
    else:
        print(0)
