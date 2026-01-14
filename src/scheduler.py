import logging
import os
import shutil
import sys
import tempfile
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from pathlib import Path

from src.graph import process_bucket
from src.partition import PartitionStats, partition_to_buckets

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
    1. RC_EXECUTOR env var override ("threads", "processes", or "serial")
    2. Auto-select based on GIL status (disabled -> threads, enabled -> processes)

    "serial" mode runs in the main thread - useful for debugging with breakpoints.
    """
    executor_override = os.environ.get(RC_EXECUTOR_ENV, "").lower()

    if executor_override == "threads":
        return ThreadPoolExecutor
    elif executor_override == "processes":
        return ProcessPoolExecutor
    elif executor_override == "serial":
        return None  # Signal to use serial execution
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
    total_start = time.perf_counter()
    input_file = Path(input_path)
    input_path = str(input_file.resolve())

    # Validate buckets is power of 2
    if buckets & (buckets - 1) != 0:
        raise ValueError(f"buckets must be a power of 2, got {buckets}")

    # Select executor based on GIL status
    ExecutorClass = _get_executor_class()
    use_process_pool = ExecutorClass is ProcessPoolExecutor
    use_serial = ExecutorClass is None

    gil_status = "enabled" if _is_gil_enabled() else "disabled"
    executor_name = "serial" if use_serial else ("threads" if ExecutorClass is ThreadPoolExecutor else "processes")
    workers_desc = "auto" if workers is None else str(workers)
    executor_override = os.environ.get(RC_EXECUTOR_ENV, "")
    override_info = f", RC_EXECUTOR={executor_override}" if executor_override else ""

    logger.info(
        f"Starting: file={input_file.name}, buckets={buckets}, workers={workers_desc}, "
        f"executor={executor_name}, GIL={gil_status}{override_info}"
    )

    # Create temporary directory for buckets
    tmp_dir = tempfile.mkdtemp(prefix="routing_cycles_")

    try:
        # Pass 1: Partition to buckets
        t1_start = time.perf_counter()

        bucket_paths, stats = partition_to_buckets(input_path, buckets, tmp_dir)

        t1_end = time.perf_counter()
        t1 = t1_end - t1_start

        if stats.malformed_lines > 0:
            logger.warning(
                "Pass 1: %d malformed lines skipped (read=%d, written=%d)",
                stats.malformed_lines, stats.lines_read, stats.lines_written
            )

        logger.info(f"Pass 1 done: {len(bucket_paths)} non-empty buckets in {t1:.2f}s")

        if not bucket_paths:
            total_time = time.perf_counter() - total_start
            logger.info(f"Result: No cycles found (total {total_time:.2f}s)")
            return None

        # Pass 2: Process buckets in parallel using executor.map
        t2_start = time.perf_counter()

        # Convert Path objects to strings for pickling (ProcessPoolExecutor)
        bucket_path_strs = [str(p) for p in bucket_paths]

        best_result: tuple[bytes, bytes, int] | None = None

        def process_results(results):
            """Process results iterator and track best result."""
            nonlocal best_result
            for result in results:
                if result is not None:
                    if best_result is None or result[2] > best_result[2]:
                        best_result = result
                        claim_id = result[0].decode("utf-8")
                        status = result[1].decode("utf-8")
                        logger.debug("New best: %s,%s,%d", claim_id, status, result[2])

        if use_serial:
            results = (process_bucket(p) for p in bucket_path_strs)
            process_results(results)
        else:
            with ExecutorClass(max_workers=workers) as executor:
                if use_process_pool:
                    results = executor.map(
                        process_bucket,
                        bucket_path_strs,
                        chunksize=PROCESS_POOL_CHUNKSIZE,
                    )
                else:
                    results = executor.map(process_bucket, bucket_path_strs)

                process_results(results)

        t2_end = time.perf_counter()
        t2 = t2_end - t2_start

        logger.info(f"Pass 2 done: {len(bucket_paths)} buckets processed in {t2:.2f}s")

        total_passes = t1 + t2
        if total_passes > 0:
            logger.debug(
                "Timing breakdown: Pass1=%.2fs (%.0f%%), Pass2=%.2fs (%.0f%%)",
                t1, 100 * t1 / total_passes, t2, 100 * t2 / total_passes
            )

        total_time = time.perf_counter() - total_start

        if best_result is not None:
            claim_id = best_result[0].decode("utf-8")
            status = best_result[1].decode("utf-8")
            cycle_len = best_result[2]
            logger.info(f"Result: cycle length {cycle_len} (total {total_time:.2f}s)")
            return (claim_id, status, cycle_len)
        else:
            logger.info(f"Result: No cycles found (total {total_time:.2f}s)")
            return None

    finally:
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
