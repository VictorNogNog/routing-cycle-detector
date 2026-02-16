import logging
import os
import shutil
import tempfile
import time
from pathlib import Path

from routing_cycle_detector.graph import process_bucket
from routing_cycle_detector.graph.types import BucketResult
from routing_cycle_detector.partition import partition_to_buckets
from routing_cycle_detector.solver.execution import (
    RC_EXECUTOR_ENV,
    describe_executor,
    get_executor_class,
    is_gil_enabled,
)

logger = logging.getLogger(__name__)

# Each process receives 16 buckets to process.
PROCESS_POOL_CHUNKSIZE = 16


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
    """
    total_start = time.perf_counter()
    input_file = Path(input_path)
    input_path = str(input_file.resolve())

    # Validate buckets is power of 2.
    if buckets & (buckets - 1) != 0:
        raise ValueError(f"buckets must be a power of 2, got {buckets}")

    # Select executor based on policy.
    executor_class = get_executor_class()
    executor_name = describe_executor(executor_class)
    use_process_pool = executor_name == "processes"

    gil_status = "enabled" if is_gil_enabled() else "disabled"
    workers_desc = "auto" if workers is None else str(workers)
    executor_override = os.environ.get(RC_EXECUTOR_ENV, "")
    override_info = f", RC_EXECUTOR={executor_override}" if executor_override else ""

    logger.info(
        f"Starting: file={input_file.name}, buckets={buckets}, workers={workers_desc}, "
        f"executor={executor_name}, GIL={gil_status}{override_info}"
    )

    # Create temporary directory for buckets.
    tmp_dir = tempfile.mkdtemp(prefix="routing_cycles_")

    try:
        # Pass 1: partition to buckets.
        t1_start = time.perf_counter()
        bucket_paths, stats = partition_to_buckets(input_path, buckets, tmp_dir)
        t1_end = time.perf_counter()
        t1 = t1_end - t1_start

        if stats.malformed_lines > 0:
            logger.warning(
                "Pass 1: %d malformed lines skipped (read=%d, written=%d)",
                stats.malformed_lines,
                stats.lines_read,
                stats.lines_written,
            )

        logger.info("Pass 1 done: %d non-empty buckets in %.2fs", len(bucket_paths), t1)

        if not bucket_paths:
            total_time = time.perf_counter() - total_start
            logger.info("Result: No cycles found (total %.2fs)", total_time)
            return None

        # Pass 2: process buckets in parallel using executor.map.
        t2_start = time.perf_counter()

        # Convert Path objects to strings for pickling (ProcessPoolExecutor).
        bucket_path_strs = [str(p) for p in bucket_paths]

        best_result: BucketResult | None = None

        def process_results(results) -> None:
            """Process results iterator and track best result."""
            nonlocal best_result
            for result in results:
                if result is not None and (
                    best_result is None or result.cycle_length > best_result.cycle_length
                ):
                    best_result = result
                    claim_id = result.claim_id.decode("utf-8")
                    status = result.status_code.decode("utf-8")
                    logger.debug("New best: %s,%s,%d", claim_id, status, result.cycle_length)

        if executor_class is None:
            results = (process_bucket(path) for path in bucket_path_strs)
            process_results(results)
        else:
            with executor_class(max_workers=workers) as executor:
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
        logger.info("Pass 2 done: %d buckets processed in %.2fs", len(bucket_paths), t2)

        total_passes = t1 + t2
        if total_passes > 0:
            logger.debug(
                "Timing breakdown: Pass1=%.2fs (%.0f%%), Pass2=%.2fs (%.0f%%)",
                t1,
                100 * t1 / total_passes,
                t2,
                100 * t2 / total_passes,
            )

        total_time = time.perf_counter() - total_start
        if best_result is None:
            logger.info("Result: No cycles found (total %.2fs)", total_time)
            return None

        claim_id = best_result.claim_id.decode("utf-8")
        status = best_result.status_code.decode("utf-8")
        cycle_len = best_result.cycle_length
        logger.info("Result: cycle length %d (total %.2fs)", cycle_len, total_time)
        return (claim_id, status, cycle_len)

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def main_solve(input_path: str, buckets: int = 1024) -> None:
    """Main entry point that prints result to stdout."""
    result = solve(input_path, buckets=buckets)

    if result:
        claim_id, status_code, cycle_length = result
        print(f"{claim_id},{status_code},{cycle_length}")
    else:
        print(0)
