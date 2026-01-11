#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = ["psutil"]
# ///
"""
Benchmark script comparing Free-threaded vs GIL-enforced execution.

Runs my_solution.py under various conditions with multiple trials,
measures wall-clock time and peak RSS (process tree), and reports statistical summary.

Supports:
- 2-mode comparison: Free-threaded vs GIL-enforced (with optional executor override)
- 4-mode matrix (--all-modes): threads×{GIL-off, GIL-on} + processes×{GIL-off, GIL-on}

Uses psutil to track memory across the entire process tree (parent + all children),
which is essential for accurate measurement when ProcessPoolExecutor is used.
"""

import argparse
import logging
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path
from statistics import median

# Check for psutil early
try:
    import psutil
except ImportError:
    sys.stderr.write("ERROR: psutil is required for process-tree memory benchmarking.\n")
    sys.stderr.write("Install with: pip install psutil\n")
    sys.stderr.write("Or if using uv: uv pip install psutil\n")
    sys.exit(1)

logger = logging.getLogger(__name__)


def parse_elapsed_to_seconds(elapsed_str: str) -> float:
    """
    Parse elapsed time string from /usr/bin/time -v.

    Formats:
      - "m:ss.xx" (e.g., "1:23.45")
      - "h:mm:ss" (e.g., "1:02:03")

    Returns:
        Elapsed time in seconds.
    """
    elapsed_str = elapsed_str.strip()
    parts = elapsed_str.split(":")

    if len(parts) == 2:
        # m:ss.xx format
        minutes = int(parts[0])
        seconds = float(parts[1])
        return minutes * 60 + seconds
    elif len(parts) == 3:
        # h:mm:ss format
        hours = int(parts[0])
        minutes = int(parts[1])
        seconds = float(parts[2])
        return hours * 3600 + minutes * 60 + seconds
    else:
        raise ValueError(f"Cannot parse elapsed time: {elapsed_str}")


def parse_timev_output(stderr: str) -> tuple[float | None, float | None]:
    """
    Parse /usr/bin/time -v output from stderr.

    Returns:
        Tuple of (seconds, rss_kbytes) or (None, None) if not found.
    """
    seconds = None
    rss_kb = None

    # Match elapsed time: "Elapsed (wall clock) time (h:mm:ss or m:ss): 0:15.82"
    elapsed_match = re.search(
        r"Elapsed \(wall clock\) time \([^)]+\):\s*(\S+)", stderr
    )
    if elapsed_match:
        try:
            seconds = parse_elapsed_to_seconds(elapsed_match.group(1))
        except ValueError:
            pass

    # Match RSS: "Maximum resident set size (kbytes): 123456"
    rss_match = re.search(
        r"Maximum resident set size \(kbytes\):\s*(\d+)", stderr
    )
    if rss_match:
        rss_kb = int(rss_match.group(1))

    return seconds, rss_kb


def measure_peak_rss_tree(
    root_pid: int,
    poll_interval_s: float,
    proc: subprocess.Popen,
) -> int:
    """
    Measure peak RSS across the entire process tree while process runs.

    Samples the RSS of the root process and all its descendants recursively,
    summing them to get total memory usage. Tracks the peak value.

    Args:
        root_pid: PID of the root process to monitor.
        poll_interval_s: Sampling interval in seconds.
        proc: The Popen object to poll for completion.

    Returns:
        Peak total RSS in bytes across the process tree.
    """
    peak_bytes = 0

    try:
        root_proc = psutil.Process(root_pid)
    except psutil.NoSuchProcess:
        return 0

    while proc.poll() is None:
        total_rss = 0

        try:
            # Get root process RSS
            total_rss += root_proc.memory_info().rss
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass

        try:
            # Get all descendant processes recursively
            children = root_proc.children(recursive=True)
            for child in children:
                try:
                    total_rss += child.memory_info().rss
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass

        peak_bytes = max(peak_bytes, total_rss)
        time.sleep(poll_interval_s)

    # Final sample after process exits (catch any late peak)
    try:
        total_rss = 0
        try:
            total_rss += root_proc.memory_info().rss
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass

        try:
            children = root_proc.children(recursive=True)
            for child in children:
                try:
                    total_rss += child.memory_info().rss
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass

        peak_bytes = max(peak_bytes, total_rss)
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        pass

    return peak_bytes


def run_benchmark(
    script_path: Path,
    input_file: str,
    env_overrides: dict[str, str],
    use_timev: bool,
    mem_sample_ms: int,
    mode_label: str = "",
) -> dict:
    """
    Run my_solution.py and capture timing and memory metrics.

    Uses subprocess.Popen to allow memory sampling during execution.
    Tracks peak RSS across the entire process tree using psutil.

    Args:
        script_path: Path to my_solution.py.
        input_file: Path to input data file.
        env_overrides: Environment variable overrides.
        use_timev: If True, wrap with /usr/bin/time -v for wall-clock timing.
        mem_sample_ms: Memory sampling interval in milliseconds.
        mode_label: Optional label for the mode (for error messages).

    Returns:
        Dict with keys: mode, seconds, peak_rss_tree_mib, timev_rss_mib, output.
    """
    env = os.environ.copy()
    env.update(env_overrides)

    if not mode_label:
        mode_label = "GIL-enforced" if env_overrides.get("PYTHON_GIL") == "1" else "Free-threaded"
    poll_interval_s = mem_sample_ms / 1000.0

    # Build command
    if use_timev:
        cmd = ["/usr/bin/time", "-v", sys.executable, str(script_path), input_file]
    else:
        cmd = [sys.executable, str(script_path), input_file]

    # Start process
    start_time = time.perf_counter()
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )

    # Measure peak RSS across process tree while running
    peak_rss_bytes = measure_peak_rss_tree(proc.pid, poll_interval_s, proc)

    # Get output after completion
    stdout, stderr = proc.communicate()
    elapsed_perf = time.perf_counter() - start_time

    if proc.returncode != 0:
        logger.error("Error running benchmark (%s):", mode_label)
        logger.error("%s", stderr)
        sys.exit(1)

    output = stdout.strip()

    # Parse timing and optional time-v RSS
    if use_timev:
        timev_seconds, timev_rss_kb = parse_timev_output(stderr)
        seconds = timev_seconds if timev_seconds is not None else elapsed_perf
        timev_rss_mib = timev_rss_kb / 1024.0 if timev_rss_kb is not None else float("nan")
    else:
        seconds = elapsed_perf
        timev_rss_mib = float("nan")

    # Convert peak RSS to MiB
    peak_rss_tree_mib = peak_rss_bytes / (1024 * 1024)

    return {
        "mode": mode_label,
        "seconds": seconds,
        "peak_rss_tree_mib": peak_rss_tree_mib,
        "timev_rss_mib": timev_rss_mib,
        "output": output,
    }


def check_timev_available() -> bool:
    """Check if /usr/bin/time is available."""
    return shutil.which("/usr/bin/time") is not None


def generate_synthetic_if_needed(
    output_path: str,
    groups: int = 175000,
    nodes: int = 32,
    out_degree: int = 2,
) -> None:
    """Generate synthetic dataset if it doesn't exist."""
    if Path(output_path).exists():
        logger.info("Synthetic file already exists: %s", output_path)
        return

    generator_path = Path(__file__).parent / "generate_synthetic_cycles.py"
    if not generator_path.exists():
        logger.error("Generator script not found: %s", generator_path)
        sys.exit(1)

    logger.info("Generating synthetic dataset: %s", output_path)
    cmd = [
        sys.executable,
        str(generator_path),
        "--out", output_path,
        "--groups", str(groups),
        "--nodes", str(nodes),
        "--out-degree", str(out_degree),
    ]
    result = subprocess.run(cmd)
    if result.returncode != 0:
        logger.error("Failed to generate synthetic dataset")
        sys.exit(1)


def compute_stats(results: list[dict]) -> dict:
    """Compute statistics from a list of benchmark results."""
    times = [r["seconds"] for r in results]
    rss_tree = [r["peak_rss_tree_mib"] for r in results]
    rss_timev = [r["timev_rss_mib"] for r in results if r["timev_rss_mib"] == r["timev_rss_mib"]]
    output = results[0]["output"] if results else ""

    return {
        "median_time": median(times),
        "min_time": min(times),
        "max_time": max(times),
        "median_rss_tree": median(rss_tree),
        "median_rss_timev": median(rss_timev) if rss_timev else float("nan"),
        "output": output,
    }


def run_two_mode_benchmark(
    script_path: Path,
    input_file: str,
    env_free: dict[str, str],
    env_gil: dict[str, str],
    use_timev: bool,
    mem_sample_ms: int,
    num_trials: int,
    num_warmup: int,
) -> None:
    """Run the standard 2-mode benchmark (Free-threaded vs GIL-enforced)."""
    # Warm-up runs (not counted)
    logger.info("Warming up (%d run(s) per mode, not counted)...", num_warmup)
    for _ in range(num_warmup):
        run_benchmark(script_path, input_file, env_free, use_timev, mem_sample_ms)
        run_benchmark(script_path, input_file, env_gil, use_timev, mem_sample_ms)
    logger.info("Warm-up complete.")
    logger.info("")

    # Timed trials with alternating order
    results_free: list[dict] = []
    results_gil: list[dict] = []

    logger.info("Running %d trials (alternating order to reduce bias)...", num_trials)
    for trial in range(1, num_trials + 1):
        if trial % 2 == 1:
            # Odd trial: free-threaded first
            r1 = run_benchmark(script_path, input_file, env_free, use_timev, mem_sample_ms)
            r2 = run_benchmark(script_path, input_file, env_gil, use_timev, mem_sample_ms)
        else:
            # Even trial: GIL-enforced first
            r2 = run_benchmark(script_path, input_file, env_gil, use_timev, mem_sample_ms)
            r1 = run_benchmark(script_path, input_file, env_free, use_timev, mem_sample_ms)

        results_free.append(r1)
        results_gil.append(r2)
        logger.info(
            "  Trial %d/%d: Free=%.2fs/%.0fMiB, GIL=%.2fs/%.0fMiB",
            trial, num_trials,
            r1['seconds'], r1['peak_rss_tree_mib'],
            r2['seconds'], r2['peak_rss_tree_mib']
        )

    logger.info("")

    stats_free = compute_stats(results_free)
    stats_gil = compute_stats(results_gil)

    # Verify outputs match
    if stats_free["output"] != stats_gil["output"]:
        logger.warning("Outputs differ between modes!")
        logger.warning("  Free-threaded: %s", stats_free['output'])
        logger.warning("  GIL-enforced:  %s", stats_gil['output'])

    # Calculate speedup
    if stats_free["median_time"] > 0:
        speedup = stats_gil["median_time"] / stats_free["median_time"]
    else:
        speedup = float("inf")

    # Print results table
    logger.info("=" * 80)
    logger.info("RESULTS")
    logger.info("=" * 80)

    # Determine if we have time-v RSS data
    has_timev_rss = stats_free["median_rss_timev"] == stats_free["median_rss_timev"]

    # Header
    header = (
        f"{'Mode':<15} {'Median(s)':<11} {'Min(s)':<9} {'Max(s)':<9} "
        f"{'Peak RSS Tree':<14}"
    )
    if has_timev_rss:
        header += f" {'time-v RSS':<11}"
    header += f" {'Output':<20}"
    logger.info(header)
    logger.info("-" * 80)

    # Free-threaded row
    row_free = (
        f"{'Free-threaded':<15} {stats_free['median_time']:<11.3f} "
        f"{stats_free['min_time']:<9.3f} {stats_free['max_time']:<9.3f} "
        f"{stats_free['median_rss_tree']:<14.1f}"
    )
    if has_timev_rss:
        row_free += f" {stats_free['median_rss_timev']:<11.1f}"
    row_free += f" {stats_free['output']:<20}"
    logger.info(row_free)

    # GIL-enforced row
    row_gil = (
        f"{'GIL-enforced':<15} {stats_gil['median_time']:<11.3f} "
        f"{stats_gil['min_time']:<9.3f} {stats_gil['max_time']:<9.3f} "
        f"{stats_gil['median_rss_tree']:<14.1f}"
    )
    if has_timev_rss:
        row_gil += f" {stats_gil['median_rss_timev']:<11.1f}"
    row_gil += f" {stats_gil['output']:<20}"
    logger.info(row_gil)

    logger.info("-" * 80)
    logger.info("")
    logger.info("Peak RSS Tree = sum of RSS across parent + all child processes (via psutil)")
    if has_timev_rss:
        logger.info("time-v RSS    = parent process only (from /usr/bin/time -v)")
    logger.info("")
    logger.info("Speedup (GIL-enforced / Free-threaded): %.2fx", speedup)
    if speedup > 1:
        logger.info("  -> Free-threading is %.2fx faster", speedup)
    elif speedup < 1:
        logger.info("  -> GIL-enforced is %.2fx faster", 1/speedup)
    else:
        logger.info("  -> No difference")
    logger.info("=" * 80)


def run_all_modes_benchmark(
    script_path: Path,
    input_file: str,
    use_timev: bool,
    mem_sample_ms: int,
    num_trials: int,
    num_warmup: int,
) -> None:
    """Run the full 2×2 matrix benchmark: (threads, processes) × (GIL-off, GIL-on)."""
    # Define all 4 configurations
    configs = [
        {"label": "threads+GIL-off", "executor": "threads", "gil": False},
        {"label": "threads+GIL-on", "executor": "threads", "gil": True},
        {"label": "procs+GIL-off", "executor": "processes", "gil": False},
        {"label": "procs+GIL-on", "executor": "processes", "gil": True},
    ]

    def make_env(cfg: dict) -> dict[str, str]:
        env: dict[str, str] = {"RC_EXECUTOR": cfg["executor"]}
        if cfg["gil"]:
            env["PYTHON_GIL"] = "1"
        return env

    # Warm-up runs (not counted)
    logger.info("Warming up (%d run(s) per mode, not counted)...", num_warmup)
    for _ in range(num_warmup):
        for cfg in configs:
            run_benchmark(
                script_path, input_file, make_env(cfg), use_timev, mem_sample_ms, cfg["label"]
            )
    logger.info("Warm-up complete.")
    logger.info("")

    # Results storage
    results: dict[str, list[dict]] = {cfg["label"]: [] for cfg in configs}

    # Timed trials with rotating order to reduce bias
    logger.info("Running %d trials (rotating order to reduce bias)...", num_trials)
    for trial in range(1, num_trials + 1):
        # Rotate order based on trial number
        rotation = (trial - 1) % len(configs)
        order = configs[rotation:] + configs[:rotation]

        trial_results = {}
        for cfg in order:
            r = run_benchmark(
                script_path, input_file, make_env(cfg), use_timev, mem_sample_ms, cfg["label"]
            )
            results[cfg["label"]].append(r)
            trial_results[cfg["label"]] = r

        # Print trial summary
        summary_parts = [
            f"{cfg['label']}={trial_results[cfg['label']]['seconds']:.1f}s"
            for cfg in configs
        ]
        logger.info("  Trial %d/%d: %s", trial, num_trials, ", ".join(summary_parts))

    logger.info("")

    # Compute stats for each configuration
    all_stats: dict[str, dict] = {}
    for cfg in configs:
        all_stats[cfg["label"]] = compute_stats(results[cfg["label"]])

    # Verify all outputs match
    outputs = [all_stats[cfg["label"]]["output"] for cfg in configs]
    if len(set(outputs)) > 1:
        logger.warning("Outputs differ between modes!")
        for cfg in configs:
            logger.warning("  %s: %s", cfg['label'], all_stats[cfg['label']]['output'])

    # Print results table
    logger.info("=" * 95)
    logger.info("RESULTS (2×2 Matrix: Executor × GIL)")
    logger.info("=" * 95)

    # Header
    logger.info(
        "%s %s %s %s %s %s %s",
        "Executor".ljust(12), "GIL".ljust(8), "Median(s)".ljust(11),
        "Min(s)".ljust(9), "Max(s)".ljust(9), "Peak RSS(MiB)".ljust(14), "Output".ljust(20)
    )
    logger.info("-" * 95)

    # Print rows
    for cfg in configs:
        stats = all_stats[cfg["label"]]
        executor_name = "threads" if cfg["executor"] == "threads" else "processes"
        gil_status = "on" if cfg["gil"] else "off"
        logger.info(
            "%s %s %s %s %s %s %s",
            executor_name.ljust(12), gil_status.ljust(8),
            f"{stats['median_time']:.3f}".ljust(11),
            f"{stats['min_time']:.3f}".ljust(9),
            f"{stats['max_time']:.3f}".ljust(9),
            f"{stats['median_rss_tree']:.1f}".ljust(14),
            stats['output'].ljust(20)
        )

    logger.info("-" * 95)
    logger.info("")

    # Compute and display speedups
    logger.info("SPEEDUPS (GIL-on / GIL-off ratio for each executor):")
    logger.info("")

    # Threads speedup
    threads_off = all_stats["threads+GIL-off"]["median_time"]
    threads_on = all_stats["threads+GIL-on"]["median_time"]
    if threads_off > 0:
        threads_speedup = threads_on / threads_off
        if threads_speedup > 1:
            logger.info("  Threads:   %.2fx (GIL-off is %.2fx faster)", threads_speedup, threads_speedup)
        elif threads_speedup < 1:
            logger.info("  Threads:   %.2fx (GIL-on is %.2fx faster)", threads_speedup, 1/threads_speedup)
        else:
            logger.info("  Threads:   %.2fx (no difference)", threads_speedup)

    # Processes speedup
    procs_off = all_stats["procs+GIL-off"]["median_time"]
    procs_on = all_stats["procs+GIL-on"]["median_time"]
    if procs_off > 0:
        procs_speedup = procs_on / procs_off
        if procs_speedup > 1:
            logger.info("  Processes: %.2fx (GIL-off is %.2fx faster)", procs_speedup, procs_speedup)
        elif procs_speedup < 1:
            logger.info("  Processes: %.2fx (GIL-on is %.2fx faster)", procs_speedup, 1/procs_speedup)
        else:
            logger.info("  Processes: %.2fx (no difference)", procs_speedup)

    logger.info("")

    # Memory comparison
    logger.info("MEMORY COMPARISON:")
    logger.info("  Threads GIL-off:   %.1f MiB", all_stats['threads+GIL-off']['median_rss_tree'])
    logger.info("  Threads GIL-on:    %.1f MiB", all_stats['threads+GIL-on']['median_rss_tree'])
    logger.info("  Processes GIL-off: %.1f MiB", all_stats['procs+GIL-off']['median_rss_tree'])
    logger.info("  Processes GIL-on:  %.1f MiB", all_stats['procs+GIL-on']['median_rss_tree'])

    logger.info("")
    logger.info("=" * 95)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Benchmark Free-threaded vs GIL-enforced Python execution."
    )
    parser.add_argument("input_file", help="Path to input data file")
    parser.add_argument(
        "--trials", type=int, default=7, help="Number of timed trials per mode (default: 7)"
    )
    parser.add_argument(
        "--warmup", type=int, default=1, help="Number of warm-up runs per mode (default: 1)"
    )
    parser.add_argument(
        "--mem-sample-ms",
        type=int,
        default=75,
        help="Memory sampling interval in milliseconds (default: 75)",
    )
    parser.add_argument(
        "--synthetic",
        action="store_true",
        help="Generate synthetic dataset if input_file doesn't exist",
    )
    parser.add_argument(
        "--synthetic-groups",
        type=int,
        default=175000,
        help="Number of groups for synthetic data (default: 175000)",
    )
    parser.add_argument(
        "--synthetic-nodes",
        type=int,
        default=32,
        help="Nodes per group for synthetic data (default: 32)",
    )
    parser.add_argument(
        "--synthetic-out-degree",
        type=int,
        default=2,
        help="Out-degree for synthetic data (default: 2)",
    )
    parser.add_argument(
        "--executor",
        choices=["auto", "threads", "processes"],
        default="auto",
        help="Force executor type: auto (GIL-based), threads, or processes (default: auto)",
    )
    parser.add_argument(
        "--all-modes",
        action="store_true",
        help="Run full 2×2 matrix: (threads, processes) × (GIL-off, GIL-on)",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level (default: INFO)",
    )
    args = parser.parse_args()

    # Configure logging (benchmark script uses INFO by default for progress/results)
    log_level = getattr(logging, args.log_level)
    logging.basicConfig(
        level=log_level,
        format="%(message)s",
        stream=sys.stderr,
    )

    input_file = args.input_file
    num_trials = args.trials
    num_warmup = args.warmup
    mem_sample_ms = args.mem_sample_ms
    executor_policy = args.executor
    all_modes = args.all_modes

    # Generate synthetic data if requested and file doesn't exist
    if args.synthetic and not Path(input_file).exists():
        generate_synthetic_if_needed(
            input_file,
            groups=args.synthetic_groups,
            nodes=args.synthetic_nodes,
            out_degree=args.synthetic_out_degree,
        )

    if not Path(input_file).exists():
        logger.error("Input file not found: %s", input_file)
        if not args.synthetic:
            logger.error("  Hint: Use --synthetic to auto-generate test data")
        sys.exit(1)

    script_path = Path(__file__).parent / "my_solution.py"
    if not script_path.exists():
        logger.error("Solution script not found: %s", script_path)
        sys.exit(1)

    # Check for /usr/bin/time
    use_timev = check_timev_available()

    # Print header
    logger.info("=" * 95)
    if all_modes:
        logger.info("GIL Benchmark: Full 2×2 Matrix (Executor × GIL)")
    else:
        logger.info("GIL Benchmark: Free-threaded vs GIL-enforced")
    logger.info("=" * 95)
    logger.info("Input: %s", input_file)
    logger.info("Trials: %d | Warm-up runs: %d", num_trials, num_warmup)
    if all_modes:
        logger.info("Mode: All combinations (threads×GIL-off, threads×GIL-on, procs×GIL-off, procs×GIL-on)")
    elif executor_policy == "auto":
        logger.info("Executor: auto (threads if GIL disabled, processes if GIL enabled)")
    else:
        logger.info("Executor: %s (forced via RC_EXECUTOR)", executor_policy)
    logger.info("Memory sampling: %dms interval (process-tree RSS via psutil)", mem_sample_ms)
    logger.info("Python: %s", sys.executable)
    logger.info("Wall-clock timing: %s", "GNU time" if use_timev else "time.perf_counter()")
    logger.info("")

    if all_modes:
        # Run full 2×2 matrix
        run_all_modes_benchmark(
            script_path, input_file, use_timev, mem_sample_ms, num_trials, num_warmup
        )
    else:
        # Run standard 2-mode benchmark
        env_free: dict[str, str] = {}
        env_gil: dict[str, str] = {"PYTHON_GIL": "1"}

        if executor_policy != "auto":
            env_free["RC_EXECUTOR"] = executor_policy
            env_gil["RC_EXECUTOR"] = executor_policy

        run_two_mode_benchmark(
            script_path, input_file, env_free, env_gil, use_timev, mem_sample_ms,
            num_trials, num_warmup
        )


if __name__ == "__main__":
    main()
