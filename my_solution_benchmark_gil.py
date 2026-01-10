#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = ["psutil"]
# ///
"""
Benchmark script comparing Free-threaded vs GIL-enforced execution.

Runs my_solution.py under both conditions with multiple trials,
measures wall-clock time and peak RSS (process tree), and reports statistical summary.

Uses psutil to track memory across the entire process tree (parent + all children),
which is essential for accurate measurement when ProcessPoolExecutor is used.
"""

import argparse
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
    print(
        "ERROR: psutil is required for process-tree memory benchmarking.",
        file=sys.stderr,
    )
    print("Install with: pip install psutil", file=sys.stderr)
    print("Or if using uv: uv pip install psutil", file=sys.stderr)
    sys.exit(1)


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
) -> dict:
    """
    Run my_solution.py and capture timing and memory metrics.

    Uses subprocess.Popen to allow memory sampling during execution.
    Tracks peak RSS across the entire process tree using psutil.

    Args:
        script_path: Path to my_solution.py.
        input_file: Path to input data file.
        env_overrides: Environment variable overrides (e.g., {"PYTHON_GIL": "1"}).
        use_timev: If True, wrap with /usr/bin/time -v for wall-clock timing.
        mem_sample_ms: Memory sampling interval in milliseconds.

    Returns:
        Dict with keys: mode, seconds, peak_rss_tree_mib, timev_rss_mib, output.
    """
    env = os.environ.copy()
    env.update(env_overrides)

    mode = "GIL-enforced" if env_overrides.get("PYTHON_GIL") == "1" else "Free-threaded"
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
        print(f"Error running benchmark ({mode}):", file=sys.stderr)
        print(stderr, file=sys.stderr)
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
        "mode": mode,
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
        print(f"Synthetic file already exists: {output_path}", file=sys.stderr)
        return

    generator_path = Path(__file__).parent / "generate_synthetic_cycles.py"
    if not generator_path.exists():
        print(f"Error: Generator script not found: {generator_path}", file=sys.stderr)
        sys.exit(1)

    print(f"Generating synthetic dataset: {output_path}", file=sys.stderr)
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
        print("Error: Failed to generate synthetic dataset", file=sys.stderr)
        sys.exit(1)
    print(file=sys.stderr)


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
    args = parser.parse_args()

    input_file = args.input_file
    num_trials = args.trials
    num_warmup = args.warmup
    mem_sample_ms = args.mem_sample_ms

    # Generate synthetic data if requested and file doesn't exist
    if args.synthetic and not Path(input_file).exists():
        generate_synthetic_if_needed(
            input_file,
            groups=args.synthetic_groups,
            nodes=args.synthetic_nodes,
            out_degree=args.synthetic_out_degree,
        )

    if not Path(input_file).exists():
        print(f"Error: Input file not found: {input_file}", file=sys.stderr)
        if not args.synthetic:
            print("  Hint: Use --synthetic to auto-generate test data", file=sys.stderr)
        sys.exit(1)

    script_path = Path(__file__).parent / "my_solution.py"
    if not script_path.exists():
        print(f"Error: Solution script not found: {script_path}", file=sys.stderr)
        sys.exit(1)

    # Check for /usr/bin/time
    use_timev = check_timev_available()

    # Environment configs
    env_free = {}  # Free-threaded: default (no PYTHON_GIL override)
    env_gil = {"PYTHON_GIL": "1"}  # GIL-enforced

    print("=" * 80)
    print("GIL Benchmark: Free-threaded vs GIL-enforced")
    print("=" * 80)
    print(f"Input: {input_file}")
    print(f"Trials: {num_trials} | Warm-up runs: {num_warmup}")
    print(f"Memory sampling: {mem_sample_ms}ms interval (process-tree RSS via psutil)")
    print(f"Python: {sys.executable}")
    print(f"Wall-clock timing: {'GNU time' if use_timev else 'time.perf_counter()'}")
    print()

    # Warm-up runs (not counted)
    print(f"Warming up ({num_warmup} run(s) per mode, not counted)...")
    for _ in range(num_warmup):
        run_benchmark(script_path, input_file, env_free, use_timev, mem_sample_ms)
        run_benchmark(script_path, input_file, env_gil, use_timev, mem_sample_ms)
    print("  Warm-up complete.")
    print()

    # Timed trials with alternating order
    results_free: list[dict] = []
    results_gil: list[dict] = []

    print(f"Running {num_trials} trials (alternating order to reduce bias)...")
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
        print(
            f"  Trial {trial}/{num_trials}: "
            f"Free={r1['seconds']:.2f}s/{r1['peak_rss_tree_mib']:.0f}MiB, "
            f"GIL={r2['seconds']:.2f}s/{r2['peak_rss_tree_mib']:.0f}MiB"
        )

    print()

    # Compute statistics
    def compute_stats(results: list[dict]) -> dict:
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

    stats_free = compute_stats(results_free)
    stats_gil = compute_stats(results_gil)

    # Verify outputs match
    if stats_free["output"] != stats_gil["output"]:
        print("WARNING: Outputs differ between modes!", file=sys.stderr)
        print(f"  Free-threaded: {stats_free['output']}", file=sys.stderr)
        print(f"  GIL-enforced:  {stats_gil['output']}", file=sys.stderr)
        print()

    # Calculate speedup
    if stats_free["median_time"] > 0:
        speedup = stats_gil["median_time"] / stats_free["median_time"]
    else:
        speedup = float("inf")

    # Print results table
    print("=" * 80)
    print("RESULTS")
    print("=" * 80)

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
    print(header)
    print("-" * 80)

    # Free-threaded row
    row_free = (
        f"{'Free-threaded':<15} {stats_free['median_time']:<11.3f} "
        f"{stats_free['min_time']:<9.3f} {stats_free['max_time']:<9.3f} "
        f"{stats_free['median_rss_tree']:<14.1f}"
    )
    if has_timev_rss:
        row_free += f" {stats_free['median_rss_timev']:<11.1f}"
    row_free += f" {stats_free['output']:<20}"
    print(row_free)

    # GIL-enforced row
    row_gil = (
        f"{'GIL-enforced':<15} {stats_gil['median_time']:<11.3f} "
        f"{stats_gil['min_time']:<9.3f} {stats_gil['max_time']:<9.3f} "
        f"{stats_gil['median_rss_tree']:<14.1f}"
    )
    if has_timev_rss:
        row_gil += f" {stats_gil['median_rss_timev']:<11.1f}"
    row_gil += f" {stats_gil['output']:<20}"
    print(row_gil)

    print("-" * 80)
    print()
    print("Peak RSS Tree = sum of RSS across parent + all child processes (via psutil)")
    if has_timev_rss:
        print("time-v RSS    = parent process only (from /usr/bin/time -v)")
    print()
    print(f"Speedup (GIL-enforced / Free-threaded): {speedup:.2f}x")
    if speedup > 1:
        print(f"  -> Free-threading is {speedup:.2f}x faster")
    elif speedup < 1:
        print(f"  -> GIL-enforced is {1/speedup:.2f}x faster")
    else:
        print("  -> No difference")
    print("=" * 80)


if __name__ == "__main__":
    main()
