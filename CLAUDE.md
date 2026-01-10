# CLAUDE.md - Project Guidelines & Operating Manual

## 1. Project Overview
**Goal:** Create a "Routing Cycle Detector" CLI tool to find the longest cycle in a large, newline-delimited dataset of claims.
**Deliverable:** A single standalone Python script (`my_solution.py`) that acts as a self-contained executable.
**Input Handling:** The tool must accept a local file path (e.g., `data/large_input_v1.txt`).
**Output:** The script must print exactly one line to STDOUT: `<claim_id>,<status_code>,<cycle_length>`. (Or `0` if no cycles found).

## 2. Data & Logic Specifications (CRITICAL)
- **Input Format:** `Source|Destination|ClaimID|StatusCode`
- **Delimiter:** Pipe character (`|`).
- **Data Constraints:**
  - File may contain **millions** of distinct `ClaimID`s.
  - Strategy: **2-Pass Partitioning** (Stream to disk buckets -> Parallel Process).
- **Cycle Definition:**
  - A sequence of hops where `ClaimID` and `StatusCode` are identical for every hop.
  - Must form a closed loop.
  - **Optimization:** Use O(N) detection for functional graphs (max out-degree ≤ 1) and DFS for general graphs.

## 3. Technical Stack
- **Python:** - **Target:** 3.14t (Free-threaded) for maximum performance.
  - **Minimum:** 3.10+ (Supported via Multiprocessing fallback).
- **Package Manager:** `uv` (for script management and execution).
- **Linting:** `ruff` (enforce PEP 8).
- **Architecture:** - Develop source code in `src/` modules (`partition.py`, `graph.py`, `scheduler.py`).
  - Use `build.py` to concat modules into `my_solution.py` and inject UV metadata.

## 4. Workflow Rules
- **Branching:** Create feature branches (`feature/name`) for all changes.
- **Verification:** - Use `pytest` for logic verification.
  - Run `ruff check .` before committing.
- **Pull Requests:** Raise PRs via GitHub MCP upon feature completion.
- **Diagrams:** Generate Mermaid diagrams only at the project completion phase.

## 5. Coding Standards & Implementation Details
### Standalone Script Requirements (PEP 723)
The final `my_solution.py` and `my_solution_benchmark_gil.py` must be executable standalone scripts.
- **Shebang:** Must start with `#!/usr/bin/env -S uv run --script`.
- **Metadata:** Must include a TOML `/// script` block declaring:
  - `requires-python = ">=3.10"`
  - `dependencies = []` (Standard library only).
- **No Sidecars:** Do not generate or rely on `.lock` files.

### Benchmarking Strategy
- Create a separate script `my_solution_benchmark_gil.py`.
- It must orchestrate the comparison between the free-threaded run and a forced GIL run (using `PYTHON_GIL=1` environment variable).

### GIL & Concurrency (Hybrid Model)
- **Auto-Detection:** The solution must check `sys._is_gil_enabled()` (safely handling older versions).
- **Executor Selection:**
  - **GIL Disabled (Free-threading):** Use `ThreadPoolExecutor`. (Preferred).
  - **GIL Enabled (Legacy/Standard):** Use `ProcessPoolExecutor`. (Fallback).
- **Data Safety:** Use "Shared-Nothing" architecture (Partitioning) to avoid locks entirely.

## 7. Command Map
- **Run Solution:** `./my_solution.py data/large_input_v1.txt`
- **Run Benchmark:** `./my_solution_benchmark_gil.py data/large_input_v1.txt`
- **Run Tests:** `uv run pytest`
- **Build Single File:** `uv run build.py`