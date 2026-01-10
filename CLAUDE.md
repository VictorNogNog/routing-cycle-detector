# CLAUDE.md - Project Guidelines & Operating Manual

## 1. Project Overview
**Goal:** Create a "Routing Cycle Detector" CLI tool to find the longest cycle in a large, newline-delimited dataset of claims.
**Deliverable:** A single standalone Python script (`my_solution.py`) that acts as a self-contained executable.
**Input Handling:** The tool must accept a local file path (e.g., `data/large_input_v1.txt`).
**Output:** The script must print exactly one line to STDOUT: `<claim_id>,<status_code>,<cycle_length>`.

## 2. Data & Logic Specifications (CRITICAL)
- **Input Format:** `Source|Destination|ClaimID|StatusCode`
- **Delimiter:** Pipe character (`|`).
- **Data Constraints:**
  - File may contain **millions** of distinct `ClaimID`s (must stream/shard).
  - No header row.
  - Fields have no additional whitespace.
- **Cycle Definition:**
  - A sequence of hops where `ClaimID` and `StatusCode` are identical for every hop.
  - Must form a closed loop (Start Node == End Node).
  - **Simple Cycles Only:** No repeated nodes except the start/end.
  - **Self-loops:** A self-loop (A → A) counts as length 1 but is unlikely to win.

## 3. Technical Stack
- **Python:** 3.14 (Free-threaded build required).
- **Package Manager:** `uv` (for script management and execution).
- **Linting:** `ruff` (enforce PEP 8).
- **Architecture:** - Develop source code in `src/` modules for separation of concerns.
  - Use a `build.py` script to concat `src/` modules into the final `my_solution.py` and inject the required UV metadata.

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
  - `requires-python = ">=3.14"`
  - `dependencies = []` (Add standard lib dependencies here if any external ones are absolutely needed).
- **No Sidecars:** Do not generate or rely on `.lock` files for the final deliverable.

### Benchmarking Strategy
- Create a separate script `my_solution_benchmark_gil.py`.
- It must orchestrate the comparison between the free-threaded run and a forced GIL run (using `PYTHON_GIL=1` environment variable).

### GIL & Concurrency
- **Concurrency Model:** Utilize Python 3.14's free-threading (no-GIL).
- **Runtime Checks:** `my_solution.py` must verify `sys._is_gil_enabled()` is False.

## 6. Documentation Rules (Memory Bank)
- **Prompt Logging:** If a user prompt results in a significant architectural decision (e.g., "Design the sharding algorithm") or complex code generation:
  1.  **Automatically summarize** the prompt and the decision rationale.
  2.  **Save it** to `.prompts/` with a sequential prefix (e.g., `01_sharding_strategy.md`).
- **Explanation File:** Maintain `explanation.txt` as a living document.

## 7. Command Map
- **Run Solution:** `./my_solution.py data/large_input_v1.txt`
- **Run Benchmark:** `./my_solution_benchmark_gil.py data/large_input_v1.txt`
- **Run Tests:** `uv run pytest`
- **Build Single File:** `uv run build.py`