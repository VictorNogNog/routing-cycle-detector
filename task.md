# Routing Cycle Detector

## Overview

This challenge should take less than 1 hour, you can access almost any tool/LLM needed.

You are given a large, newline-delimited text file.
Each line represents a claim being routed between internal systems:

```
<source_system>|<destination_system>|<claim_id>|<status_code>
```

Your task is to find the **longest routing cycle**, defined as a sequence of hops where:

- All hops share the same `claim_id`
- All hops share the same `status_code`
- The hops form a closed loop (returning to the original system)

### Example cycle:

```
Epic → Availity
Availity → Optum
Optum → Epic
```

(with all hops having the same `claim_id` and `status_code`)

Cycles may have length 2 or greater.

## Your Task

Write a command-line program that:

1. Streams/loads the input file (the file will be large; do not load it fully into memory)
2. Detects all cycles of the form described above
3. Finds the single longest cycle, measured in number of hops
4. Prints one line to STDOUT:

```
<claim_id>,<status_code>,<cycle_length>
```

If multiple cycles tie for longest length, any one is acceptable.

**Total runtime target:** ≤ 90 minutes.

## Constraints

### Size Constraints

- You may store limited per-claim state, but should assume the file may contain millions of distinct `claim_id` values.

### Cycle Constraints

- Cycles must follow directional edges.
- Cycles must be simple (no repeated nodes except start/end).
- Self-loops (A → A) count as cycle length = 1, but will never win.

### Input Guarantees

- The file is valid UTF-8.
- The file is newline-delimited.
- Fields have no additional whitespace.
- There is no header row.

## Deliverables

### Solution Python Script

Create a public repository on GitHub with a script (`my_solution.py`), that can be called with the filepath of the data set (available at `https://drive.google.com/file/d/10WF0EwKH7pac1Pxp3BmRwC_1B1Lxuix0/view?usp=sharing`).

For example:

```
python3 my_solution.py large_input_v1.txt
```

This command should print out the solution, for example:

```
python3 my_solution.py <url>
3,102,4
```

### Explanation File

Include a text file (`explanation.txt`) with a short (< 400 characters) explanation of your strategy in creating your solution. Some interesting topics would be speed, memory utilization, complexity, pros/cons, make sure to include any future improvements you would make to your solution.

### Solution File

Include a solution file (`solution.txt`) with the output of your script when ran on the large input, for example, the contents might be:

```
3,102,4
```

## Allowed Tools

### You may use:

- Python
- LLM-assisted editors (Cursor, etc.)
- Standard libraries

### Do not use:

- Distributed compute frameworks
- External databases
- Off-the-shelf cycle-detection libraries

## Example

### Input

```
Epic|Availity|123|197
Availity|Optum|123|197
Optum|Epic|123|197
Epic|Availity|891|45
Availity|Epic|891|45
```

### Output

```
123,197,3
```