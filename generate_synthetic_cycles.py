#!/usr/bin/env python3
"""
Synthetic dataset generator for routing cycle detection benchmarks.

Generates a large newline-delimited file with many (claim_id, status) groups,
each containing a directed graph with multiple cycles. This exercises the
"general DFS" path in the cycle detector.

WARNING: Large values of --nodes or --out-degree can cause cycle enumeration
to become extremely expensive. Recommended defaults: nodes=32, out-degree=2.
"""

import argparse
import random
import sys

# Large buffer for efficient streaming writes
BUFFER_SIZE = 1024 * 1024  # 1MB


def generate_group_edges(
    group_id: int,
    nodes: int,
    out_degree: int,
    chord_mode: str,
    rng: random.Random,
) -> list[tuple[int, int]]:
    """
    Generate edges for a single group's graph.

    Creates a base cycle of length `nodes`, plus additional chord edges
    to create many simple cycles.

    Args:
        group_id: The group/claim ID (for deterministic seeding).
        nodes: Number of nodes in the graph.
        out_degree: Number of outgoing edges per node.
        chord_mode: "fixed" or "random" chord selection.
        rng: Random number generator (used only for random mode).

    Returns:
        List of (src_idx, dst_idx) edge tuples.
    """
    edges = []

    for i in range(nodes):
        # Base cycle edge: i -> (i+1) % nodes
        edges.append((i, (i + 1) % nodes))

        # Additional chord edges to create more cycles
        if chord_mode == "fixed":
            # Fixed steps: i -> (i+2), i -> (i+3), etc.
            for step in range(2, out_degree + 1):
                dst = (i + step) % nodes
                edges.append((i, dst))
        else:
            # Random mode: pick distinct steps from [2..nodes-1]
            # Seed RNG with (base_seed, group_id) for determinism
            available_steps = list(range(2, nodes))
            rng.shuffle(available_steps)
            num_chords = min(out_degree - 1, len(available_steps))
            for j in range(num_chords):
                dst = (i + available_steps[j]) % nodes
                edges.append((i, dst))

    return edges


def generate_synthetic_dataset(
    output_path: str,
    num_groups: int,
    nodes: int,
    out_degree: int,
    status_code: int,
    chord_mode: str,
    seed: int,
) -> int:
    """
    Generate a synthetic dataset with many cycles.

    Streams output line-by-line to avoid memory issues.

    Args:
        output_path: Path to output file.
        num_groups: Number of (claim_id, status) groups.
        nodes: Number of nodes per group.
        out_degree: Out-degree per node (>= 1).
        status_code: Status code to use for all lines.
        chord_mode: "fixed" or "random".
        seed: Random seed for reproducibility.

    Returns:
        Total number of lines written.
    """
    rng = random.Random(seed)
    total_lines = 0
    status_str = str(status_code)

    with open(output_path, "w", encoding="utf-8", buffering=BUFFER_SIZE) as f:
        for g in range(num_groups):
            claim_id = str(g)

            # Generate node names for this group
            node_names = [f"S{g:06d}_{i:02d}" for i in range(nodes)]

            # Seed RNG for this group (deterministic per-group randomness)
            if chord_mode == "random":
                rng.seed((seed, g))

            # Generate edges
            edges = generate_group_edges(g, nodes, out_degree, chord_mode, rng)

            # Write edges
            for src_idx, dst_idx in edges:
                src = node_names[src_idx]
                dst = node_names[dst_idx]
                f.write(f"{src}|{dst}|{claim_id}|{status_str}\n")
                total_lines += 1

            # Progress indicator every 10000 groups
            if (g + 1) % 10000 == 0:
                print(f"  Generated {g + 1}/{num_groups} groups...", file=sys.stderr)

    return total_lines


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate synthetic routing cycle dataset.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate ~11M lines (similar to real dataset)
  python generate_synthetic_cycles.py --out data/synthetic.txt --groups 175000

  # Generate heavier CPU load (more cycles per group)
  python generate_synthetic_cycles.py --out data/synthetic_heavy.txt --groups 175000 --out-degree 3

  # Generate with random chord patterns
  python generate_synthetic_cycles.py --out data/synthetic_random.txt --groups 175000 --chord-mode random
""",
    )

    parser.add_argument(
        "--out",
        required=True,
        help="Output file path",
    )
    parser.add_argument(
        "--groups",
        type=int,
        default=175000,
        help="Number of (claim_id, status) groups (default: 175000)",
    )
    parser.add_argument(
        "--nodes",
        type=int,
        default=32,
        help="Number of nodes per group (default: 32). WARNING: Large values expensive!",
    )
    parser.add_argument(
        "--out-degree",
        type=int,
        default=2,
        help="Out-degree per node (default: 2). WARNING: Values > 3 can be very slow!",
    )
    parser.add_argument(
        "--status",
        type=int,
        default=190310,
        help="Status code for all lines (default: 190310)",
    )
    parser.add_argument(
        "--chord-mode",
        choices=["fixed", "random"],
        default="fixed",
        help="Chord edge selection mode (default: fixed)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=1,
        help="Random seed for reproducibility (default: 1)",
    )

    args = parser.parse_args()

    # Validate
    if args.nodes < 3:
        parser.error("--nodes must be at least 3")
    if args.out_degree < 1:
        parser.error("--out-degree must be at least 1")
    if args.out_degree >= args.nodes:
        parser.error("--out-degree must be less than --nodes")

    # Estimate output size
    edges_per_group = args.nodes * args.out_degree
    total_edges = args.groups * edges_per_group
    # Approximate line length: ~35 chars
    approx_size_mb = (total_edges * 35) / (1024 * 1024)

    print("=" * 60, file=sys.stderr)
    print("Synthetic Cycle Dataset Generator", file=sys.stderr)
    print("=" * 60, file=sys.stderr)
    print(f"Output: {args.out}", file=sys.stderr)
    print(f"Groups: {args.groups:,}", file=sys.stderr)
    print(f"Nodes per group: {args.nodes}", file=sys.stderr)
    print(f"Out-degree: {args.out_degree}", file=sys.stderr)
    print(f"Chord mode: {args.chord_mode}", file=sys.stderr)
    print(f"Status code: {args.status}", file=sys.stderr)
    print(f"Seed: {args.seed}", file=sys.stderr)
    print(f"Estimated lines: {total_edges:,}", file=sys.stderr)
    print(f"Estimated size: ~{approx_size_mb:.1f} MB", file=sys.stderr)
    print("=" * 60, file=sys.stderr)
    print(file=sys.stderr)

    # Generate
    print("Generating...", file=sys.stderr)
    total_lines = generate_synthetic_dataset(
        output_path=args.out,
        num_groups=args.groups,
        nodes=args.nodes,
        out_degree=args.out_degree,
        status_code=args.status,
        chord_mode=args.chord_mode,
        seed=args.seed,
    )

    print(file=sys.stderr)
    print("=" * 60, file=sys.stderr)
    print(f"Done! Wrote {total_lines:,} lines to {args.out}", file=sys.stderr)
    print("=" * 60, file=sys.stderr)


if __name__ == "__main__":
    main()
