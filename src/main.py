"""CLI entry point for the Routing Cycle Detector."""

import argparse
import sys

from .scheduler import main_solve


def create_parser() -> argparse.ArgumentParser:
    """Create and configure the argument parser."""
    parser = argparse.ArgumentParser(
        prog="routing-cycle-detector",
        description="Find the longest routing cycle in claim data.",
    )

    parser.add_argument(
        "input_file",
        help="Path to the input file (pipe-delimited: Source|Dest|ClaimID|Status)",
    )

    parser.add_argument(
        "--buckets",
        type=int,
        default=1024,
        help="Number of buckets for partitioning (power of 2, default: 1024)",
    )

    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Print progress information to stderr",
    )

    return parser


def main() -> None:
    """Main entry point."""
    parser = create_parser()
    args = parser.parse_args()

    # Validate buckets is power of 2
    if args.buckets & (args.buckets - 1) != 0:
        parser.error(f"--buckets must be a power of 2, got {args.buckets}")

    main_solve(
        input_path=args.input_file,
        buckets=args.buckets,
        verbose=args.verbose,
    )


if __name__ == "__main__":
    main()
