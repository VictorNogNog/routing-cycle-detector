#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = []
# ///
"""
Routing Cycle Detector - Find the longest directed cycle in routing claim data.

This is the main entry point for the solution. It uses the modules in src/ for
partitioning, graph processing, and parallel scheduling.
"""

import argparse
import logging
import sys

from src.scheduler import main_solve


def configure_logging(level: int = logging.INFO) -> None:
    """Configure logging to write to stderr."""
    logging.basicConfig(
        level=level,
        format="%(levelname)s: %(message)s",
        stream=sys.stderr,
    )


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
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level (default: INFO)",
    )

    return parser


def main() -> None:
    """Main entry point."""
    parser = create_parser()
    args = parser.parse_args()

    # Configure logging based on --log-level
    log_level = getattr(logging, args.log_level)
    configure_logging(log_level)

    # Validate buckets is power of 2
    if args.buckets & (args.buckets - 1) != 0:
        parser.error(f"--buckets must be a power of 2, got {args.buckets}")

    main_solve(
        input_path=args.input_file,
        buckets=args.buckets,
    )


if __name__ == "__main__":
    main()
