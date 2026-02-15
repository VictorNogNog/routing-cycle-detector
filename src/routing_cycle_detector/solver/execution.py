"""Execution policy and executor selection utilities."""

import os
import sys
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

type ExecutorClass = type[ThreadPoolExecutor] | type[ProcessPoolExecutor] | None

# Environment variable to override executor selection.
RC_EXECUTOR_ENV = "RC_EXECUTOR"


def is_gil_enabled() -> bool:
    """Check if GIL is enabled."""
    try:
        return sys._is_gil_enabled()
    except AttributeError:
        return True


def get_executor_class() -> ExecutorClass:
    """
    Select the appropriate executor class.

    Priority:
    1. RC_EXECUTOR env var override ("threads", "processes", or "serial")
    2. Auto-select based on GIL status (disabled -> threads, enabled -> processes)

    "serial" mode runs in the main thread - useful for debugging with breakpoints.
    """
    executor_override = os.environ.get(RC_EXECUTOR_ENV, "").lower()

    if executor_override == "threads":
        return ThreadPoolExecutor
    if executor_override == "processes":
        return ProcessPoolExecutor
    if executor_override == "serial":
        return None

    if is_gil_enabled():
        return ProcessPoolExecutor
    return ThreadPoolExecutor


def describe_executor(executor_class: ExecutorClass) -> str:
    """Convert an executor class into a readable policy name."""
    if executor_class is None:
        return "serial"
    if executor_class is ThreadPoolExecutor:
        return "threads"
    return "processes"
