"""Tests for execution-policy helpers."""

from routing_cycle_detector.solver import execution


def test_executor_override_modes(monkeypatch) -> None:
    monkeypatch.setenv(execution.RC_EXECUTOR_ENV, "serial")
    assert execution.describe_executor(execution.get_executor_class()) == "serial"

    monkeypatch.setenv(execution.RC_EXECUTOR_ENV, "threads")
    assert execution.describe_executor(execution.get_executor_class()) == "threads"

    monkeypatch.setenv(execution.RC_EXECUTOR_ENV, "processes")
    assert execution.describe_executor(execution.get_executor_class()) == "processes"


def test_executor_auto_policy(monkeypatch) -> None:
    monkeypatch.delenv(execution.RC_EXECUTOR_ENV, raising=False)

    monkeypatch.setattr(execution, "is_gil_enabled", lambda: True)
    assert execution.describe_executor(execution.get_executor_class()) == "processes"

    monkeypatch.setattr(execution, "is_gil_enabled", lambda: False)
    assert execution.describe_executor(execution.get_executor_class()) == "threads"
