# pytest configuration — custom pass/fail reporter that prints test docstrings.
from collections.abc import Callable
from typing import Any

import pytest

from tertius.vm import run


@pytest.fixture
def collect():
    """Drain a run() call, returning (return_value, [all_emitted_events])."""

    def _collect(fn: Callable[..., Any], *args: Any, scope: dict | None = None) -> tuple[Any, list[Any]]:
        gen = run(fn, *args, scope=scope)
        events: list[Any] = []
        try:
            while True:
                events.append(next(gen))
        except StopIteration as stop:
            return stop.value, events

    return _collect


RESET = "\033[0m"
GREEN = "\033[32m"
RED = "\033[31m"


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()
    report._docstring = item.obj.__doc__.strip() if item.obj.__doc__ else item.name


def pytest_runtest_logreport(report):
    if report.when != "call":
        return

    doc = getattr(report, "_docstring", report.nodeid)
    indicator, colour = (
        (f"{GREEN}✓{RESET}", GREEN) if report.passed else (f"{RED}✗{RESET}", RED)
    )
    lines = [line.strip() for line in doc.splitlines() if line.strip()]
    print(f"\n  {indicator}  {colour}{lines[0]}{RESET}")

    for line in lines[1:]:
        print(f"     {colour}{line}{RESET}")
