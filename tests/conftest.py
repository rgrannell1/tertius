# pytest configuration — spawn-mode-parametrised collect fixture, shared thread-exception
# capture, and a custom pass/fail reporter that prints test docstrings.
import threading
from collections.abc import Callable, Generator
from functools import partial
from typing import Any

import pytest

from tertius.constants import SpawnMode
from tertius.vm import run


@pytest.fixture(params=[SpawnMode.PROCESS, SpawnMode.THREAD], ids=["process", "thread"])
def collect(request):
    """Drain a run() call, returning (return_value, [all_emitted_events]).

    Parametrised over both spawn modes — every integration test using collect
    proves the actor semantics hold identically for process and thread workers.
    """

    def _collect(
        fn: Callable[..., Any], *args: Any, scope: dict | None = None, **kwargs: Any
    ) -> tuple[Any, list[Any]]:
        gen = run(fn, *args, scope=scope, spawn_mode=request.param, **kwargs)
        events: list[Any] = []
        try:
            while True:
                events.append(next(gen))
        except StopIteration as stop:
            return stop.value, events

    return _collect


def record_thread_exception(
    captured: list[BaseException], args: threading.ExceptHookArgs
) -> None:
    """Append an exception escaping to threading.excepthook to the capture list."""

    if args.exc_value is not None:
        captured.append(args.exc_value)


@pytest.fixture
def thread_exceptions() -> Generator[list[BaseException], None, None]:
    """Capture exceptions reaching threading.excepthook while the test runs."""

    captured: list[BaseException] = []
    original_hook = threading.excepthook
    threading.excepthook = partial(record_thread_exception, captured)
    try:
        yield captured
    finally:
        threading.excepthook = original_hook


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
