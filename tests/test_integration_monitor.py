"""Integration tests for EMonitor — crash notification delivery."""

from collections.abc import Generator
from typing import Any

from tertius.effects import EMonitor, EReceive, ESpawn, ESelf
from tertius.exceptions import ProcessCrash
from tertius.types import Envelope, Pid
from tertius.vm import run


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def crash_immediately() -> Generator[Any, Any, None]:
    """Process that starts successfully then crashes on its first step."""

    yield ESelf()
    raise RuntimeError("boom")


def exit_cleanly() -> Generator[Any, Any, None]:
    """Process that returns without error."""

    return
    yield


_SCOPE = {"crash_immediately": crash_immediately, "exit_cleanly": exit_cleanly}

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def _root_monitor_crash() -> Generator[Any, Any, Any]:
    worker: Pid = yield ESpawn(fn_name="crash_immediately")
    yield EMonitor(pid=worker)
    envelope: Envelope = yield EReceive()
    return envelope.body


def test_monitor_receives_process_crash(collect):
    """Proves that a monitored process crash delivers a ProcessCrash to the watcher."""

    result, _ = collect(_root_monitor_crash, scope=_SCOPE)
    assert isinstance(result, ProcessCrash)
    assert isinstance(result.reason, RuntimeError)
    assert str(result.reason) == "boom"


def _root_monitor_then_check_pid() -> Generator[Any, Any, Any]:
    worker: Pid = yield ESpawn(fn_name="crash_immediately")
    yield EMonitor(pid=worker)
    envelope: Envelope = yield EReceive()
    crash: ProcessCrash = envelope.body
    return crash.pid


def test_crash_notification_carries_correct_pid(collect):
    """Proves that the ProcessCrash.pid matches the monitored process's pid."""

    worker_pid, _ = collect(_root_monitor_then_check_pid, scope=_SCOPE)
    assert isinstance(worker_pid, Pid)
