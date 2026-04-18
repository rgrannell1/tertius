"""Integration tests for name registry cleanup after process crash."""
from collections.abc import Generator
from typing import Any

from tertius.effects import EMonitor, EReceive, ERegister, ESpawn, EWhereis
from tertius.types import Pid
from tertius.vm import run


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def register_then_crash(name: str) -> Generator[Any, Any, None]:
    yield ERegister(name=name)
    raise RuntimeError("crash after register")
    yield


# ---------------------------------------------------------------------------
# Root programs
# ---------------------------------------------------------------------------


def _root_stale_name(name: str) -> Generator[Any, Any, Pid | None]:
    worker: Pid = yield ESpawn(
        fn_name="tests.test_integration_registry_crash:register_then_crash",
        args=(name,),
    )
    yield EMonitor(pid=worker)
    # Wait for crash notification so we know the process is gone
    yield EReceive()
    # Now the name should have been cleaned up
    return (yield EWhereis(name=name))


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_whereis_returns_none_after_registered_process_crashes():
    """Proves that EWhereis returns None after the named process has crashed."""

    result = run(_root_stale_name, "doomed-worker")
    assert result is None
