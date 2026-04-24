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


def _root_stale_name(name: str) -> Generator[Any, Any, Any]:
    worker: Pid = yield ESpawn(
        fn_name="register_then_crash",
        args=(name,),
    )
    yield EMonitor(pid=worker)
    yield EReceive()
    return (yield EWhereis(name=name))


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


_SCOPE = {"register_then_crash": register_then_crash}


def test_whereis_returns_none_after_registered_process_crashes(collect):
    """Proves that EWhereis returns None after the named process has crashed."""

    result, _ = collect(_root_stale_name, "doomed-worker", scope=_SCOPE)
    assert result is None
