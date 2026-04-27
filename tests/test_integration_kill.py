"""Integration tests for EKill — broker-initiated process termination."""

from collections.abc import Generator
from typing import Any

import pytest

from tertius.effects import EKill, ELink, EMonitor, EReceive, ESpawn
from tertius.exceptions import DeadProcessError, ProcessCrashError
from tertius.types import Envelope, Pid

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def wait_forever() -> Generator[Any, Any, None]:
    yield EReceive()


def linked_waiter(other_pid_bytes: bytes) -> Generator[Any, Any, None]:
    yield ELink(pid=Pid.from_bytes(other_pid_bytes))
    yield EReceive()


_SCOPE = {"wait_forever": wait_forever, "linked_waiter": linked_waiter}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def _root_kill_notifies_monitor() -> Generator[Any, Any, Any]:
    worker: Pid = yield ESpawn(fn_name="wait_forever")
    yield EMonitor(pid=worker)
    yield EKill(pid=worker)
    envelope: Envelope = yield EReceive()
    return envelope.body


def test_kill_delivers_process_crash_to_monitor(collect):
    """Proves that EKill causes a ProcessCrashError notification to arrive at the monitor."""

    result, _ = collect(_root_kill_notifies_monitor, scope=_SCOPE)
    assert isinstance(result, ProcessCrashError)
    assert isinstance(result.reason, RuntimeError)
    assert str(result.reason) == "killed"


def _root_kill_already_dead_raises() -> Generator[Any, Any, None]:
    worker: Pid = yield ESpawn(fn_name="wait_forever")
    yield EMonitor(pid=worker)
    yield EKill(pid=worker)
    yield EReceive()
    yield EKill(pid=worker)


def test_kill_already_dead_process_raises(collect):
    """Proves that killing an already-dead process raises DeadProcessError."""

    with pytest.raises(DeadProcessError):
        collect(_root_kill_already_dead_raises, scope=_SCOPE)


def _root_kill_propagates_to_linked_peer() -> Generator[Any, Any, Any]:
    target: Pid = yield ESpawn(fn_name="wait_forever")
    peer: Pid = yield ESpawn(fn_name="linked_waiter", args=(bytes(target),))
    yield EMonitor(pid=peer)
    yield EKill(pid=target)
    envelope: Envelope = yield EReceive()
    return envelope.body


def test_kill_propagates_crash_to_linked_peer(collect):
    """Proves that killing a process delivers a crash to processes linked to it."""

    result, _ = collect(_root_kill_propagates_to_linked_peer, scope=_SCOPE)
    assert isinstance(result, ProcessCrashError)
