"""Integration tests for EKill — broker-initiated process termination."""

from collections.abc import Generator
from typing import Any

from tertius.effects import EEmit, EKill, ELink, EMonitor, EReceive, ESpawn
from tertius.exceptions import ProcessCrash
from tertius.types import Envelope, Pid
from tertius.vm import run


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


def _root_kill_notifies_monitor() -> Generator[Any, Any, None]:
    worker: Pid = yield ESpawn(fn_name="wait_forever")
    yield EMonitor(pid=worker)
    yield EKill(pid=worker)
    envelope: Envelope = yield EReceive()
    yield EEmit(envelope.body)


def test_kill_delivers_process_crash_to_monitor():
    """Proves that EKill causes a ProcessCrash notification to arrive at the monitor."""

    result = next(run(_root_kill_notifies_monitor, scope=_SCOPE))
    assert isinstance(result, ProcessCrash)
    assert isinstance(result.reason, RuntimeError)
    assert str(result.reason) == "killed"


def _root_kill_already_dead_is_noop() -> Generator[Any, Any, None]:
    worker: Pid = yield ESpawn(fn_name="wait_forever")
    yield EMonitor(pid=worker)
    yield EKill(pid=worker)
    envelope: Envelope = yield EReceive()
    yield EKill(pid=worker)
    yield EEmit(envelope.body)


def test_kill_already_dead_process_is_noop():
    """Proves that killing a dead process a second time does not hang or error."""

    result = next(run(_root_kill_already_dead_is_noop, scope=_SCOPE))
    assert isinstance(result, ProcessCrash)


def _root_kill_propagates_to_linked_peer() -> Generator[Any, Any, None]:
    target: Pid = yield ESpawn(fn_name="wait_forever")
    peer: Pid = yield ESpawn(fn_name="linked_waiter", args=(bytes(target),))
    yield EMonitor(pid=peer)
    yield EKill(pid=target)
    envelope: Envelope = yield EReceive()
    yield EEmit(envelope.body)


def test_kill_propagates_crash_to_linked_peer():
    """Proves that killing a process delivers a crash to processes linked to it."""

    result = next(run(_root_kill_propagates_to_linked_peer, scope=_SCOPE))
    assert isinstance(result, ProcessCrash)
