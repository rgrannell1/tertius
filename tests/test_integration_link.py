"""Integration tests for ELink — bidirectional crash propagation."""
from collections.abc import Generator
from typing import Any

from tertius.effects import ELink, EMonitor, EReceive, EReceiveTimeout, ESpawn, ESelf
from tertius.exceptions import ProcessCrash
from tertius.types import Envelope, Pid
from tertius.vm import run


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def crash_immediately() -> Generator[Any, Any, None]:
    raise RuntimeError("bang")
    yield


def linked_worker(peer_pid_bytes: bytes) -> Generator[Any, Any, None]:
    """Link to peer, then block — dies when peer crashes."""
    yield ELink(pid=Pid.from_bytes(peer_pid_bytes))
    yield EReceive()


def linked_worker_timeout(peer_pid_bytes: bytes) -> Generator[Any, Any, None]:
    """Link to peer, block on timeout — dies when peer crashes."""
    yield ELink(pid=Pid.from_bytes(peer_pid_bytes))
    yield EReceiveTimeout(timeout_ms=5000)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

_SCOPE = {
    "crash_immediately": crash_immediately,
    "linked_worker": linked_worker,
    "linked_worker_timeout": linked_worker_timeout,
}


def _root_link_propagates_crash() -> Generator[Any, Any, Any]:
    """Spawn a crasher and a worker linked to it; monitor the worker to observe it dying."""
    me: Pid = yield ESelf()
    crasher: Pid = yield ESpawn(fn_name="crash_immediately")
    worker: Pid = yield ESpawn(fn_name="linked_worker", args=(bytes(crasher),))
    yield EMonitor(pid=worker)
    envelope: Envelope = yield EReceive()
    return envelope.body


def test_linked_process_dies_when_peer_crashes():
    """Proves that a process linked to a crasher also dies."""
    result = run(_root_link_propagates_crash, scope=_SCOPE)
    assert isinstance(result, ProcessCrash)


def _root_link_kills_receive_timeout() -> Generator[Any, Any, Any]:
    """Spawn a crasher and a worker blocked on EReceiveTimeout linked to it."""
    crasher: Pid = yield ESpawn(fn_name="crash_immediately")
    worker: Pid = yield ESpawn(fn_name="linked_worker_timeout", args=(bytes(crasher),))
    yield EMonitor(pid=worker)
    envelope: Envelope = yield EReceive()
    return envelope.body


def test_linked_process_dies_when_blocked_on_receive_timeout():
    """Proves that EReceiveTimeout raises LinkedCrash when a linked peer crashes."""
    result = run(_root_link_kills_receive_timeout, scope=_SCOPE)
    assert isinstance(result, ProcessCrash)


