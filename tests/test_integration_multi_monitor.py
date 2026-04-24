"""Integration tests for multiple monitors on a single process."""

from collections.abc import Generator
from typing import Any

from tertius.genserver import mcast
from tertius.effects import EMonitor, EReceive, ESelf, ESend, ESpawn
from tertius.exceptions import ProcessCrash
from tertius.types import CastMsg, Envelope, Pid
from tertius.vm import run


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def crash_on_command() -> Generator[Any, Any, None]:
    """Wait for any message, then raise."""
    yield EReceive()
    raise RuntimeError("bang")


def watch_and_forward(
    target_pid_bytes: bytes, collector_pid_bytes: bytes
) -> Generator[Any, Any, None]:
    """Monitor target, signal readiness to collector, then forward any crash notification."""
    target = Pid.from_bytes(target_pid_bytes)
    collector = Pid.from_bytes(collector_pid_bytes)
    yield EMonitor(pid=target)
    # Signal that the monitor is registered before waiting for the crash
    yield ESend(collector, "watching")
    envelope: Envelope = yield EReceive()
    yield from mcast(collector, envelope.body)


# ---------------------------------------------------------------------------
# Root program
# ---------------------------------------------------------------------------


def _root_two_watchers() -> Generator[Any, Any, list[Any]]:
    me: Pid = yield ESelf()
    crasher: Pid = yield ESpawn(fn_name="crash_on_command")

    for _ in range(2):
        yield ESpawn(
            fn_name="watch_and_forward",
            args=(bytes(crasher), bytes(me)),
        )

    # Wait until both watchers have registered their monitors before triggering crash
    watching_count = 0
    while watching_count < 2:
        envelope: Envelope = yield EReceive()
        if envelope.body == "watching":
            watching_count += 1

    yield from mcast(crasher, "go")

    results = []
    for _ in range(2):
        envelope = yield EReceive()
        match envelope.body:
            case CastMsg(body=body):
                results.append(body)

    return results


_SCOPE = {"crash_on_command": crash_on_command, "watch_and_forward": watch_and_forward}

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_all_monitors_receive_crash_notification(collect):
    """Proves that every process monitoring a crasher receives a ProcessCrash."""

    results, _ = collect(_root_two_watchers, scope=_SCOPE)
    assert len(results) == 2
    assert all(isinstance(res, ProcessCrash) for res in results)


def test_all_crash_notifications_identify_same_pid(collect):
    """Proves that all crash notifications carry the same crashed pid."""

    results, _ = collect(_root_two_watchers, scope=_SCOPE)
    pids = {res.pid for res in results}
    assert len(pids) == 1
