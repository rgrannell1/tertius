"""Integration tests for multiple monitors on a single process."""
from collections.abc import Generator
from typing import Any

from tertius.decorators import cast
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
    yield from cast(collector, envelope.body)


# ---------------------------------------------------------------------------
# Root program
# ---------------------------------------------------------------------------


def _root_two_watchers() -> Generator[Any, Any, list[Any]]:
    me: Pid = yield ESelf()
    crasher: Pid = yield ESpawn(
        fn_name="tests.test_integration_multi_monitor:crash_on_command"
    )

    for _ in range(2):
        yield ESpawn(
            fn_name="tests.test_integration_multi_monitor:watch_and_forward",
            args=(bytes(crasher), bytes(me)),
        )

    # Wait until both watchers have registered their monitors before triggering crash
    watching_count = 0
    while watching_count < 2:
        envelope: Envelope = yield EReceive()
        if envelope.body == "watching":
            watching_count += 1

    yield from cast(crasher, "go")

    results = []
    for _ in range(2):
        envelope = yield EReceive()
        match envelope.body:
            case CastMsg(body=body):
                results.append(body)

    return results


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_all_monitors_receive_crash_notification():
    """Proves that every process monitoring a crasher receives a ProcessCrash."""

    results = run(_root_two_watchers)
    assert len(results) == 2
    assert all(isinstance(res, ProcessCrash) for res in results)


def test_all_crash_notifications_identify_same_pid():
    """Proves that all crash notifications carry the same crashed pid."""

    results = run(_root_two_watchers)
    pids = {res.pid for res in results}
    assert len(pids) == 1
