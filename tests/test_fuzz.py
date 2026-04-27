"""Fuzz tests for the Tertius VM — asserts no combination of process operations crashes the VM."""
import contextlib
import threading
from collections import defaultdict
from functools import partial
from typing import Any

import pytest
import zmq
from bookman.events import Event

from tertius.exceptions import LinkedCrashError

from .fuzz_executor import fuzz_root, generate_action_sequence
from .fuzz_types import FuzzAction
from .fuzz_workers import WORKER_SCOPE

# Steps per fuzz run
SEQUENCE_LENGTH = 50

# Number of distinct seeds to exercise
NUM_SEEDS = 50


def _events_by_tag(events: list[Any]) -> dict[str, list[Event]]:
    """Group bookman Events by their tag dimension; non-Event items are ignored."""
    result: dict[str, list[Event]] = defaultdict(list)
    for item in events:
        if isinstance(item, Event):
            result[item.dim("tag")].append(item)
    return result


def _pids_with_tag(by_tag: dict[str, list[Event]], tag: str) -> list[str]:
    """Return the PID hex strings from all events carrying the given tag."""
    return [event.dim("id") for event in by_tag.get(tag, [])]


def _assert_no_double_terminal(by_tag: dict[str, list[Event]]) -> None:
    """Assert a PID cannot appear in both process:crash and process:exit."""
    crash_pids = set(_pids_with_tag(by_tag, "process:crash"))
    exit_pids = set(_pids_with_tag(by_tag, "process:exit"))
    overlap = crash_pids & exit_pids
    assert not overlap, f"PIDs with both process:crash and process:exit: {overlap}"


def _assert_no_double_crash(by_tag: dict[str, list[Event]]) -> None:
    """Assert no PID appears in process:crash more than once."""
    crash_pids = _pids_with_tag(by_tag, "process:crash")
    counts: dict[str, int] = defaultdict(int)
    for pid_hex in crash_pids:
        counts[pid_hex] += 1
    doubled = {pid_hex: count for pid_hex, count in counts.items() if count > 1}
    assert not doubled, f"PIDs with multiple process:crash events: {doubled}"


def _assert_spawns_resolved(by_tag: dict[str, list[Event]]) -> None:
    """Assert every spawn:started is followed by spawn:ready or spawn:timeout."""
    started = set(_pids_with_tag(by_tag, "spawn:started"))
    ready = set(_pids_with_tag(by_tag, "spawn:ready"))
    timed_out = set(_pids_with_tag(by_tag, "spawn:timeout"))
    unresolved = started - ready - timed_out
    assert not unresolved, f"PIDs with spawn:started but no resolution: {unresolved}"


def _format_event_line(event: Any) -> str:
    if not isinstance(event, Event):
        return f"(user-emitted) {event!r}"
    tag = event.dim("tag")
    pid_id = event.dim("id")
    parts = [f"{tag:<28} id={pid_id}"]
    with contextlib.suppress(Exception):
        parts.append(f"  name={event.dim('name')!r}")
    value = getattr(event, "value", None)
    if value is not None:
        parts.append(f"  value={value}")
    return "".join(parts)


def _format_event_stream(events: list[Any]) -> str:
    lines = [f"Event stream ({len(events)} items):"]
    lines.extend(f"  [{idx:3d}] {_format_event_line(ev)}" for idx, ev in enumerate(events))
    return "\n".join(lines)


def _format_action_sequence(seed: int, actions: list[FuzzAction]) -> str:
    lines = [f"Action sequence (seed={seed}, {len(actions)} steps):"]
    lines.extend(f"  [{idx:3d}] {action!r}" for idx, action in enumerate(actions))
    return "\n".join(lines)


def _assert_telemetry_invariants(events: list[Any]) -> None:
    """Assert structural invariants on the bookman event stream."""
    by_tag = _events_by_tag(events)
    _assert_no_double_terminal(by_tag)
    _assert_no_double_crash(by_tag)
    _assert_spawns_resolved(by_tag)


@pytest.mark.parametrize("seed", range(NUM_SEEDS))
def test_fuzz_no_thread_exception_under_random_message_traffic(collect, seed):
    """No ZMQ error must escape to the thread machinery under any random workload.

    message_flood_worker saturates the data router so the send_multipart race
    fires whenever ctx.term() overlaps with an in-flight forward.
    Before the fix this catches ContextTerminated escaping from _run_data_loop.
    """
    thread_exceptions: list[BaseException] = []
    original_hook = threading.excepthook

    def _capture(args: threading.ExceptHookArgs) -> None:
        thread_exceptions.append(args.exc_value)

    threading.excepthook = _capture
    try:
        with contextlib.suppress(LinkedCrashError):
            collect(partial(fuzz_root, seed, SEQUENCE_LENGTH), scope=WORKER_SCOPE)
    finally:
        threading.excepthook = original_hook

    zmq_errors = [exc for exc in thread_exceptions if isinstance(exc, zmq.ZMQError)]
    if zmq_errors:
        actions = generate_action_sequence(seed, SEQUENCE_LENGTH)
        pytest.fail(
            f"Unhandled ZMQ errors in broker threads: {zmq_errors}\n"
            f"{_format_action_sequence(seed, actions)}",
            pytrace=False,
        )


@pytest.mark.parametrize("seed", range(NUM_SEEDS))
def test_fuzz_vm_survives_random_action_sequence(collect, seed):
    """VM must not crash under any random sequence of process operations.

    LinkedCrashError propagating to the root is expected Erlang-style behaviour (the
    root linked to a process that crashed) and is not a failure.
    """
    try:
        _result, events = collect(
            partial(fuzz_root, seed, SEQUENCE_LENGTH),
            scope=WORKER_SCOPE,
        )
    except LinkedCrashError:
        return

    try:
        _assert_telemetry_invariants(events)
    except AssertionError as err:
        actions = generate_action_sequence(seed, SEQUENCE_LENGTH)
        pytest.fail(
            f"\n{_format_action_sequence(seed, actions)}\n\n{_format_event_stream(events)}\n\nViolation: {err}",
            pytrace=False,
        )
