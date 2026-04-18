"""Integration tests for GenServer over real ZMQ processes."""
from collections.abc import Generator
from typing import Any

from tertius.genserver import GenServer, call, call_timeout, cast
from tertius.effects import ESpawn
from tertius.types import Pid
from tertius.vm import run


# ---------------------------------------------------------------------------
# Counter GenServer
# ---------------------------------------------------------------------------


class Counter(GenServer[int]):
    def init(self, initial: int = 0) -> int:
        return initial

    def handle_cast(self, state: int, body: Any) -> Generator[Any, Any, int]:
        match body:
            case ("inc", n):
                return state + n
            case _:
                return state
        yield

    def handle_call(self, state: int, body: Any) -> Generator[Any, Any, tuple[int, Any]]:
        match body:
            case "get":
                return state, state
        yield


def run_counter(initial: int) -> Generator[Any, Any, None]:
    yield from Counter().loop(initial)


# ---------------------------------------------------------------------------
# Root programs
# ---------------------------------------------------------------------------


def _root_cast_then_call() -> Generator[Any, Any, int]:
    server: Pid = yield ESpawn(
        fn_name="tests.test_integration_genserver:run_counter",
        args=(0,),
    )
    yield from cast(server, ("inc", 5))
    yield from cast(server, ("inc", 3))
    return (yield from call(server, "get"))


def _root_call_does_not_mutate() -> Generator[Any, Any, tuple[int, int]]:
    server: Pid = yield ESpawn(
        fn_name="tests.test_integration_genserver:run_counter",
        args=(10,),
    )
    first = yield from call(server, "get")
    second = yield from call(server, "get")
    return first, second


def _root_unknown_cast_ignored() -> Generator[Any, Any, int]:
    server: Pid = yield ESpawn(
        fn_name="tests.test_integration_genserver:run_counter",
        args=(7,),
    )
    yield from cast(server, "unknown-message")
    return (yield from call(server, "get"))


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_cast_accumulates_state_across_processes():
    """Proves that cast messages mutate GenServer state in a real process."""

    result = run(_root_cast_then_call)
    assert result == 8


def test_call_does_not_mutate_state_across_processes():
    """Proves that two consecutive get calls return the same value in a real process."""

    first, second = run(_root_call_does_not_mutate)
    assert first == second == 10


def test_unknown_cast_leaves_state_unchanged_across_processes():
    """Proves that an unrecognised cast body leaves state intact in a real process."""

    result = run(_root_unknown_cast_ignored)
    assert result == 7


# ---------------------------------------------------------------------------
# call_timeout integration
# ---------------------------------------------------------------------------


def _root_call_timeout_succeeds() -> Generator[Any, Any, Any]:
    server: Pid = yield ESpawn(
        fn_name="tests.test_integration_genserver:run_counter",
        args=(42,),
    )
    return (yield from call_timeout(server, "get", timeout_ms=2000))


def _root_call_timeout_fires() -> Generator[Any, Any, Any]:
    """Call a process that doesn't exist — timeout must fire."""
    ghost = Pid(99999)
    return (yield from call_timeout(ghost, "get", timeout_ms=50))


def test_call_timeout_returns_reply_when_server_is_alive():
    """Proves that call_timeout returns the reply when the server responds in time."""

    result = run(_root_call_timeout_succeeds)
    assert result == 42


def test_call_timeout_returns_none_when_server_is_unreachable():
    """Proves that call_timeout returns None when no reply arrives within the deadline."""

    result = run(_root_call_timeout_fires)
    assert result is None
