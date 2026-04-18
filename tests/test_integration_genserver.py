"""Integration tests for GenServer over real ZMQ processes."""
from collections.abc import Generator
from typing import Any

from tertius.genserver import GenServer, mcall, mcall_timeout, mcast
from tertius.effects import EEmit, ESpawn
from tertius.types import Pid
from tertius.vm import run


# ---------------------------------------------------------------------------
# Counter GenServer
# ---------------------------------------------------------------------------


class Counter(GenServer[int]):
    def init(self, initial: int = 0) -> int:
        return initial

    def handle_cast(self, state: int, body: Any) -> int:
        match body:
            case ("inc", n):
                return state + n
            case _:
                return state

    def handle_call(self, state: int, body: Any) -> tuple[int, int]:
        match body:
            case "get":
                return state, state


def run_counter(initial: int) -> Generator[Any, Any, None]:
    yield from Counter().loop(initial)


# ---------------------------------------------------------------------------
# Root programs
# ---------------------------------------------------------------------------


def _root_cast_then_call() -> Generator[Any, Any, None]:
    server: Pid = yield ESpawn(
        fn_name="run_counter",
        args=(0,),
    )
    yield from mcast(server, ("inc", 5))
    yield from mcast(server, ("inc", 3))
    yield EEmit((yield from mcall(server, "get")))


def _root_call_does_not_mutate() -> Generator[Any, Any, None]:
    server: Pid = yield ESpawn(
        fn_name="run_counter",
        args=(10,),
    )
    first = yield from mcall(server, "get")
    second = yield from mcall(server, "get")
    yield EEmit((first, second))


def _root_unknown_cast_ignored() -> Generator[Any, Any, None]:
    server: Pid = yield ESpawn(
        fn_name="run_counter",
        args=(7,),
    )
    yield from mcast(server, "unknown-message")
    yield EEmit((yield from mcall(server, "get")))


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


_SCOPE = {"run_counter": run_counter}


def test_cast_accumulates_state_across_processes():
    """Proves that cast messages mutate GenServer state in a real process."""

    result = next(run(_root_cast_then_call, scope=_SCOPE))
    assert result == 8


def test_call_does_not_mutate_state_across_processes():
    """Proves that two consecutive get calls return the same value in a real process."""

    first, second = next(run(_root_call_does_not_mutate, scope=_SCOPE))
    assert first == second == 10


def test_unknown_cast_leaves_state_unchanged_across_processes():
    """Proves that an unrecognised cast body leaves state intact in a real process."""

    result = next(run(_root_unknown_cast_ignored, scope=_SCOPE))
    assert result == 7


# ---------------------------------------------------------------------------
# call_timeout integration
# ---------------------------------------------------------------------------


def _root_call_timeout_succeeds() -> Generator[Any, Any, None]:
    server: Pid = yield ESpawn(
        fn_name="run_counter",
        args=(42,),
    )
    yield EEmit((yield from mcall_timeout(server, "get", timeout_ms=2000)))


def _root_call_timeout_fires() -> Generator[Any, Any, None]:
    """Call a process that doesn't exist — timeout must fire."""
    ghost = Pid(99999)
    yield EEmit((yield from mcall_timeout(ghost, "get", timeout_ms=50)))


def test_call_timeout_returns_reply_when_server_is_alive():
    """Proves that call_timeout returns the reply when the server responds in time."""

    result = next(run(_root_call_timeout_succeeds, scope=_SCOPE))
    assert result == 42


def test_call_timeout_returns_none_when_server_is_unreachable():
    """Proves that call_timeout returns None when no reply arrives within the deadline."""

    result = next(run(_root_call_timeout_fires, scope=_SCOPE))
    assert result is None
