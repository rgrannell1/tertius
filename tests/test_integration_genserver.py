"""Integration tests for gen_server over real ZMQ processes."""

from collections.abc import Generator
from typing import Any

from tertius.effects import EEmit, ESleep, ESpawn
from tertius.genserver import gen_server, mcall, mcall_timeout, mcast
from tertius.types import Pid
from tertius.vm import run

# ---------------------------------------------------------------------------
# Counter process
# ---------------------------------------------------------------------------


def _init(initial: int = 0) -> Generator[Any, Any, int]:
    return initial
    yield


def _cast(state: int, body: Any) -> Generator[Any, Any, int]:
    match body:
        case ("inc", n):
            return state + n
        case _:
            return state
    yield


def _call(state: int, body: Any) -> Generator[Any, Any, tuple[int, Any]]:
    match body:
        case "get":
            return state, state
    raise NotImplementedError(body)
    yield


counter = gen_server(init=_init, handle_cast=_cast, handle_call=_call)


def run_counter(initial: int) -> Generator[Any, Any, None]:
    yield from counter(initial)


# ---------------------------------------------------------------------------
# Root programs
# ---------------------------------------------------------------------------


def _root_cast_then_call() -> Generator[Any, Any, Any]:
    server: Pid = yield ESpawn(
        fn_name="run_counter",
        args=(0,),
    )
    yield from mcast(server, ("inc", 5))
    yield from mcast(server, ("inc", 3))
    return (yield from mcall(server, "get"))


def _root_call_does_not_mutate() -> Generator[Any, Any, Any]:
    server: Pid = yield ESpawn(
        fn_name="run_counter",
        args=(10,),
    )
    first = yield from mcall(server, "get")
    second = yield from mcall(server, "get")
    return (first, second)


def _root_unknown_cast_ignored() -> Generator[Any, Any, Any]:
    server: Pid = yield ESpawn(
        fn_name="run_counter",
        args=(7,),
    )
    yield from mcast(server, "unknown-message")
    return (yield from mcall(server, "get"))


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


_SCOPE = {"run_counter": run_counter}


def test_cast_accumulates_state_across_processes(collect):
    """Proves that cast messages mutate gen_server state in a real process."""

    result, _ = collect(_root_cast_then_call, scope=_SCOPE)
    assert result == 8


def test_call_does_not_mutate_state_across_processes(collect):
    """Proves that two consecutive get calls return the same value in a real process."""

    result, _ = collect(_root_call_does_not_mutate, scope=_SCOPE)
    first, second = result
    assert first == second == 10


def test_unknown_cast_leaves_state_unchanged_across_processes(collect):
    """Proves that an unrecognised cast body leaves state intact in a real process."""

    result, _ = collect(_root_unknown_cast_ignored, scope=_SCOPE)
    assert result == 7


# ---------------------------------------------------------------------------
# call_timeout integration
# ---------------------------------------------------------------------------


def _root_call_timeout_succeeds() -> Generator[Any, Any, Any]:
    server: Pid = yield ESpawn(
        fn_name="run_counter",
        args=(42,),
    )
    return (yield from mcall_timeout(server, "get", timeout_ms=2000))


def _root_call_timeout_fires() -> Generator[Any, Any, Any]:
    """Call a process that doesn't exist — timeout must fire."""
    ghost = Pid(node_id=0, id=99999)
    return (yield from mcall_timeout(ghost, "get", timeout_ms=50))


def test_call_timeout_returns_reply_when_server_is_alive(collect):
    """Proves that call_timeout returns the reply when the server responds in time."""

    result, _ = collect(_root_call_timeout_succeeds, scope=_SCOPE)
    assert result == 42


def test_call_timeout_returns_none_when_server_is_unreachable(collect):
    """Proves that call_timeout returns None when no reply arrives within the deadline."""

    result, _ = collect(_root_call_timeout_fires, scope=_SCOPE)
    assert result is None


# ---------------------------------------------------------------------------
# Effectful handlers — cast and init that yield real process effects
# ---------------------------------------------------------------------------


def _emitting_cast(state: int, body: Any) -> Generator[Any, Any, int]:
    # generator cast: emits a telemetry event as a side-effect before updating state
    match body:
        case ("inc", n):
            yield EEmit(("cast_applied", n))
            return state + n
        case _:
            return state


def _sleeping_init(initial: int) -> Generator[Any, Any, int]:
    # generator init: sleeps briefly then returns the initial state
    yield ESleep(ms=1)
    return initial


_emitting_counter = gen_server(
    init=_init, handle_cast=_emitting_cast, handle_call=_call
)
_sleeping_init_counter = gen_server(
    init=_sleeping_init, handle_cast=_cast, handle_call=_call
)


def run_emitting_counter(initial: int) -> Generator[Any, Any, None]:
    yield from _emitting_counter(initial)


def run_sleeping_init_counter(initial: int) -> Generator[Any, Any, None]:
    yield from _sleeping_init_counter(initial)


def _root_emitting_cast() -> Generator[Any, Any, None]:
    server: Pid = yield ESpawn(fn_name="run_emitting_counter", args=(0,))
    yield from mcast(server, ("inc", 5))
    result = yield from mcall(server, "get")
    yield EEmit(("final", result))


def _root_sleeping_init() -> Generator[Any, Any, Any]:
    server: Pid = yield ESpawn(fn_name="run_sleeping_init_counter", args=(99,))
    return (yield from mcall(server, "get"))


_EFFECTFUL_SCOPE = {
    "run_emitting_counter": run_emitting_counter,
    "run_sleeping_init_counter": run_sleeping_init_counter,
}


def test_handler_can_emit_telemetry_as_side_effect():
    """Proves that a generator handle_cast can yield EEmit and the event arrives at the caller."""

    events = list(run(_root_emitting_cast, scope=_EFFECTFUL_SCOPE))
    assert ("cast_applied", 5) in events
    assert ("final", 5) in events


def test_generator_init_can_yield_process_effects(collect):
    """Proves that a generator init function can yield effects handled by the process runtime."""

    result, _ = collect(_root_sleeping_init, scope=_EFFECTFUL_SCOPE)
    assert result == 99
