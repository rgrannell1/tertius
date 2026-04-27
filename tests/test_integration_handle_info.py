"""Integration tests for handle_info — non-call/cast message handling."""

from collections.abc import Generator
from typing import Any

from tertius.effects import ESend, ESpawn
from tertius.genserver import gen_server, mcall
from tertius.types import Pid

# ---------------------------------------------------------------------------
# Accumulator process — collects raw messages via handle_info
# ---------------------------------------------------------------------------


def _accumulator_call(state: list, body: Any) -> Generator[Any, Any, tuple[list, Any]]:
    match body:
        case "get":
            return state, state
    raise NotImplementedError(body)
    yield


def _accumulator_init(*_: Any) -> Generator[Any, Any, list]:
    return []
    yield


def _accumulator_info(state: list, body: Any) -> Generator[Any, Any, list]:
    return [*state, body]
    yield


accumulator = gen_server(
    init=_accumulator_init,
    handle_info=_accumulator_info,
    handle_call=_accumulator_call,
)


def run_accumulator() -> Generator[Any, Any, None]:
    yield from accumulator()


_SCOPE = {"run_accumulator": run_accumulator}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def _root_handle_info_single_message() -> Generator[Any, Any, Any]:
    server: Pid = yield ESpawn(fn_name="run_accumulator")
    yield ESend(server, "hello")
    return (yield from mcall(server, "get"))


def test_handle_info_receives_raw_message(collect):
    """Proves that a non-cast/call message is routed to handle_info and updates state."""

    result, _ = collect(_root_handle_info_single_message, scope=_SCOPE)
    assert result == ["hello"]


def _root_handle_info_multiple_messages() -> Generator[Any, Any, Any]:
    server: Pid = yield ESpawn(fn_name="run_accumulator")
    yield ESend(server, "a")
    yield ESend(server, "b")
    yield ESend(server, "c")
    return (yield from mcall(server, "get"))


def test_handle_info_accumulates_multiple_messages(collect):
    """Proves that handle_info is called for each raw message in arrival order."""

    result, _ = collect(_root_handle_info_multiple_messages, scope=_SCOPE)
    assert result == ["a", "b", "c"]
