"""Integration tests for GenServer.handle_info — non-call/cast message handling."""

from collections.abc import Generator
from typing import Any

from tertius.effects import EEmit, ESend, ESpawn
from tertius.genserver import GenServer, mcall
from tertius.types import Pid
from tertius.vm import run


# ---------------------------------------------------------------------------
# Accumulator GenServer — collects raw messages via handle_info
# ---------------------------------------------------------------------------


class Accumulator(GenServer[list]):
    def init(self, *_: Any) -> list:
        return []

    def handle_info(self, state: list, body: Any) -> list:
        return state + [body]

    def handle_call(self, state: list, body: Any) -> tuple[list, list]:
        match body:
            case "get":
                return state, state
        raise NotImplementedError(body)


def run_accumulator() -> Generator[Any, Any, None]:
    yield from Accumulator().loop()


_SCOPE = {"run_accumulator": run_accumulator}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def _root_handle_info_single_message() -> Generator[Any, Any, None]:
    server: Pid = yield ESpawn(fn_name="run_accumulator")
    yield ESend(server, "hello")
    yield EEmit((yield from mcall(server, "get")))


def test_handle_info_receives_raw_message():
    """Proves that a non-cast/call message is routed to handle_info and updates state."""

    result = next(run(_root_handle_info_single_message, scope=_SCOPE))
    assert result == ["hello"]


def _root_handle_info_multiple_messages() -> Generator[Any, Any, None]:
    server: Pid = yield ESpawn(fn_name="run_accumulator")
    yield ESend(server, "a")
    yield ESend(server, "b")
    yield ESend(server, "c")
    yield EEmit((yield from mcall(server, "get")))


def test_handle_info_accumulates_multiple_messages():
    """Proves that handle_info is called for each raw message in arrival order."""

    result = next(run(_root_handle_info_multiple_messages, scope=_SCOPE))
    assert result == ["a", "b", "c"]
