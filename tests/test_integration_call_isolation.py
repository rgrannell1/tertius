"""Integration tests for call() ref isolation under concurrent callers."""

from collections.abc import Generator
from typing import Any

from tertius.genserver import gen_server, mcall, mcast
from tertius.effects import EEmit, EReceive, ESelf, ESpawn
from tertius.types import CastMsg, Envelope, Pid
from tertius.vm import run


# ---------------------------------------------------------------------------
# Echo process — replies with whatever body it receives
# ---------------------------------------------------------------------------


def _echo_init(*_: Any) -> None:
    return None


def _echo_call(state: None, body: Any) -> tuple[None, Any]:
    return state, body


echo = gen_server(init=_echo_init, handle_call=_echo_call)


def run_echo() -> Generator[Any, Any, None]:
    yield from echo()


# ---------------------------------------------------------------------------
# Caller fixture
# ---------------------------------------------------------------------------


def caller(
    server_pid_bytes: bytes, collector_pid_bytes: bytes, body: Any
) -> Generator[Any, Any, None]:
    server = Pid.from_bytes(server_pid_bytes)
    collector = Pid.from_bytes(collector_pid_bytes)
    reply = yield from mcall(server, body)
    yield from mcast(collector, reply)


# ---------------------------------------------------------------------------
# Root program
# ---------------------------------------------------------------------------


def _root_concurrent_calls() -> Generator[Any, Any, None]:
    me: Pid = yield ESelf()
    server: Pid = yield ESpawn(fn_name="run_echo")

    yield ESpawn(
        fn_name="caller",
        args=(bytes(server), bytes(me), "hello"),
    )
    yield ESpawn(
        fn_name="caller",
        args=(bytes(server), bytes(me), "world"),
    )

    results = []
    for _ in range(2):
        envelope: Envelope = yield EReceive()
        match envelope.body:
            case CastMsg(body=body):
                results.append(body)

    yield EEmit(sorted(results))


_SCOPE = {"run_echo": run_echo, "caller": caller}

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_concurrent_callers_each_receive_correct_reply():
    """Proves that two concurrent callers each receive their own reply, not each other's."""

    results = next(run(_root_concurrent_calls, scope=_SCOPE))
    assert results == ["hello", "world"]
