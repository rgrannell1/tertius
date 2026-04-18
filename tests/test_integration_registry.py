"""Integration tests for ERegister / EWhereis — name-based process lookup."""

from tertius.genserver import mcast
from collections.abc import Generator
from typing import Any

from tertius.effects import EEmit, EReceive, ERegister, ESpawn, EWhereis
from tertius.types import CastMsg, Envelope, Pid
from tertius.vm import run


# ---------------------------------------------------------------------------
# Fixtures (module-level so they are resolvable via scope)
# ---------------------------------------------------------------------------


def register_and_wait(name: str) -> Generator[Any, Any, None]:
    """Register under `name`, then echo back whatever is sent."""

    yield ERegister(name=name)
    envelope: Envelope = yield EReceive()

    match envelope.body:
        case CastMsg(body=body):
            yield from mcast(envelope.sender, body)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def _root_whereis_found(name: str) -> Generator[Any, Any, None]:
    yield ESpawn(
        fn_name="register_and_wait",
        args=(name,),
    )

    while True:
        result: Pid | None = yield EWhereis(name=name)
        if result is not None:
            yield EEmit(result)
            return


_SCOPE = {"register_and_wait": register_and_wait}


def test_whereis_returns_pid_of_registered_process():
    """Proves that a process registered under a name is findable via EWhereis."""

    result = next(run(_root_whereis_found, "echo-worker", scope=_SCOPE))
    assert isinstance(result, Pid)


def _root_whereis_unknown() -> Generator[Any, Any, None]:
    result = yield EWhereis(name="no-such-process")
    yield EEmit(result)


def test_whereis_returns_none_for_unknown_name():
    """Proves that EWhereis returns None when no process is registered under the name."""

    result = next(run(_root_whereis_unknown, scope=_SCOPE))
    assert result is None


def _root_roundtrip(name: str) -> Generator[Any, Any, None]:
    """Register a named echo worker, send it a message, collect the reply."""

    yield ESpawn(
        fn_name="register_and_wait",
        args=(name,),
    )

    while True:
        pid: Pid | None = yield EWhereis(name=name)
        if pid is not None:
            break

    yield from mcast(pid, "hello")

    envelope: Envelope = yield EReceive()
    match envelope.body:
        case CastMsg(body=body):
            yield EEmit(body)


def test_message_via_registered_name_roundtrips():
    """Proves that a message sent to a pid resolved by name arrives & gets a return message."""

    result = next(run(_root_roundtrip, "echo-worker-2", scope=_SCOPE))
    assert result == "hello"
