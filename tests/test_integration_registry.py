"""Integration tests for ERegister / EWhereis — name-based process lookup."""

from tertius.genserver import cast
from collections.abc import Generator
from typing import Any

from tertius.effects import EReceive, ERegister, ESpawn, EWhereis
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
            yield from __import__("tertius.genserver", fromlist=["cast"]).cast(
                envelope.sender, body
            )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def _root_whereis_found(name: str) -> Generator[Any, Any, Pid | None]:
    """Spawn a named worker, look it up by name, return the resolved pid."""

    yield ESpawn(
        fn_name="tests.test_integration_registry:register_and_wait",
        args=(name,),
    )

    # Give the worker a moment to register — poll until non-None
    while True:
        result: Pid | None = yield EWhereis(name=name)
        if result is not None:
            return result


def test_whereis_returns_pid_of_registered_process():
    """Proves that a process registered under a name is findable via EWhereis."""

    result = run(_root_whereis_found, "echo-worker")
    assert isinstance(result, Pid)


def _root_whereis_unknown() -> Generator[Any, Any, Pid | None]:
    return (yield EWhereis(name="no-such-process"))


def test_whereis_returns_none_for_unknown_name():
    """Proves that EWhereis returns None when no process is registered under the name."""

    result = run(_root_whereis_unknown)
    assert result is None


def _root_roundtrip(name: str) -> Generator[Any, Any, Any]:
    """Register a named echo worker, send it a message, collect the reply."""


    yield ESpawn(
        fn_name="tests.test_integration_registry:register_and_wait",
        args=(name,),
    )

    while True:
        pid: Pid | None = yield EWhereis(name=name)
        if pid is not None:
            break

    yield from cast(pid, "hello")

    envelope: Envelope = yield EReceive()
    match envelope.body:
        case CastMsg(body=body):
            return body


def test_message_via_registered_name_roundtrips():
    """Proves that a message sent to a pid resolved by name arrives & gets a return message."""

    result = run(_root_roundtrip, "echo-worker-2")
    assert result == "hello"
