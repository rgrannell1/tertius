"""Spawnable worker process generators for the Tertius VM fuzzer."""
from collections.abc import Callable, Generator
from typing import Any

from tertius.effects import EReceive, EReceiveTimeout, ESelf, ESend
from tertius.types import Envelope


def idle_worker() -> Generator[Any, Any, None]:
    """Loops indefinitely, discarding all received messages."""
    while True:
        yield EReceiveTimeout(200)


def echo_worker() -> Generator[Any, Any, None]:
    """Receives one message and sends the body back to the sender."""
    envelope: Envelope = yield EReceive()
    yield ESend(pid=envelope.sender, body=envelope.body)


def crash_worker() -> Generator[Any, Any, None]:
    """Starts successfully then crashes immediately after the first yield."""
    yield ESelf()
    raise RuntimeError("fuzz crash")


def immediate_exit_worker() -> Generator[Any, Any, None]:
    """Exits cleanly without doing anything."""
    return
    yield


WORKER_SCOPE: dict[str, Callable[..., Any]] = {
    "idle_worker": idle_worker,
    "echo_worker": echo_worker,
    "crash_worker": crash_worker,
    "immediate_exit_worker": immediate_exit_worker,
}

WORKER_FN_NAMES: list[str] = list(WORKER_SCOPE.keys())
