"""Spawnable worker process generators for the Tertius VM fuzzer."""
from collections.abc import Callable, Generator
from typing import Any

from tertius.effects import ELink, EReceive, EReceiveTimeout, ESelf, ESend, ESleep, ESpawn
from tertius.types import Envelope, Pid


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


def linker_worker(target_bytes: bytes) -> Generator[Any, Any, None]:
    """Links to the target PID then waits forever.

    Forces crash cascades to propagate through a worker-to-worker link rather
    than only through root-to-worker links.
    """
    yield ELink(pid=Pid.from_bytes(target_bytes))
    while True:
        yield EReceiveTimeout(200)


def spawner_worker() -> Generator[Any, Any, None]:
    """Spawns an immediate_exit_worker then exits.

    Issues a SPAWN command from a non-root process, creating concurrent
    broker command interleaving — the root may be issuing its own commands
    while this worker's spawn request is in flight.
    """
    yield ESpawn(fn_name="immediate_exit_worker")


def slow_crash_worker() -> Generator[Any, Any, None]:
    """Sleeps briefly then crashes.

    Creates a timing window: the process is alive long enough for the root
    to link or monitor it before it crashes, and long enough for the kill
    to race with the natural crash notification.
    """
    yield ESleep(ms=30)
    raise RuntimeError("slow crash")


def slow_exit_worker() -> Generator[Any, Any, None]:
    """Sleeps briefly then exits normally.

    Creates a race window between EKill arriving at the broker and the
    process's own NormalExit notification — both target the same PID.
    """
    yield ESleep(ms=30)


WORKER_SCOPE: dict[str, Callable[..., Any]] = {
    "idle_worker": idle_worker,
    "echo_worker": echo_worker,
    "crash_worker": crash_worker,
    "immediate_exit_worker": immediate_exit_worker,
    "linker_worker": linker_worker,
    "spawner_worker": spawner_worker,
    "slow_crash_worker": slow_crash_worker,
    "slow_exit_worker": slow_exit_worker,
}

# Workers that take no args — freely spawnable by SpawnAction.
WORKER_FN_NAMES: list[str] = [
    "idle_worker",
    "echo_worker",
    "crash_worker",
    "immediate_exit_worker",
    "spawner_worker",
    "slow_crash_worker",
    "slow_exit_worker",
]
