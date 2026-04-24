"""Fuzz action types and runtime state for the Tertius VM fuzzer."""
from dataclasses import dataclass, field
from typing import Any

from tertius.types import Pid


@dataclass(frozen=True)
class SpawnAction:
    """Spawn a new worker process."""

    fn_name: str


@dataclass(frozen=True)
class KillAction:
    """Terminate a process at target_idx in the pid pool."""

    target_idx: int


@dataclass(frozen=True)
class SendAction:
    """Send a message to a process at target_idx in the pid pool."""

    target_idx: int
    body: Any


@dataclass(frozen=True)
class MonitorAction:
    """Set a one-shot monitor on a process at target_idx in the pid pool."""

    target_idx: int


@dataclass(frozen=True)
class LinkAction:
    """Bidirectionally link to a process at target_idx in the pid pool."""

    target_idx: int


@dataclass(frozen=True)
class RegisterAction:
    """Register the root process under a fuzz name."""

    name: str


@dataclass(frozen=True)
class WhereisAction:
    """Look up a fuzz-named process."""

    name: str


@dataclass(frozen=True)
class EmitAction:
    """Emit a value across the VM boundary to the test host."""

    body: Any


@dataclass(frozen=True)
class GetSelfAction:
    """Yield ESelf() and add root's PID to the pool so it can be targeted."""


@dataclass(frozen=True)
class FakePidAction:
    """Insert a randomly-constructed PID into the pool.

    This PID has never been spawned and has no tombstone — it exercises
    ghost-kill, dangling-link, and non-existent-monitor code paths.
    """

    node_id: int
    pid_id: int


@dataclass(frozen=True)
class SpawnLinkerAction:
    """Spawn a linker_worker that links to a specific target PID then waits.

    Forces worker-to-worker links so crash cascades propagate through the
    broker rather than only through the root process.
    """

    target_idx: int


@dataclass(frozen=True)
class SleepAction:
    """Sleep the root process for ms milliseconds.

    Creates a timing window where background worker operations (crashes,
    exits, sends) can advance concurrently while the root is paused.
    """

    ms: int


FuzzAction = (
    SpawnAction
    | KillAction
    | SendAction
    | MonitorAction
    | LinkAction
    | RegisterAction
    | WhereisAction
    | EmitAction
    | GetSelfAction
    | FakePidAction
    | SpawnLinkerAction
    | SleepAction
)


@dataclass
class FuzzRunState:
    """Tracks all PIDs ever spawned (including dead ones) and the total spawn count.

    pid_pool may include stale dead PIDs — intentional, so we fire into them.
    """

    pid_pool: list[Pid] = field(default_factory=list)
    spawn_count: int = 0
