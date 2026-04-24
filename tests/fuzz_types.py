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


FuzzAction = (
    SpawnAction
    | KillAction
    | SendAction
    | MonitorAction
    | LinkAction
    | RegisterAction
    | WhereisAction
    | EmitAction
)


@dataclass
class FuzzRunState:
    """Tracks all PIDs ever spawned (including dead ones) and the total spawn count.

    pid_pool may include stale dead PIDs — intentional, so we fire into them.
    """

    pid_pool: list[Pid] = field(default_factory=list)
    spawn_count: int = 0
