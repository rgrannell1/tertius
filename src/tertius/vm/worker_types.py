"""Types describing a single spawned worker, shared by the broker and worker entry points."""

from dataclasses import dataclass
from typing import Any

from tertius.transport_types import Transport
from tertius.types import Pid, Scope


@dataclass(frozen=True)
class WorkerSpec:
    """Everything a worker needs to boot: identity, entry function, and broker wiring.

    Picklable, so it can cross into a spawn-context OS process or be round-tripped
    into a thread worker for copy semantics.
    """

    pid: Pid
    fn_name: str
    args: tuple[Any, ...]
    scope: Scope
    broker_addr: str
    ctrl_addr: str
    transport: Transport
