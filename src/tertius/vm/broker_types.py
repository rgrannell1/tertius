"""Configuration types for the broker and its spawn handler."""

from collections.abc import Callable
from dataclasses import dataclass

from tertius.constants import SpawnMode
from tertius.transport_types import Transport
from tertius.types import Pid, Scope


@dataclass(frozen=True)
class BrokerConfig:
    """Static configuration a broker is constructed with."""

    broker_addr: str
    ctrl_addr: str
    scope: Scope
    node_id: int
    transport: Transport
    spawn_mode: SpawnMode


@dataclass(frozen=True)
class SpawnContext:
    """Everything handle_spawn needs to start workers, bundled for partial application."""

    alloc_pid: Callable[[], Pid]
    scope: Scope
    broker_addr: str
    ctrl_addr: str
    transport: Transport
    default_mode: SpawnMode
