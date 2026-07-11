"""Types for the VM runner — root-thread wiring and run options."""

import queue
from dataclasses import dataclass
from typing import Any

import zmq

from tertius.constants import SpawnMode
from tertius.transport_types import Transport
from tertius.types import Pid


@dataclass(frozen=True)
class VmOptions:
    """Per-run VM configuration."""

    transport: Transport
    spawn_mode: SpawnMode


@dataclass(frozen=True)
class RootWiring:
    """Sockets and queue the root thread runs against."""

    pid: Pid
    dealer: "zmq.Socket[bytes]"
    ctrl: "zmq.Socket[bytes]"
    ctx: "zmq.Context[zmq.Socket[bytes]]"
    emit_queue: "queue.Queue[Any]"


@dataclass
class RootSlot:
    """Mutable cell the root thread writes its outcome into."""

    exc: BaseException | None = None
    result: Any = None
