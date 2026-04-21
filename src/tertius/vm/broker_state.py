# Shared mutable state for the broker — process registry, links, monitors, and tombstones.
import multiprocessing
import queue
from dataclasses import dataclass, field
from typing import Any

from tertius.types import Pid


@dataclass
class BrokerState:
    """Shared state for the broker and its handlers."""

    # registered name -> pid
    names: dict[str, Pid] = field(default_factory=dict)

    # watched pid -> {watchers}
    monitors: dict[Pid, set[Pid]] = field(default_factory=dict)

    # pid -> bidirectional crash partners
    links: dict[Pid, set[Pid]] = field(default_factory=dict)

    # tombstone: pid -> crash reason
    dead: dict[Pid, Exception] = field(default_factory=dict)

    # live OS processes
    procs: dict[Pid, multiprocessing.Process] = field(default_factory=dict)

    # outbound events for the host
    emit_queue: queue.Queue[Any] = field(default_factory=queue.Queue)
