import pickle
import queue
import threading
from collections.abc import Callable
from functools import partial
from typing import Any

import zmq

from tertius.constants import (
    CRASH,
    EMIT,
    ERROR,
    KILL,
    LINK,
    MONITOR,
    REGISTER,
    SPAWN,
    WHEREIS,
)
from tertius.types import Pid
from tertius.vm.broker_crash import handle_crash, handle_kill
from tertius.vm.broker_utils import reply
from tertius.vm.broker_handlers import (
    handle_emit,
    handle_link,
    handle_monitor,
    handle_register,
    handle_whereis,
)
from tertius.vm.broker_spawn import handle_spawn
from tertius.vm.broker_state import BrokerState

Scope = dict[str, Callable[..., Any]]


def make_ctrl_handlers(
    alloc_pid: Callable[[], Pid],
    scope: Scope,
    broker_addr: str,
    ctrl_addr: str,
    state: BrokerState,
    notifier: "zmq.Socket[bytes]",
) -> dict[bytes, Callable[..., None]]:
    """Factory for control command handlers"""

    handlers: dict[bytes, Callable[..., None]] = {}
    handlers[SPAWN] = partial(
        handle_spawn, alloc_pid, scope, broker_addr, ctrl_addr, state, handlers
    )
    handlers[REGISTER] = partial(handle_register, state)
    handlers[WHEREIS] = partial(handle_whereis, state)
    handlers[LINK] = partial(handle_link, state, notifier)
    handlers[MONITOR] = partial(handle_monitor, state, notifier)
    handlers[EMIT] = partial(handle_emit, state)
    handlers[KILL] = partial(handle_kill, state, notifier)
    handlers[CRASH] = partial(handle_crash, state, notifier)

    return handlers


class Broker:
    """Central VM coordinator. Runs two sockets on separate threads:

    - run_data: a dumb ROUTER that forwards messages between processes by
      address. Processes talk to each other through this without the broker
      needing to understand the content.

    - run_control: a ROUTER that handles VM-level operations (spawn, kill,
      link, monitor, etc.). All state that crosses process boundaries lives
      here — process registry, links, monitors, the dead-process tombstones.
    """

    def __init__(
        self, broker_addr: str, ctrl_addr: str, ctx: "zmq.Context[zmq.Socket[bytes]]", scope: Scope
    ) -> None:
        self._broker_addr = broker_addr
        self._ctrl_addr = ctrl_addr
        self._ctx = ctx
        self._scope = scope
        self._next_pid = 0
        self._pid_lock = threading.Lock()
        self._state = BrokerState()
        self.ready = threading.Event()  # signals that run_data is bound and accepting

    @property
    def emit_queue(self) -> "queue.Queue[Any]":
        return self._state.emit_queue

    def alloc_pid(self) -> Pid:
        # Lock ensures uniqueness across threads; pids are never reused so the
        # dead dict remains a reliable tombstone after a process exits.

        with self._pid_lock:
            pid = Pid(self._next_pid)
            self._next_pid += 1
            return pid

    def run_data(self) -> None:
        """Blind message relay between processes.

        Processes address each other directly by pid. The broker just forwards
        frames without inspecting the body — keeping latency low and the data
        path free of VM logic.
        """

        router: zmq.Socket[bytes] = self._ctx.socket(zmq.ROUTER)
        router.bind(self._broker_addr)
        self.ready.set()

        while True:
            _sender, target, sender_pid, body = router.recv_multipart()
            router.send_multipart([target, sender_pid, body])

    def run_control(self) -> None:
        """Serialised handler loop for all VM control operations.

        Single-threaded by design: all mutable VM state (_names, _links,
        _monitors, _dead, _procs) is touched exclusively here, so no locking
        is needed beyond alloc_pid.

        The notifier DEALER connects back to the data broker so that crash
        propagation messages (to monitors and linked processes) flow through
        the same relay path as normal inter-process messages.
        """
        # Wait until the data broker is bound so the notifier can connect.
        self.ready.wait()

        # Sends crash/kill notifications to affected processes via the data broker.
        notifier: zmq.Socket[bytes] = self._ctx.socket(zmq.DEALER)
        notifier.identity = b"vm-notifier"
        notifier.connect(self._broker_addr)

        router: zmq.Socket[bytes] = self._ctx.socket(zmq.ROUTER)
        router.bind(self._ctrl_addr)

        handlers = make_ctrl_handlers(
            self.alloc_pid,
            self._scope,
            self._broker_addr,
            self._ctrl_addr,
            self._state,
            notifier,
        )

        while True:
            frames = router.recv_multipart()
            requester = frames[0]
            command = frames[1]

            if command in handlers:
                try:
                    handlers[command](router, requester, frames)
                except Exception as err:
                    # Send the exception back to the caller rather than crashing
                    # the broker — one bad request shouldn't take down the VM.
                    reply(router, requester, ERROR, pickle.dumps(err))
