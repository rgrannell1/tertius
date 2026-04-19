import multiprocessing
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
    OK,
    REGISTER,
    SPAWN,
    WHEREIS,
)
from tertius.types import Pid
from tertius.vm.broker_crash import handle_crash, handle_kill
from tertius.vm.broker_handlers import (
    handle_emit,
    handle_link,
    handle_monitor,
    handle_register,
    handle_whereis,
)
from tertius.vm.broker_spawn import handle_spawn

Scope = dict[str, Callable[..., Any]]


class Broker:
    def __init__(
        self, broker_addr: str, ctrl_addr: str, ctx: "zmq.Context[bytes]", scope: Scope
    ) -> None:
        self._broker_addr = broker_addr
        self._ctrl_addr = ctrl_addr
        self._ctx = ctx
        self._scope = scope
        self._next_pid = 0
        self._pid_lock = threading.Lock()
        self._names: dict[str, Pid] = {}
        self._monitors: dict[Pid, list[Pid]] = {}
        self._links: dict[Pid, list[Pid]] = {}
        self._dead: dict[Pid, Exception] = {}
        self._procs: dict[Pid, multiprocessing.Process] = {}
        self.emit_queue: queue.Queue[Any] = queue.Queue()
        self.ready = threading.Event()

    def alloc_pid(self) -> Pid:
        with self._pid_lock:
            pid = Pid(self._next_pid)
            self._next_pid += 1
            return pid

    def run_data(self) -> None:
        router: zmq.Socket[bytes] = self._ctx.socket(zmq.ROUTER)
        router.bind(self._broker_addr)
        self.ready.set()

        while True:
            _sender, target, sender_pid, body = router.recv_multipart()
            router.send_multipart([target, sender_pid, body])

    def run_control(self) -> None:
        self.ready.wait()

        # Injects crash notifications into the data broker on behalf of crashed processes
        notifier: zmq.Socket[bytes] = self._ctx.socket(zmq.DEALER)
        notifier.identity = b"vm-notifier"
        notifier.connect(self._broker_addr)

        router: zmq.Socket[bytes] = self._ctx.socket(zmq.ROUTER)
        router.bind(self._ctrl_addr)

        handlers: dict = {}
        handlers[SPAWN] = partial(
            handle_spawn,
            self.alloc_pid, self._scope, self._broker_addr, self._ctrl_addr,
            self._procs, handlers,
        )
        handlers[REGISTER] = partial(handle_register, self._names)
        handlers[WHEREIS] = partial(handle_whereis, self._names)
        handlers[LINK] = partial(handle_link, self._links, self._dead, notifier)
        handlers[MONITOR] = partial(handle_monitor, self._monitors, self._dead, notifier)
        handlers[EMIT] = partial(handle_emit, self.emit_queue)
        handlers[KILL] = partial(
            handle_kill,
            self._procs, self._names, self._monitors, self._links, self._dead, notifier,
        )
        handlers[CRASH] = partial(
            handle_crash,
            self._names, self._monitors, self._links, self._dead, notifier,
        )

        while True:
            frames = router.recv_multipart()
            requester = frames[0]
            command = frames[1]

            if command in handlers:
                try:
                    handlers[command](router, requester, frames)
                except Exception as err:
                    router.send_multipart([requester, ERROR, pickle.dumps(err)])
