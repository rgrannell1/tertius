# VM entry point — wires the broker, process threads, and effect handlers together.
import hashlib
import itertools
import os
import queue
import socket
import threading
from collections.abc import Callable, Generator
from functools import partial
from typing import Any

import zmq
from orbis import complete

from tertius.types import Pid, Scope
from tertius.vm.broker import Broker
from tertius.vm.process import make_handlers


def make_node_id(host: str, port: int) -> int:
    """Derive a 4-byte node identifier from a host and port."""
    digest = hashlib.sha256(f"{host}:{port}".encode()).digest()
    return int.from_bytes(digest[:4], "big")

_DONE = object()
_vm_id = itertools.count().__next__


def _root_thread(
    fn: Callable[..., Any],
    args: tuple[Any, ...],
    root_pid: Pid,
    dealer: "zmq.Socket[bytes]",
    ctrl: "zmq.Socket[bytes]",
    ctx: "zmq.Context[zmq.Socket[bytes]]",
    root_exc: list[BaseException],
    emit_queue: "queue.Queue[Any]",
) -> None:
    try:
        complete(fn(*args), **make_handlers(root_pid, dealer, ctrl))
    except Exception as err:
        root_exc.append(err)
    finally:
        dealer.close()
        ctrl.close()
        ctx.term()
        emit_queue.put(_DONE)


class VM:
    def __init__(self, scope: Scope) -> None:
        vm_pid = os.getpid()
        vm_instance = _vm_id()
        self._broker_addr = f"ipc:///tmp/tertius-{vm_pid}-{vm_instance}-data.sock"
        self._ctrl_addr = f"ipc:///tmp/tertius-{vm_pid}-{vm_instance}-ctrl.sock"
        self._ctx: zmq.Context[zmq.Socket[bytes]] = zmq.Context()
        # vm_pid stands in for a port until the broker binds a TCP address
        node_id = make_node_id(socket.gethostname(), vm_pid)
        self._broker = Broker(self._broker_addr, self._ctrl_addr, self._ctx, scope, node_id)

    def start(
        self, fn: Callable[..., Any], args: tuple[Any, ...]
    ) -> Generator[Any, None, None]:
        threading.Thread(target=self._broker.run_data, daemon=True).start()
        threading.Thread(target=self._broker.run_control, daemon=True).start()
        self._broker.ready.wait()

        root_pid = self._broker.alloc_pid()
        ctx: zmq.Context[zmq.Socket[bytes]] = zmq.Context()

        dealer: zmq.Socket[bytes] = ctx.socket(zmq.DEALER)
        dealer.identity = bytes(root_pid)
        dealer.connect(self._broker_addr)

        ctrl: zmq.Socket[bytes] = ctx.socket(zmq.DEALER)
        ctrl.identity = bytes(root_pid)
        ctrl.connect(self._ctrl_addr)

        root_exc: list[BaseException] = []

        threading.Thread(
            target=partial(
                _root_thread, fn, args, root_pid, dealer, ctrl, ctx, root_exc,
                self._broker.emit_queue,
            ),
            daemon=True,
        ).start()

        while True:
            event = self._broker.emit_queue.get()
            if event is _DONE:
                if root_exc:
                    raise root_exc[0]
                return
            yield event


def run(
    fn: Callable[..., Any], *args: Any, scope: Scope | None = None
) -> Generator[Any, None, None]:
    yield from VM(scope or {}).start(fn, args)
