import os
import threading
from collections.abc import Callable
from typing import Any

import zmq
from orbis import complete

from tertius.vm.broker import Broker
from tertius.vm.process import make_handlers

Scope = dict[str, Callable[..., Any]]


class VM:
    def __init__(self, scope: Scope) -> None:
        vm_pid = os.getpid()
        self._broker_addr = f"ipc:///tmp/tertius-{vm_pid}-data.sock"
        self._ctrl_addr = f"ipc:///tmp/tertius-{vm_pid}-ctrl.sock"
        self._ctx: zmq.Context[bytes] = zmq.Context()
        self._broker = Broker(self._broker_addr, self._ctrl_addr, self._ctx, scope)

    def start(self, fn: Callable[..., Any], args: tuple[Any, ...]) -> Any:
        threading.Thread(target=self._broker.run_data, daemon=True).start()
        threading.Thread(target=self._broker.run_control, daemon=True).start()
        self._broker.ready.wait()

        root_pid = self._broker.alloc_pid()
        ctx: zmq.Context[bytes] = zmq.Context()

        dealer: zmq.Socket[bytes] = ctx.socket(zmq.DEALER)
        dealer.identity = bytes(root_pid)
        dealer.connect(self._broker_addr)

        ctrl: zmq.Socket[bytes] = ctx.socket(zmq.DEALER)
        ctrl.identity = bytes(root_pid)
        ctrl.connect(self._ctrl_addr)

        try:
            return complete(fn(*args), **make_handlers(root_pid, dealer, ctrl))
        finally:
            dealer.close()
            ctrl.close()
            ctx.term()


def run(fn: Callable[..., Any], *args: Any, scope: Scope | None = None) -> Any:
    return VM(scope or {}).start(fn, args)
