"""VM entry point — wires the broker, process threads, and effect handlers together."""

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

from tertius.transport_types import IpcTransport, Transport
from tertius.types import Pid, Scope
from tertius.vm.broker import Broker
from tertius.vm.process_handlers import make_handlers
from tertius.vm.transport import build_addresses, make_dealer


def make_node_id(host: str, port: int) -> int:
    """Derive a 4-byte node identifier from a host and port."""

    digest = hashlib.sha256(f"{host}:{port}".encode()).digest()
    return int.from_bytes(digest[:4], "big")

_DONE = object()
_vm_id = itertools.count().__next__


def _drain_queue(emit_queue: "queue.Queue[Any]") -> Generator[Any, None, None]:
    """Yield all events left in the emit queue after broker shutdown."""

    while True:
        try:
            yield emit_queue.get_nowait()
        except queue.Empty:
            return


def _root_thread(
    fn: Callable[..., Any],
    args: tuple[Any, ...],
    root_pid: Pid,
    dealer: "zmq.Socket[bytes]",
    ctrl: "zmq.Socket[bytes]",
    ctx: "zmq.Context[zmq.Socket[bytes]]",
    root_exc: list[BaseException],
    root_result: list[Any],
    emit_queue: "queue.Queue[Any]",
) -> None:
    """Root process thread: run the function and handle exceptions."""

    try:
        result = complete(fn(*args), **make_handlers(root_pid, dealer, ctrl))
        root_result.append(result)
    except Exception as err:  # noqa: BLE001
        root_exc.append(err)
    finally:
        dealer.close()
        ctrl.close()
        ctx.term()
        emit_queue.put(_DONE)


def _start_broker(broker: Broker) -> None:
    """Start the broker's data and control threads."""

    threading.Thread(target=broker.relay_data, daemon=True).start()
    threading.Thread(target=broker.run_vm_control, daemon=True).start()
    broker.ready.wait()


def vm_run(
    fn: Callable[..., Any],
    args: tuple[Any, ...],
    scope: Scope,
    transport: Transport | None = None,
) -> Generator[Any, None, Any]:
    """Run the function in the Tertius runtime."""

    selected_transport = transport or IpcTransport()
    vm_pid = os.getpid()
    node_id = make_node_id(socket.gethostname(), vm_pid)
    broker_addr, ctrl_addr = build_addresses(selected_transport, vm_pid, _vm_id())
    ctx: zmq.Context[zmq.Socket[bytes]] = zmq.Context()
    broker = Broker(broker_addr, ctrl_addr, ctx, scope, node_id, selected_transport)

    _start_broker(broker)

    root_pid = broker.alloc_pid()
    root_ctx: zmq.Context[zmq.Socket[bytes]] = zmq.Context()

    dealer = make_dealer(root_ctx, root_pid, broker_addr, selected_transport)
    ctrl = make_dealer(root_ctx, root_pid, ctrl_addr, selected_transport)

    root_exc: list[BaseException] = []
    root_result: list[Any] = []

    threading.Thread(
        target=partial(
            _root_thread, fn, args, root_pid, dealer, ctrl, root_ctx, root_exc, root_result,
            broker.emit_queue,
        ),
        daemon=True,
    ).start()

    broker_stopped = False
    try:
        yield from iter(broker.emit_queue.get, _DONE)

        if root_exc:
            raise root_exc[0]

        # Stop the broker before declaring done — it may emit spawn_timeout
        # events during shutdown that must be captured before collect returns.
        broker.stop()
        broker_stopped = True

        yield from _drain_queue(broker.emit_queue)
        return root_result[0] if root_result else None

    finally:
        if not broker_stopped:
            broker.stop()


def run(
    fn: Callable[..., Any],
    *args: Any,
    scope: Scope | None = None,
    transport: Transport | None = None,
) -> Generator[Any, None, Any]:
    return (yield from vm_run(fn, args, scope or {}, transport))
