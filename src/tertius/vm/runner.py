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

from tertius.constants import SpawnMode
from tertius.transport_types import IpcTransport, Transport
from tertius.types import Scope
from tertius.vm.broker import Broker
from tertius.vm.broker_types import BrokerConfig
from tertius.vm.process_handlers import make_handlers
from tertius.vm.runner_types import RootSlot, RootWiring, VmOptions
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
    wiring: RootWiring,
    slot: RootSlot,
) -> None:
    """Root process thread: run the function and record its outcome in the slot."""

    try:
        handlers = make_handlers(wiring.pid, wiring.dealer, wiring.ctrl, None)
        slot.result = complete(fn(*args), **handlers)
    except Exception as err:  # noqa: BLE001
        slot.exc = err
    finally:
        wiring.dealer.close()
        wiring.ctrl.close()
        wiring.ctx.term()
        wiring.emit_queue.put(_DONE)


def _start_broker(broker: Broker) -> None:
    """Start the broker's data and control threads."""

    threading.Thread(target=broker.relay_data, daemon=True).start()
    threading.Thread(target=broker.run_vm_control, daemon=True).start()
    broker.ready.wait()


def make_broker(scope: Scope, options: VmOptions) -> Broker:
    """Build a broker with fresh addresses and its own ZMQ context."""

    vm_pid = os.getpid()
    node_id = make_node_id(socket.gethostname(), vm_pid)
    broker_addr, ctrl_addr = build_addresses(options.transport, vm_pid, _vm_id())
    config = BrokerConfig(
        broker_addr=broker_addr,
        ctrl_addr=ctrl_addr,
        scope=scope,
        node_id=node_id,
        transport=options.transport,
        spawn_mode=options.spawn_mode,
    )
    ctx: zmq.Context[zmq.Socket[bytes]] = zmq.Context()
    return Broker(config, ctx)


def start_root(fn: Callable[..., Any], args: tuple[Any, ...], broker: Broker) -> RootSlot:
    """Connect the root process's sockets and run it on its own thread."""

    root_pid = broker.alloc_pid()
    root_ctx: zmq.Context[zmq.Socket[bytes]] = zmq.Context()
    dealer = make_dealer(root_ctx, root_pid, broker.config.broker_addr, broker.config.transport)
    ctrl = make_dealer(root_ctx, root_pid, broker.config.ctrl_addr, broker.config.transport)
    wiring = RootWiring(
        pid=root_pid, dealer=dealer, ctrl=ctrl, ctx=root_ctx, emit_queue=broker.emit_queue
    )
    slot = RootSlot()

    threading.Thread(target=partial(_root_thread, fn, args, wiring, slot), daemon=True).start()
    return slot


def vm_run(
    fn: Callable[..., Any],
    args: tuple[Any, ...],
    scope: Scope,
    options: VmOptions,
) -> Generator[Any, None, Any]:
    """Run the function in the Tertius runtime."""

    broker = make_broker(scope, options)
    _start_broker(broker)
    slot = start_root(fn, args, broker)

    try:
        yield from iter(broker.emit_queue.get, _DONE)

        if slot.exc is not None:
            raise slot.exc
    finally:
        # Stop the broker before declaring done — it may emit spawn_timeout
        # events during shutdown that must be captured before collect returns.
        broker.stop()

    yield from _drain_queue(broker.emit_queue)
    return slot.result


def run(
    fn: Callable[..., Any],
    *args: Any,
    scope: Scope | None = None,
    transport: Transport | None = None,
    spawn_mode: SpawnMode = SpawnMode.PROCESS,
) -> Generator[Any, None, Any]:
    options = VmOptions(transport=transport or IpcTransport(), spawn_mode=spawn_mode)
    return (yield from vm_run(fn, args, scope or {}, options))
