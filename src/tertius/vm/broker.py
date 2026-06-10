"""Central VM broker — routes inter-process messages and handles all control commands."""

import pickle
import queue
import threading
from collections.abc import Generator
from functools import partial
from typing import Any

import zmq
from orbis import complete

from tertius.constants import Cmd
from tertius.transport_types import Transport
from tertius.types import Pid, Scope
from tertius.vm.broker_effects import BrokerCmd, decode_frame
from tertius.vm.broker_handlers import (
    handle_crash,
    handle_emit,
    handle_kill,
    handle_link,
    handle_monitor,
    handle_register,
    handle_spawn,
    handle_whereis,
)
from tertius.vm.broker_state import BrokerState
from tertius.vm.broker_utils import reply
from tertius.vm.messages import frame_id
from tertius.vm.transport import make_notifier, make_router


def _is_shutdown_error(err: zmq.ZMQError) -> bool:
    # ctx.term() causes blocking recv/send to raise ETERM; ENOTSOCK is a fallback
    # if the socket is closed by the time we check. Both mean clean shutdown.
    return err.errno in (zmq.ETERM, zmq.ENOTSOCK)


def _terminate_procs(state: BrokerState) -> None:
    """Terminate all spawned processes."""

    for proc in state.procs.values():
        if proc.is_alive():
            proc.terminate()


def _run_relay_data_loop(router: "zmq.Socket[bytes]") -> None:
    """Forward messages between processes."""

    while True:
        try:
            _sender, target, sender_pid, body = router.recv_multipart()
        except zmq.ZMQError as err:
            if _is_shutdown_error(err):
                return
            raise
        try:
            router.send_multipart([target, sender_pid, body])
        except zmq.ZMQError as err:
            if _is_shutdown_error(err):
                return
            raise


type ControlHandlers = dict[str, Any]


def make_control_handlers(
    alloc_pid: Any,
    scope: Scope,
    broker_addr: str,
    ctrl_addr: str,
    state: BrokerState,
    notifier: "zmq.Socket[bytes]",
    router: "zmq.Socket[bytes]",
    transport: Transport,
) -> ControlHandlers:
    return {
        "spawn_cmd": partial(
            handle_spawn, alloc_pid, scope, broker_addr, ctrl_addr, state, router, transport
        ),
        "register_cmd": partial(handle_register, state, router),
        "whereis_cmd": partial(handle_whereis, state, router),
        "link_cmd": partial(handle_link, state, notifier, router),
        "monitor_cmd": partial(handle_monitor, state, notifier, router),
        "emit_cmd": partial(handle_emit, state, router),
        "kill_cmd": partial(handle_kill, state, notifier, router),
        "crash_cmd": partial(handle_crash, state, notifier, router),
    }


def broker_control_gen(router: "zmq.Socket[bytes]") -> Generator[BrokerCmd, Any, None]:
    """Drive the broker control loop as a generator.

    orbis dispatches each command effect to its handler.
    """

    while True:
        try:
            frames = router.recv_multipart()
        except zmq.ZMQError as err:
            if _is_shutdown_error(err):
                return
            raise

        requester = frame_id(frames)
        effect = decode_frame(frames)

        if effect is None:
            continue

        try:
            yield effect
        except zmq.ZMQError as err:
            if _is_shutdown_error(err):
                return
            raise
        except Exception as err:  # noqa: BLE001
            # Send the exception back to the caller rather than crashing
            # the broker — one bad request shouldn't take down the VM.
            try:
                reply(router, requester, Cmd.ERROR, pickle.dumps(err))
            except zmq.ZMQError as zmq_err:
                if _is_shutdown_error(zmq_err):
                    return
                raise


class Broker:
    """Central VM coordinator. Runs two sockets on separate threads:

    - relay_data: a dumb ROUTER that forwards messages between processes by
      address. Processes talk to each other through this without the broker
      needing to understand the content.

    - run_vm_control: a ROUTER that handles VM-level operations (spawn, kill,
      link, monitor, etc.). All state that crosses process boundaries lives
      here — process registry, links, monitors, the dead-process tombstones.
    """

    def __init__(
        self,
        broker_addr: str,
        ctrl_addr: str,
        ctx: "zmq.Context[zmq.Socket[bytes]]",
        scope: Scope,
        node_id: int,
        transport: Transport,
    ) -> None:
        self._broker_addr = broker_addr
        self._ctrl_addr = ctrl_addr
        self._ctx = ctx
        self._scope = scope
        self._node_id = node_id
        self._transport = transport
        self._next_pid = 0
        self._pid_lock = threading.Lock()
        self._state = BrokerState()
        self.ready = threading.Event()  # signals that run_data is bound and accepting

    @property
    def emit_queue(self) -> "queue.Queue[Any]":
        """for telemetry communication"""

        return self._state.emit_queue

    def alloc_pid(self) -> Pid:
        """Allocate a new PID for a new process."""

        with self._pid_lock:
            pid = Pid(node_id=self._node_id, id=self._next_pid)
            self._next_pid += 1

            return pid

    def relay_data(self) -> None:
        """Blind message relay between processes.

        Processes address each other directly by pid. The broker just forwards
        frames without inspecting the body.
        """

        router = make_router(self._ctx, self._broker_addr, self._transport)
        self.ready.set()
        try:
            _run_relay_data_loop(router)
        finally:
            router.close()

    def run_vm_control(self) -> None:
        """Serialised handler loop for all VM control operations.

        Single-threaded by design: all mutable VM state (_names, _links,
        _monitors, _dead, _procs) is touched exclusively here, so no locking
        is needed beyond alloc_pid.
        """
        # Wait until the data broker is bound so the notifier can connect.
        self.ready.wait()

        notifier = make_notifier(self._ctx, self._broker_addr, self._transport)
        router = make_router(self._ctx, self._ctrl_addr, self._transport)

        alloc_pid = self.alloc_pid
        state = self._state
        broker_addr = self._broker_addr
        ctrl_addr = self._ctrl_addr
        handlers = make_control_handlers(
            alloc_pid,
            self._scope,
            broker_addr,
            ctrl_addr,
            state,
            notifier,
            router,
            self._transport,
        )

        try:
            complete(broker_control_gen(router), **handlers)
        finally:
            router.close()
            notifier.close()

    def stop(self) -> None:
        """Terminate all spawned processes then shut down the broker context.

        Child processes are terminated before ctx.term() so they don't hold
        IPC connections open while the sockets are being torn down.
        """
        _terminate_procs(self._state)
        self._ctx.term()
