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
from tertius.types import Pid
from tertius.vm.broker_effects import BrokerCmd, decode_frame
from tertius.vm.broker_handlers import (
    handle_crash,
    handle_emit,
    handle_join,
    handle_kill,
    handle_link,
    handle_monitor,
    handle_register,
    handle_spawn,
    handle_whereis,
)
from tertius.vm.broker_state import BrokerState
from tertius.vm.broker_types import BrokerConfig, SpawnContext
from tertius.vm.broker_utils import reply
from tertius.vm.messages import frame_id
from tertius.vm.transport import make_notifier, make_router


def _is_shutdown_error(err: zmq.ZMQError) -> bool:
    # ctx.term() causes blocking recv/send to raise ETERM; ENOTSOCK is a fallback
    # if the socket is closed by the time we check. Both mean clean shutdown.
    return err.errno in (zmq.ETERM, zmq.ENOTSOCK)


def _terminate_procs(state: BrokerState) -> None:
    """Terminate all spawned workers. Thread workers stop at their next effect yield."""

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
    spawn_ctx: SpawnContext,
    state: BrokerState,
    notifier: "zmq.Socket[bytes]",
    router: "zmq.Socket[bytes]",
) -> ControlHandlers:
    return {
        "spawn_cmd": partial(handle_spawn, spawn_ctx, state, router),
        "register_cmd": partial(handle_register, state, router),
        "join_cmd": partial(handle_join, state, router),
        "whereis_cmd": partial(handle_whereis, state, router),
        "link_cmd": partial(handle_link, state, notifier, router),
        "monitor_cmd": partial(handle_monitor, state, notifier, router),
        "emit_cmd": partial(handle_emit, state, router),
        "kill_cmd": partial(handle_kill, state, notifier, router),
        "crash_cmd": partial(handle_crash, state, notifier, router),
    }


def recv_control_frames(router: "zmq.Socket[bytes]") -> list[bytes] | None:
    """Receive control frames; None means the broker is shutting down."""

    try:
        return router.recv_multipart()
    except zmq.ZMQError as err:
        if _is_shutdown_error(err):
            return None
        raise


def reply_error(router: "zmq.Socket[bytes]", requester: bytes, err: Exception) -> bool:
    """Send the handler's exception back to the caller rather than crashing the
    broker — one bad request shouldn't take down the VM. False on shutdown."""

    try:
        reply(router, requester, Cmd.ERROR, pickle.dumps(err))
        return True
    except zmq.ZMQError as zmq_err:
        if _is_shutdown_error(zmq_err):
            return False
        raise


def dispatch_frames(
    router: "zmq.Socket[bytes]", frames: list[bytes]
) -> Generator[BrokerCmd, Any, bool]:
    """Yield the decoded command effect for orbis to dispatch; False means shut down."""

    effect = decode_frame(frames)
    if effect is None:
        return True

    try:
        yield effect
    except zmq.ZMQError as err:
        if _is_shutdown_error(err):
            return False
        raise
    except Exception as err:  # noqa: BLE001
        return reply_error(router, frame_id(frames), err)

    return True


def broker_control_gen(router: "zmq.Socket[bytes]") -> Generator[BrokerCmd, Any, None]:
    """Drive the broker control loop as a generator.

    orbis dispatches each command effect to its handler.
    """

    while True:
        frames = recv_control_frames(router)
        if frames is None:
            return

        alive = yield from dispatch_frames(router, frames)
        if not alive:
            return


class Broker:
    """Central VM coordinator. Runs two sockets on separate threads:

    - relay_data: a dumb ROUTER that forwards messages between processes by
      address. Processes talk to each other through this without the broker
      needing to understand the content.

    - run_vm_control: a ROUTER that handles VM-level operations (spawn, kill,
      link, monitor, etc.). All state that crosses process boundaries lives
      here — process registry, links, monitors, the dead-process tombstones.
    """

    def __init__(self, config: BrokerConfig, ctx: "zmq.Context[zmq.Socket[bytes]]") -> None:
        self.config = config
        self._ctx = ctx
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
            pid = Pid(node_id=self.config.node_id, id=self._next_pid)
            self._next_pid += 1

            return pid

    def make_spawn_context(self) -> SpawnContext:
        """Bundle the spawn handler's configuration."""

        return SpawnContext(
            alloc_pid=self.alloc_pid,
            scope=self.config.scope,
            broker_addr=self.config.broker_addr,
            ctrl_addr=self.config.ctrl_addr,
            transport=self.config.transport,
            default_mode=self.config.spawn_mode,
        )

    def relay_data(self) -> None:
        """Blind message relay between processes.

        Processes address each other directly by pid. The broker just forwards
        frames without inspecting the body.
        """

        router = make_router(self._ctx, self.config.broker_addr, self.config.transport)
        self.ready.set()
        try:
            _run_relay_data_loop(router)
        finally:
            router.close()

    def run_vm_control(self) -> None:
        """Serialised handler loop for all VM control operations.

        Single-threaded by design: all mutable VM state (names, links,
        monitors, dead, procs) is touched exclusively here, so no locking
        is needed beyond alloc_pid.
        """
        # Wait until the data broker is bound so the notifier can connect.
        self.ready.wait()

        notifier = make_notifier(self._ctx, self.config.broker_addr, self.config.transport)
        router = make_router(self._ctx, self.config.ctrl_addr, self.config.transport)
        handlers = make_control_handlers(self.make_spawn_context(), self._state, notifier, router)

        try:
            complete(broker_control_gen(router), **handlers)
        finally:
            router.close()
            notifier.close()

    def stop(self) -> None:
        """Terminate all spawned workers then shut down the broker context.

        Child processes are terminated before ctx.term() so they don't hold
        IPC connections open while the sockets are being torn down.
        """
        _terminate_procs(self._state)
        self._ctx.term()
