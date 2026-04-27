# Central VM broker — routes inter-process messages and handles all control commands.
import pickle
import queue
import threading
from collections.abc import Callable
from functools import partial
from typing import Any

import zmq

from tertius.constants import Cmd
from tertius.types import Pid, Scope
from tertius.vm.broker_crash import handle_crash, handle_kill
from tertius.vm.broker_handlers import (
    handle_emit,
    handle_link,
    handle_monitor,
    handle_register,
    handle_whereis,
)
from tertius.vm.broker_spawn import handle_spawn
from tertius.vm.broker_state import BrokerState
from tertius.vm.broker_utils import reply


def _is_shutdown_error(err: zmq.ZMQError) -> bool:
    # ctx.term() causes blocking recv/send to raise ETERM; ENOTSOCK is a fallback
    # if the socket is closed by the time we check. Both mean clean shutdown.
    return err.errno in (zmq.ETERM, zmq.ENOTSOCK)


def _make_router(
    ctx: "zmq.Context[zmq.Socket[bytes]]", addr: str
) -> "zmq.Socket[bytes]":
    router: zmq.Socket[bytes] = ctx.socket(zmq.ROUTER)
    router.setsockopt(zmq.LINGER, 0)
    router.bind(addr)
    return router


def _make_notifier(
    ctx: "zmq.Context[zmq.Socket[bytes]]", broker_addr: str
) -> "zmq.Socket[bytes]":
    notifier: zmq.Socket[bytes] = ctx.socket(zmq.DEALER)
    notifier.setsockopt(zmq.LINGER, 0)
    notifier.identity = b"vm-notifier"
    notifier.connect(broker_addr)
    return notifier


def _terminate_procs(state: BrokerState) -> None:
    for proc in state.procs.values():
        if proc.is_alive():
            proc.terminate()


def _run_data_loop(router: "zmq.Socket[bytes]") -> None:
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


def _dispatch_command(
    router: "zmq.Socket[bytes]",
    requester: bytes,
    frames: list[bytes],
    handlers: dict[bytes, Callable[..., None]],
) -> bool:
    """Dispatch one control command. Returns False if the loop should exit."""
    command = frames[1]
    if command not in handlers:
        return True
    try:
        handlers[command](router, requester, frames)
    except zmq.ZMQError as err:
        if _is_shutdown_error(err):
            return False
        raise
    except Exception as err:  # noqa: BLE001
        # Send the exception back to the caller rather than crashing
        # the broker — one bad request shouldn't take down the VM.
        try:
            reply(router, requester, Cmd.ERROR, pickle.dumps(err))
        except zmq.ZMQError as zmq_err:
            if _is_shutdown_error(zmq_err):
                return False
            raise
    return True


def _run_ctrl_loop(
    router: "zmq.Socket[bytes]",
    handlers: dict[bytes, Callable[..., None]],
) -> None:
    while True:
        try:
            frames = router.recv_multipart()
        except zmq.ZMQError as err:
            if _is_shutdown_error(err):
                return
            raise
        requester = frames[0]
        if not _dispatch_command(router, requester, frames, handlers):
            return


def make_ctrl_handlers(
    alloc_pid: Callable[[], Pid],
    scope: Scope,
    broker_addr: str,
    ctrl_addr: str,
    state: BrokerState,
    notifier: "zmq.Socket[bytes]",
) -> dict[bytes, Callable[..., None]]:
    """Factory for control command handlers."""

    handlers: dict[bytes, Callable[..., None]] = {}
    handlers[Cmd.SPAWN] = partial(
        handle_spawn, alloc_pid, scope, broker_addr, ctrl_addr, state, handlers
    )
    handlers[Cmd.REGISTER] = partial(handle_register, state)
    handlers[Cmd.WHEREIS] = partial(handle_whereis, state)
    handlers[Cmd.LINK] = partial(handle_link, state, notifier)
    handlers[Cmd.MONITOR] = partial(handle_monitor, state, notifier)
    handlers[Cmd.EMIT] = partial(handle_emit, state)
    handlers[Cmd.KILL] = partial(handle_kill, state, notifier)
    handlers[Cmd.CRASH] = partial(handle_crash, state, notifier)

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
        self,
        broker_addr: str,
        ctrl_addr: str,
        ctx: "zmq.Context[zmq.Socket[bytes]]",
        scope: Scope,
        node_id: int,
    ) -> None:
        self._broker_addr = broker_addr
        self._ctrl_addr = ctrl_addr
        self._ctx = ctx
        self._scope = scope
        self._node_id = node_id
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
            pid = Pid(node_id=self._node_id, id=self._next_pid)
            self._next_pid += 1
            return pid

    def stop(self) -> None:
        """Terminate all spawned processes then shut down the broker context.

        Child processes are terminated before ctx.term() so they don't hold
        IPC connections open while the sockets are being torn down.

        ctx.term() (not ctx.destroy) is used because destroy() closes sockets
        from the calling thread while broker threads may be inside recv_multipart —
        libzmq 5.2.5 has an internal assertion that fires in that case. term()
        instead signals ETERM to all blocking recv calls, letting each thread
        close its own sockets cleanly before term() returns.
        """
        _terminate_procs(self._state)
        self._ctx.term()

    def run_data(self) -> None:
        """Blind message relay between processes.

        Processes address each other directly by pid. The broker just forwards
        frames without inspecting the body — keeping latency low and the data
        path free of VM logic.
        """
        router = _make_router(self._ctx, self._broker_addr)
        self.ready.set()
        try:
            _run_data_loop(router)
        finally:
            router.close()

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

        notifier = _make_notifier(self._ctx, self._broker_addr)
        router = _make_router(self._ctx, self._ctrl_addr)
        handlers = make_ctrl_handlers(
            self.alloc_pid,
            self._scope,
            self._broker_addr,
            self._ctrl_addr,
            self._state,
            notifier,
        )

        try:
            _run_ctrl_loop(router, handlers)
        finally:
            router.close()
            notifier.close()
