# Broker spawn handler — starts new OS processes and waits for them to signal readiness.
import multiprocessing
import time
from collections.abc import Callable
from multiprocessing.process import BaseProcess
from typing import Any

import zmq

from tertius.constants import OK, READY, SPAWN_READY_TIMEOUT_MS
from tertius.types import Pid, Scope
from tertius.vm.broker_state import BrokerState
from tertius.vm.broker_utils import reply
from tertius.vm.events import spawn_ready, spawn_started, spawn_timeout
from tertius.vm.messages import pid_reply, spawn
from tertius.vm.process import process_entry


_SPAWN_CTX = multiprocessing.get_context("spawn")


def _start_process(
    pid: Pid,
    fn_name: str,
    args: tuple[Any, ...],
    broker_addr: str,
    ctrl_addr: str,
    scope: Scope,
    state: BrokerState,
) -> BaseProcess:
    # Daemon=True so child processes don't outlive the broker if it exits uncleanly.
    # spawn (not fork) avoids inheriting the parent's ZMQ IO threads, which
    # causes libzmq to abort() when the child later calls zmq_msg_recv.
    proc = _SPAWN_CTX.Process(
        target=process_entry,
        args=(pid.node_id, pid.id, broker_addr, ctrl_addr, fn_name, args, scope),
        daemon=True,
    )
    proc.start()
    state.procs[pid] = proc
    return proc


def _await_ready(
    router: "zmq.Socket[bytes]",
    proc: BaseProcess,
    new_pid: Pid,
    fn_name: str,
    handlers: dict,
) -> None:
    """Block until the newly spawned process sends READY on the control socket.

    Other control messages that arrive while waiting are dispatched normally —
    the system can't simply pause while one process is starting up, since other
    live processes may be sending control messages concurrently.

    A 1s receive timeout lets us check if the child died without ever sending
    READY, which would otherwise block forever.
    """
    router.setsockopt(zmq.RCVTIMEO, SPAWN_READY_TIMEOUT_MS)

    try:
        while True:
            try:
                child_frames = router.recv_multipart()
            except zmq.Again:
                # Timed out waiting — check if the process is still alive before retrying.
                if not proc.is_alive():
                    raise RuntimeError(
                        f"ESpawn: process {fn_name!r} died before sending READY "
                        f"(exit code {proc.exitcode})"
                    )
                continue

            child_requester, child_command = child_frames[0], child_frames[1]

            if child_command == READY and child_requester == bytes(new_pid):
                # The child process is ready — reply to the original spawn requester and return.
                reply(router, child_requester, OK)
                break

            if child_command in handlers:
                handlers[child_command](router, child_requester, child_frames)
    finally:
        router.setsockopt(zmq.RCVTIMEO, -1)


def handle_spawn(
    alloc_pid: Callable[[], Pid],
    scope: Scope,
    broker_addr: str,
    ctrl_addr: str,
    state: BrokerState,
    handlers: dict,
    router: "zmq.Socket[bytes]",
    requester: bytes,
    frames: list[bytes],
) -> None:
    fn_name, args = spawn.decode(frames)

    if fn_name not in scope:
        raise KeyError(f"ESpawn: {fn_name!r} not in scope; available: {sorted(scope)}")

    new_pid = alloc_pid()
    spawn_start = time.time()
    proc = _start_process(new_pid, fn_name, args, broker_addr, ctrl_addr, scope, state)
    state.emit_queue.put(spawn_started(new_pid))

    try:
        # Block until the process is ready before replying to the caller, so the
        # caller can safely send to the new pid immediately after receiving its pid back.
        _await_ready(router, proc, new_pid, fn_name, handlers)
    except (RuntimeError, zmq.ZMQError):
        # ZMQError covers broker shutdown racing with _await_ready — still emit
        # the timeout event so the telemetry stream is always closed.
        state.emit_queue.put(spawn_timeout(new_pid, fn_name, proc.exitcode))
        raise

    state.emit_queue.put(spawn_ready(new_pid, spawn_start))
    reply(router, requester, *pid_reply.encode(new_pid))
