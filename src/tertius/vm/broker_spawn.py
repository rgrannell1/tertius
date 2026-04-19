import multiprocessing
from collections.abc import Callable
from functools import partial
from typing import Any

import zmq

from tertius.constants import OK, READY
from tertius.types import Pid
from tertius.vm.messages import decode_spawn, encode_pid_reply
from tertius.vm.process import process_entry

Scope = dict[str, Callable[..., Any]]


def _start_process(
    pid: Pid,
    fn_name: str,
    args: tuple[Any, ...],
    broker_addr: str,
    ctrl_addr: str,
    scope: Scope,
    procs: dict[Pid, multiprocessing.Process],
) -> multiprocessing.Process:
    proc = multiprocessing.Process(
        target=process_entry,
        args=(pid.id, broker_addr, ctrl_addr, fn_name, args, scope),
        daemon=True,
    )
    proc.start()
    procs[pid] = proc
    return proc


def _await_ready(
    router: "zmq.Socket[bytes]",
    proc: multiprocessing.Process,
    new_pid: Pid,
    fn_name: str,
    handlers: dict,
) -> None:
    # Drain ctrl messages until READY arrives from new_pid.
    # Other messages that arrive in the meantime are dispatched normally.
    router.setsockopt(zmq.RCVTIMEO, 1000)
    try:
        while True:
            try:
                child_frames = router.recv_multipart()
            except zmq.Again:
                if not proc.is_alive():
                    raise RuntimeError(
                        f"ESpawn: process {fn_name!r} died before sending READY "
                        f"(exit code {proc.exitcode})"
                    )
                continue
            child_requester, child_command = child_frames[0], child_frames[1]
            if child_command == READY and child_requester == bytes(new_pid):
                router.send_multipart([child_requester, OK])
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
    procs: dict[Pid, multiprocessing.Process],
    handlers: dict,
    router: "zmq.Socket[bytes]",
    requester: bytes,
    frames: list[bytes],
) -> None:
    fn_name, args = decode_spawn(frames)

    if fn_name not in scope:
        raise KeyError(f"ESpawn: {fn_name!r} not in scope; available: {sorted(scope)}")

    new_pid = alloc_pid()
    proc = _start_process(new_pid, fn_name, args, broker_addr, ctrl_addr, scope, procs)
    _await_ready(router, proc, new_pid, fn_name, handlers)
    router.send_multipart([requester] + encode_pid_reply(new_pid))
