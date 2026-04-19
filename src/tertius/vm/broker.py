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
    READY,
    REGISTER,
    SPAWN,
    WHEREIS,
)
from tertius.exceptions import LinkedCrash, ProcessCrash
from tertius.types import Pid
from tertius.vm.messages import (
    decode_crash,
    decode_emit,
    decode_kill,
    decode_link,
    decode_monitor,
    decode_register,
    decode_spawn,
    decode_whereis,
    encode_crash_notification,
    encode_linked_crash_notification,
    encode_pid_reply,
    encode_whereis_reply,
)
from tertius.vm.process import process_entry

Scope = dict[str, Callable[..., Any]]


# ---------------------------------------------------------------------------
# Spawn helpers
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Crash notification helpers
# ---------------------------------------------------------------------------


def _notify_monitors(
    monitors: dict[Pid, list[Pid]],
    notifier: "zmq.Socket[bytes]",
    pid: Pid,
    reason: Exception,
) -> None:
    crash_msg = ProcessCrash(pid=pid, reason=reason)

    for watcher in monitors.pop(pid, []):
        notifier.send_multipart(encode_crash_notification(watcher, pid, crash_msg))


def _notify_links(
    links: dict[Pid, list[Pid]],
    notifier: "zmq.Socket[bytes]",
    pid: Pid,
    reason: Exception,
) -> None:
    kill_msg = LinkedCrash(pid=pid, reason=reason)

    for peer in links.pop(pid, []):
        if peer in links:
            links[peer] = [linked for linked in links[peer] if linked != pid]
        notifier.send_multipart(encode_linked_crash_notification(peer, pid, kill_msg))


def _record_crash(
    names: dict[str, Pid],
    monitors: dict[Pid, list[Pid]],
    links: dict[Pid, list[Pid]],
    dead: dict[Pid, Exception],
    notifier: "zmq.Socket[bytes]",
    pid: Pid,
    reason: Exception,
) -> None:
    dead[pid] = reason

    for name in [n for n, owner in names.items() if owner == pid]:
        del names[name]

    _notify_monitors(monitors, notifier, pid, reason)
    _notify_links(links, notifier, pid, reason)


# ---------------------------------------------------------------------------
# Control message handlers
# ---------------------------------------------------------------------------


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


def handle_register(
    names: dict[str, Pid],
    router: "zmq.Socket[bytes]",
    requester: bytes,
    frames: list[bytes],
) -> None:
    names[decode_register(frames)] = Pid.from_bytes(requester)
    router.send_multipart([requester, OK])


def handle_whereis(
    names: dict[str, Pid],
    router: "zmq.Socket[bytes]",
    requester: bytes,
    frames: list[bytes],
) -> None:
    pid = names.get(decode_whereis(frames))
    router.send_multipart([requester] + encode_whereis_reply(pid))


def handle_link(
    links: dict[Pid, list[Pid]],
    dead: dict[Pid, Exception],
    notifier: "zmq.Socket[bytes]",
    router: "zmq.Socket[bytes]",
    requester: bytes,
    frames: list[bytes],
) -> None:
    requester_pid = Pid.from_bytes(requester)
    target_pid = decode_link(frames)
    router.send_multipart([requester, OK])

    if target_pid in dead:
        kill_msg = LinkedCrash(pid=target_pid, reason=dead[target_pid])
        notifier.send_multipart(
            encode_linked_crash_notification(requester_pid, target_pid, kill_msg)
        )
        return

    links.setdefault(requester_pid, []).append(target_pid)
    links.setdefault(target_pid, []).append(requester_pid)


def handle_monitor(
    monitors: dict[Pid, list[Pid]],
    dead: dict[Pid, Exception],
    notifier: "zmq.Socket[bytes]",
    router: "zmq.Socket[bytes]",
    requester: bytes,
    frames: list[bytes],
) -> None:

    target_pid = decode_monitor(frames)
    requester_pid = Pid.from_bytes(requester)
    router.send_multipart([requester, OK])

    if target_pid in dead:
        crash_msg = ProcessCrash(pid=target_pid, reason=dead[target_pid])
        notifier.send_multipart(
            encode_crash_notification(requester_pid, target_pid, crash_msg)
        )
        return

    monitors.setdefault(target_pid, []).append(requester_pid)


def handle_emit(
    emit_queue: "queue.Queue[Any]",
    router: "zmq.Socket[bytes]",
    requester: bytes,
    frames: list[bytes],
) -> None:

    emit_queue.put(decode_emit(frames))
    router.send_multipart([requester, OK])


def handle_kill(
    procs: dict[Pid, multiprocessing.Process],
    names: dict[str, Pid],
    monitors: dict[Pid, list[Pid]],
    links: dict[Pid, list[Pid]],
    dead: dict[Pid, Exception],
    notifier: "zmq.Socket[bytes]",
    router: "zmq.Socket[bytes]",
    requester: bytes,
    frames: list[bytes],
) -> None:

    target_pid = decode_kill(frames)
    router.send_multipart([requester, OK])

    if target_pid in dead:
        return
    proc = procs.pop(target_pid, None)

    if proc is not None:
        proc.terminate()

    _record_crash(names, monitors, links, dead, notifier, target_pid, RuntimeError("killed"))


def handle_crash(
    names: dict[str, Pid],
    monitors: dict[Pid, list[Pid]],
    links: dict[Pid, list[Pid]],
    dead: dict[Pid, Exception],
    notifier: "zmq.Socket[bytes]",
    router: "zmq.Socket[bytes]",
    requester: bytes,
    frames: list[bytes],
) -> None:

    crashed_pid = Pid.from_bytes(requester)
    reason = decode_crash(frames)

    _record_crash(names, monitors, links, dead, notifier, crashed_pid, reason)
    router.send_multipart([requester, OK])


# ---------------------------------------------------------------------------
# Broker
# ---------------------------------------------------------------------------


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
