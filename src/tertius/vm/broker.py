import multiprocessing
import threading
from collections.abc import Callable
from typing import Any

import zmq

from tertius.constants import CRASH, LINK, MONITOR, OK, READY, REGISTER, SPAWN, WHEREIS
from tertius.exceptions import LinkedCrash, ProcessCrash
from tertius.types import Pid
from tertius.vm.messages import (
    decode_crash,
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
        self._procs: list[multiprocessing.Process] = []
        self.ready = threading.Event()

    def alloc_pid(self) -> Pid:
        with self._pid_lock:
            pid = Pid(self._next_pid)
            self._next_pid += 1
            return pid

    def run_data(self) -> None:
        """Route ESend messages between processes."""
        router: zmq.Socket[bytes] = self._ctx.socket(zmq.ROUTER)
        router.bind(self._broker_addr)
        self.ready.set()

        while True:
            _sender, target, sender_pid, body = router.recv_multipart()
            router.send_multipart([target, sender_pid, body])

    def _handle_spawn(
        self,
        router: "zmq.Socket[bytes]",
        requester: bytes,
        frames: list[bytes],
        handlers: dict,
    ) -> None:
        """Spawn a new process and wait for it to signal readiness before returning its pid."""
        fn_name, args = decode_spawn(frames)
        if fn_name not in self._scope:
            raise KeyError(f"ESpawn: {fn_name!r} not in scope; available: {sorted(self._scope)}")
        new_pid = self.alloc_pid()
        proc = multiprocessing.Process(
            target=process_entry,
            args=(new_pid.id, self._broker_addr, self._ctrl_addr, fn_name, args, self._scope),
            daemon=True,
        )
        proc.start()
        self._procs.append(proc)

        # Drain ctrl messages until READY arrives from new_pid.
        # Other messages that arrive in the meantime are dispatched normally.
        while True:
            child_frames = router.recv_multipart()
            child_requester, child_command = child_frames[0], child_frames[1]
            if child_command == READY and child_requester == bytes(new_pid):
                router.send_multipart([child_requester, OK])
                break
            if child_command in handlers:
                handlers[child_command](router, child_requester, child_frames)

        router.send_multipart([requester] + encode_pid_reply(new_pid))

    def _handle_register(
        self, router: "zmq.Socket[bytes]", requester: bytes, frames: list[bytes]
    ) -> None:
        """Register a process name"""
        self._names[decode_register(frames)] = Pid.from_bytes(requester)
        router.send_multipart([requester, OK])

    def _handle_whereis(
        self, router: "zmq.Socket[bytes]", requester: bytes, frames: list[bytes]
    ) -> None:
        """Lookup a process by name"""
        pid = self._names.get(decode_whereis(frames))
        router.send_multipart([requester] + encode_whereis_reply(pid))

    def _handle_link(
        self, router: "zmq.Socket[bytes]", requester: bytes, frames: list[bytes], notifier: "zmq.Socket[bytes]"
    ) -> None:
        """Register a bidirectional link; immediately kill requester if target is already dead."""
        requester_pid = Pid.from_bytes(requester)
        target_pid = decode_link(frames)
        router.send_multipart([requester, OK])
        if target_pid in self._dead:
            kill_msg = LinkedCrash(pid=target_pid, reason=self._dead[target_pid])
            notifier.send_multipart(encode_linked_crash_notification(requester_pid, target_pid, kill_msg))
            return
        self._links.setdefault(requester_pid, []).append(target_pid)
        self._links.setdefault(target_pid, []).append(requester_pid)

    def _handle_monitor(
        self, router: "zmq.Socket[bytes]", requester: bytes, frames: list[bytes], notifier: "zmq.Socket[bytes]"
    ) -> None:
        """Monitor a process for crashes; immediately notify if target is already dead."""
        target_pid = decode_monitor(frames)
        requester_pid = Pid.from_bytes(requester)
        router.send_multipart([requester, OK])
        if target_pid in self._dead:
            crash_msg = ProcessCrash(pid=target_pid, reason=self._dead[target_pid])
            notifier.send_multipart(encode_crash_notification(requester_pid, target_pid, crash_msg))
            return
        self._monitors.setdefault(target_pid, []).append(requester_pid)

    def _handle_crash(
        self,
        router: "zmq.Socket[bytes]",
        requester: bytes,
        frames: list[bytes],
        notifier: "zmq.Socket[bytes]",
    ) -> None:
        """Deliver a ProcessCrash message to all monitors of the crashed process."""
        crashed_pid = Pid.from_bytes(requester)
        reason = decode_crash(frames)
        self._dead[crashed_pid] = reason
        watchers = self._monitors.pop(crashed_pid, [])
        linked = self._links.pop(crashed_pid, [])
        self._names = {k: v for k, v in self._names.items() if v != crashed_pid}

        crash_msg = ProcessCrash(pid=crashed_pid, reason=reason)
        for watcher in watchers:
            notifier.send_multipart(encode_crash_notification(watcher, crashed_pid, crash_msg))

        kill_msg = LinkedCrash(pid=crashed_pid, reason=reason)
        for peer in linked:
            if peer in self._links:
                self._links[peer] = [p for p in self._links[peer] if p != crashed_pid]
            notifier.send_multipart(encode_linked_crash_notification(peer, crashed_pid, kill_msg))

        router.send_multipart([requester, OK])

    def run_control(self) -> None:
        """Handle ESpawn, ERegister, EWhereis, EMonitor, CRASH from processes."""
        self.ready.wait()

        # Injects crash notifications into the data broker on behalf of crashed processes
        notifier: zmq.Socket[bytes] = self._ctx.socket(zmq.DEALER)
        notifier.identity = b"vm-notifier"
        notifier.connect(self._broker_addr)

        router: zmq.Socket[bytes] = self._ctx.socket(zmq.ROUTER)
        router.bind(self._ctrl_addr)

        handlers = {
            SPAWN: lambda router, requester, frames: self._handle_spawn(
                router, requester, frames, handlers
            ),
            LINK: lambda router, requester, frames: self._handle_link(
                router, requester, frames, notifier
            ),
            REGISTER: lambda router, requester, frames: self._handle_register(
                router, requester, frames
            ),
            WHEREIS: lambda router, requester, frames: self._handle_whereis(
                router, requester, frames
            ),
            MONITOR: lambda router, requester, frames: self._handle_monitor(
                router, requester, frames, notifier
            ),
            CRASH: lambda router, requester, frames: self._handle_crash(
                router, requester, frames, notifier
            ),
        }

        while True:
            frames = router.recv_multipart()
            requester = frames[0]
            command = frames[1]

            if command in handlers:
                handlers[command](router, requester, frames)
