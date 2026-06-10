"""All broker command handlers — spawn, register, whereis, link, monitor, emit, kill, crash."""

import multiprocessing
import pickle
import time
from collections.abc import Callable, Generator
from multiprocessing.process import BaseProcess
from typing import Any

import zmq

from tertius.constants import SPAWN_READY_TIMEOUT_MS, Cmd
from tertius.exceptions import (
    DeadProcessError,
    LinkedCrashError,
    NormalExitError,
    ProcessCrashError,
)
from tertius.transport_types import Transport
from tertius.types import Pid, Scope
from tertius.vm.broker_effects import (
    ECrashCmd,
    EEmitCmd,
    EKillCmd,
    ELinkCmd,
    EMonitorCmd,
    ERegisterCmd,
    ESpawnCmd,
    EWhereisCmd,
    decode_frame,
)
from tertius.vm.broker_state import BrokerState
from tertius.vm.broker_utils import reply
from tertius.vm.events import (
    link_delivered,
    link_established,
    link_retroactive,
    monitor_delivered,
    monitor_established,
    monitor_retroactive,
    name_registered,
    name_unbound,
    process_crashed,
    process_exited,
    spawn_ready,
    spawn_started,
    spawn_timeout,
)
from tertius.vm.messages import (
    encode_crash_notification,
    encode_linked_crash_notification,
    pid_reply,
    whereis_reply,
)
from tertius.vm.process import process_entry

_SPAWN_CTX = multiprocessing.get_context("spawn")


def handle_register(
    state: BrokerState,
    router: "zmq.Socket[bytes]",
    effect: ERegisterCmd,
) -> Generator[None, Any, None]:
    """Register a process name."""

    yield from ()
    # Names are process-scoped: the pid is derived from the requester identity
    # rather than the message body, so a process can only register itself.
    pid = Pid.from_bytes(effect.requester)
    state.names[effect.name] = pid
    state.emit_queue.put(name_registered(pid, effect.name))
    reply(router, effect.requester, Cmd.OK)
    return


def handle_whereis(
    state: BrokerState,
    router: "zmq.Socket[bytes]",
    effect: EWhereisCmd,
) -> Generator[None, Any, None]:
    """Lookup a process by name."""

    yield from ()
    pid = state.names.get(effect.name)
    reply(router, effect.requester, *whereis_reply.encode(pid))
    return


def handle_link(
    state: BrokerState,
    notifier: "zmq.Socket[bytes]",
    router: "zmq.Socket[bytes]",
    effect: ELinkCmd,
) -> Generator[None, Any, None]:
    """Bidirectionally link two processes, so that if one crashes the other is notified."""

    yield from ()
    requester_pid = Pid.from_bytes(effect.requester)
    # Ack immediately so the requester isn't blocked while we check the tombstone.
    reply(router, effect.requester, Cmd.OK)

    if effect.pid in state.dead:
        # Target already gone — deliver the crash signal retroactively so the
        # caller behaves consistently whether the link races with a crash or not.
        kill_msg = LinkedCrashError(pid=effect.pid, reason=state.dead[effect.pid])
        notifier.send_multipart(
            encode_linked_crash_notification(requester_pid, effect.pid, kill_msg)
        )
        state.emit_queue.put(link_retroactive(effect.pid))
        return

    state.links.setdefault(requester_pid, set()).add(effect.pid)
    state.links.setdefault(effect.pid, set()).add(requester_pid)
    state.emit_queue.put(link_established(requester_pid))
    return


def handle_monitor(
    state: BrokerState,
    notifier: "zmq.Socket[bytes]",
    router: "zmq.Socket[bytes]",
    effect: EMonitorCmd,
) -> Generator[None, Any, None]:
    """Notify the broker that this process wants to monitor the target PID."""

    yield from ()
    requester_pid = Pid.from_bytes(effect.requester)
    reply(router, effect.requester, Cmd.OK)

    if effect.pid in state.dead:
        # Same retroactive delivery as handle_link — the monitor guarantee is
        # that you always receive exactly one notification, even if the target
        # died before you asked.
        crash_msg = ProcessCrashError(pid=effect.pid, reason=state.dead[effect.pid])
        notifier.send_multipart(
            encode_crash_notification(requester_pid, effect.pid, crash_msg)
        )
        state.emit_queue.put(monitor_retroactive(effect.pid))
        return

    state.monitors.setdefault(effect.pid, set()).add(requester_pid)
    state.emit_queue.put(monitor_established(requester_pid))
    return


def handle_emit(
    state: BrokerState,
    router: "zmq.Socket[bytes]",
    effect: EEmitCmd,
) -> Generator[None, Any, None]:
    """Emitted values are surfaced to the host application via the queue rather
    than being routed to another process, so they cross the VM boundary."""

    yield from ()
    state.emit_queue.put(effect.body)
    reply(router, effect.requester, Cmd.OK)
    return


def _notify_monitors(
    state: BrokerState,
    notifier: "zmq.Socket[bytes]",
    pid: Pid,
    reason: Exception,
) -> list[Pid]:
    """Notify the monitors of a process crash."""

    crash_msg = ProcessCrashError(pid=pid, reason=reason)
    watchers = list(state.monitors.pop(pid, []))

    for watcher in watchers:
        notifier.send_multipart(encode_crash_notification(watcher, pid, crash_msg))

    return watchers


def _notify_links(
    state: BrokerState,
    notifier: "zmq.Socket[bytes]",
    pid: Pid,
    reason: Exception,
) -> list[Pid]:
    """Notify the links of a process crash."""

    if isinstance(reason, NormalExitError):
        state.links.pop(pid, None)
        return []

    # Links are bidirectional: when one end dies the other gets a LinkedCrashError
    # signal and the back-reference is cleaned up so the surviving process isn't
    # notified again if it subsequently dies itself.
    kill_msg = LinkedCrashError(pid=pid, reason=reason)
    peers = list(state.links.pop(pid, set()))

    for peer in peers:
        if peer in state.links:
            state.links[peer].discard(pid)
        notifier.send_multipart(encode_linked_crash_notification(peer, pid, kill_msg))

    return peers


def _record_crash(
    state: BrokerState,
    notifier: "zmq.Socket[bytes]",
    pid: Pid,
    reason: Exception,
) -> tuple[list[str], list[Pid], list[Pid]]:
    """Mark a process as dead and propagate the crash to any observers.

    The pid is kept in `dead` as a tombstone so that future link/monitor
    requests against it can be answered immediately rather than hanging.
    Registered names are unbound so they can be reclaimed by a replacement process.

    Returns (unbound_names, notified_monitors, notified_links) for the caller to emit.
    """
    state.dead[pid] = reason

    unbound = [name for name, owner in state.names.items() if owner == pid]
    for name in unbound:
        del state.names[name]

    watchers = _notify_monitors(state, notifier, pid, reason)
    peers = _notify_links(state, notifier, pid, reason)

    return unbound, watchers, peers


def _emit_crash_events(
    state: BrokerState,
    pid: Pid,
    unbound: list[str],
    watchers: list[Pid],
    peers: list[Pid],
) -> None:
    """Emit crash events for a process."""

    for name in unbound:
        state.emit_queue.put(name_unbound(pid, name))

    for _watcher in watchers:
        state.emit_queue.put(monitor_delivered(pid))

    for _peer in peers:
        state.emit_queue.put(link_delivered(pid))


def handle_kill(
    state: BrokerState,
    notifier: "zmq.Socket[bytes]",
    router: "zmq.Socket[bytes]",
    effect: EKillCmd,
) -> Generator[None, Any, None]:
    """Kill a process."""

    yield from ()
    if effect.target in state.dead:
        reply(router, effect.requester, Cmd.ERROR, pickle.dumps(DeadProcessError(effect.target)))
        return

    # Ack before terminating so the caller isn't blocked waiting on a process
    # that may take a moment to actually die.
    reply(router, effect.requester, Cmd.OK)

    proc = state.procs.pop(effect.target, None)
    if proc is not None:
        proc.terminate()

    # Treat an external kill as a crash so monitors and links are notified
    # through the same path as a natural process failure.
    killed_reason = RuntimeError("killed")
    state.emit_queue.put(process_crashed(effect.target, killed_reason))
    unbound, watchers, peers = _record_crash(state, notifier, effect.target, killed_reason)
    _emit_crash_events(state, effect.target, unbound, watchers, peers)
    return


def handle_crash(
    state: BrokerState,
    notifier: "zmq.Socket[bytes]",
    router: "zmq.Socket[bytes]",
    effect: ECrashCmd,
) -> Generator[None, Any, None]:
    """Handle a process crash."""

    yield from ()
    # A process reports its own crash rather than the broker detecting it via
    # polling, so the reason is accurate and propagation is synchronous.
    if isinstance(effect.reason, NormalExitError):
        state.emit_queue.put(process_exited(effect.pid))
    else:
        state.emit_queue.put(process_crashed(effect.pid, effect.reason))

    unbound, watchers, peers = _record_crash(state, notifier, effect.pid, effect.reason)
    _emit_crash_events(state, effect.pid, unbound, watchers, peers)
    reply(router, effect.requester, Cmd.OK)
    return


def _start_process(
    pid: Pid,
    fn_name: str,
    args: tuple[Any, ...],
    broker_addr: str,
    ctrl_addr: str,
    scope: Scope,
    state: BrokerState,
    transport: Transport,
) -> BaseProcess:
    """Start a new process."""

    # Daemon=True so child processes don't outlive the broker if it exits uncleanly.
    # spawn (not fork) avoids inheriting the parent's ZMQ IO threads, which
    # causes libzmq to abort() when the child later calls zmq_msg_recv.
    proc = _SPAWN_CTX.Process(
        target=process_entry,
        args=(pid.node_id, pid.id, broker_addr, ctrl_addr, fn_name, args, scope, transport),
        daemon=True,
    )
    proc.start()
    state.procs[pid] = proc
    return proc


def await_ready_gen(
    router: "zmq.Socket[bytes]",
    proc: BaseProcess,
    new_pid: Pid,
    fn_name: str,
    pending_readies: dict[bytes, None],
) -> Generator[Any, Any, None]:
    """Wait for the spawned process to send READY, dispatching other commands that arrive meanwhile.

    A nested spawn handler may consume and stash our READY while waiting for its own; we check
    pending_readies at each iteration so we don't miss it. RCVTIMEO is saved and restored so
    nested calls don't accidentally disable the outer timeout.
    """

    pid_bytes = bytes(new_pid)
    prev_timeout = router.getsockopt(zmq.RCVTIMEO)
    router.setsockopt(zmq.RCVTIMEO, SPAWN_READY_TIMEOUT_MS)
    try:
        while True:
            # A nested await_ready_gen may have consumed our READY and stashed it.
            if pid_bytes in pending_readies:
                del pending_readies[pid_bytes]
                reply(router, pid_bytes, Cmd.OK)
                return

            try:
                frames = router.recv_multipart()
            except zmq.Again:
                if not proc.is_alive():
                    raise RuntimeError(
                        f"ESpawn: process {fn_name!r} died before sending READY "
                        f"(exit code {proc.exitcode})"
                    ) from None
                continue

            child_requester, child_command = frames[0], frames[1]

            if child_command == Cmd.READY and child_requester == pid_bytes:
                reply(router, child_requester, Cmd.OK)
                return

            if child_command == Cmd.READY:
                # READY for a different PID — stash it for that process's await_ready_gen.
                pending_readies[child_requester] = None
                continue

            effect = decode_frame(frames)
            if effect is not None:
                yield effect
    finally:
        router.setsockopt(zmq.RCVTIMEO, prev_timeout)


def handle_spawn(
    alloc_pid: Callable[[], Pid],
    scope: Scope,
    broker_addr: str,
    ctrl_addr: str,
    state: BrokerState,
    router: "zmq.Socket[bytes]",
    transport: Transport,
    effect: ESpawnCmd,
) -> Generator[Any, Any, None]:
    """Spawn a new process."""

    yield from ()
    fn_name = effect.fn_name
    args = effect.args

    if fn_name not in scope:
        raise KeyError(f"ESpawn: {fn_name!r} not in scope; available: {sorted(scope)}")

    new_pid = alloc_pid()
    spawn_start = time.time()
    proc = _start_process(
        new_pid, fn_name, args, broker_addr, ctrl_addr, scope, state, transport
    )
    state.emit_queue.put(spawn_started(new_pid))

    try:
        # Block until the process is ready before replying to the caller, so the
        # caller can safely send to the new pid immediately after receiving its pid back.
        yield from await_ready_gen(router, proc, new_pid, fn_name, state.pending_readies)
    except (RuntimeError, zmq.ZMQError):
        # ZMQError covers broker shutdown racing with await_ready_gen — still emit
        # the timeout event so the telemetry stream is always closed.
        state.emit_queue.put(spawn_timeout(new_pid, fn_name, proc.exitcode))
        raise

    state.emit_queue.put(spawn_ready(new_pid, spawn_start))
    reply(router, effect.requester, *pid_reply.encode(new_pid))
    return
