# Crash and kill handlers — propagate process failures to monitors and linked processes.
import pickle

import zmq

from tertius.constants import Cmd
from tertius.exceptions import DeadProcessError, LinkedCrashError, NormalExitError, ProcessCrashError
from tertius.types import Pid
from tertius.vm.broker_state import BrokerState
from tertius.vm.broker_utils import reply
from tertius.vm.events import (
    link_delivered,
    monitor_delivered,
    name_unbound,
    process_crashed,
    process_exited,
)
from tertius.vm.messages import (
    crash,
    encode_crash_notification,
    encode_linked_crash_notification,
    kill,
)


def _notify_monitors(
    state: BrokerState,
    notifier: "zmq.Socket[bytes]",
    pid: Pid,
    reason: Exception,
) -> list[Pid]:
    # Monitors receive a one-shot notification and are then removed — they don't
    # re-arm automatically, matching Erlang's monitor semantics.
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
    requester: bytes,
    frames: list[bytes],
) -> None:
    target_pid = kill.decode(frames)

    if target_pid in state.dead:
        reply(router, requester, Cmd.ERROR, pickle.dumps(DeadProcessError(target_pid)))
        return

    # Ack before terminating so the caller isn't blocked waiting on a process
    # that may take a moment to actually die.
    reply(router, requester, Cmd.OK)

    proc = state.procs.pop(target_pid, None)
    if proc is not None:
        proc.terminate()

    # Treat an external kill as a crash so monitors and links are notified
    # through the same path as a natural process failure.
    killed_reason = RuntimeError("killed")
    state.emit_queue.put(process_crashed(target_pid, killed_reason))
    unbound, watchers, peers = _record_crash(state, notifier, target_pid, killed_reason)
    _emit_crash_events(state, target_pid, unbound, watchers, peers)


def handle_crash(
    state: BrokerState,
    notifier: "zmq.Socket[bytes]",
    router: "zmq.Socket[bytes]",
    requester: bytes,
    frames: list[bytes],
) -> None:
    # A process reports its own crash rather than the broker detecting it via
    # polling, so the reason is accurate and propagation is synchronous.
    crashed_pid = Pid.from_bytes(requester)
    reason = crash.decode(frames)

    if isinstance(reason, NormalExitError):
        state.emit_queue.put(process_exited(crashed_pid))
    else:
        state.emit_queue.put(process_crashed(crashed_pid, reason))

    unbound, watchers, peers = _record_crash(state, notifier, crashed_pid, reason)
    _emit_crash_events(state, crashed_pid, unbound, watchers, peers)
    reply(router, requester, Cmd.OK)
