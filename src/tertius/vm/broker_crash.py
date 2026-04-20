import zmq

from tertius.constants import OK
from tertius.exceptions import LinkedCrash, ProcessCrash
from tertius.types import Pid
from tertius.vm.broker_state import BrokerState
from tertius.vm.broker_utils import reply
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
) -> None:
    # Monitors receive a one-shot notification and are then removed — they don't
    # re-arm automatically, matching Erlang's monitor semantics.
    crash_msg = ProcessCrash(pid=pid, reason=reason)

    for watcher in state.monitors.pop(pid, []):
        notifier.send_multipart(encode_crash_notification(watcher, pid, crash_msg))


def _notify_links(
    state: BrokerState,
    notifier: "zmq.Socket[bytes]",
    pid: Pid,
    reason: Exception,
) -> None:
    # Links are bidirectional: when one end dies the other gets a LinkedCrash
    # signal and the back-reference is cleaned up so the surviving process isn't
    # notified again if it subsequently dies itself.
    kill_msg = LinkedCrash(pid=pid, reason=reason)

    for peer in state.links.pop(pid, []):
        if peer in state.links:
            state.links[peer] = [
                linked for linked in state.links[peer] if linked != pid
            ]
        notifier.send_multipart(encode_linked_crash_notification(peer, pid, kill_msg))


def _record_crash(
    state: BrokerState,
    notifier: "zmq.Socket[bytes]",
    pid: Pid,
    reason: Exception,
) -> None:
    """Mark a process as dead and propagate the crash to any observers.

    The pid is kept in `dead` as a tombstone so that future link/monitor
    requests against it can be answered immediately rather than hanging.
    Registered names are unbound so they can be reclaimed by a replacement process.
    """
    state.dead[pid] = reason

    for name in [n for n, owner in state.names.items() if owner == pid]:
        del state.names[name]

    _notify_monitors(state, notifier, pid, reason)
    _notify_links(state, notifier, pid, reason)


def handle_kill(
    state: BrokerState,
    notifier: "zmq.Socket[bytes]",
    router: "zmq.Socket[bytes]",
    requester: bytes,
    frames: list[bytes],
) -> None:
    target_pid = kill.decode(frames)
    # Ack before terminating so the caller isn't blocked waiting on a process
    # that may take a moment to actually die.
    reply(router, requester, OK)

    if target_pid in state.dead:
        return

    proc = state.procs.pop(target_pid, None)
    if proc is not None:
        proc.terminate()

    # Treat an external kill as a crash so monitors and links are notified
    # through the same path as a natural process failure.
    _record_crash(state, notifier, target_pid, RuntimeError("killed"))


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

    _record_crash(state, notifier, crashed_pid, reason)
    reply(router, requester, OK)
