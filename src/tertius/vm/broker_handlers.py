# Broker control handlers — register, whereis, link, monitor, emit.
import zmq

from tertius.constants import Cmd
from tertius.exceptions import LinkedCrashError, ProcessCrashError
from tertius.types import Pid
from tertius.vm.broker_state import BrokerState
from tertius.vm.broker_utils import reply
from tertius.vm.events import (
    link_established,
    link_retroactive,
    monitor_established,
    monitor_retroactive,
    name_registered,
)
from tertius.vm.messages import (
    emit,
    encode_crash_notification,
    encode_linked_crash_notification,
    link,
    monitor,
    register,
    whereis,
    whereis_reply,
)


def handle_register(
    state: BrokerState,
    router: "zmq.Socket[bytes]",
    requester: bytes,
    frames: list[bytes],
) -> None:
    # Names are process-scoped: the pid is derived from the requester identity
    # rather than the message body, so a process can only register itself.
    registered_name = register.decode(frames)
    pid = Pid.from_bytes(requester)
    state.names[registered_name] = pid
    state.emit_queue.put(name_registered(pid, registered_name))
    reply(router, requester, Cmd.OK)


def handle_whereis(
    state: BrokerState,
    router: "zmq.Socket[bytes]",
    requester: bytes,
    frames: list[bytes],
) -> None:
    pid = state.names.get(whereis.decode(frames))
    reply(router, requester, *whereis_reply.encode(pid))


def handle_link(
    state: BrokerState,
    notifier: "zmq.Socket[bytes]",
    router: "zmq.Socket[bytes]",
    requester: bytes,
    frames: list[bytes],
) -> None:
    requester_pid = Pid.from_bytes(requester)
    target_pid = link.decode(frames)
    # Ack immediately so the requester isn't blocked while we check the tombstone.
    reply(router, requester, Cmd.OK)

    if target_pid in state.dead:
        # Target already gone — deliver the crash signal retroactively so the
        # caller behaves consistently whether the link races with a crash or not.
        kill_msg = LinkedCrashError(pid=target_pid, reason=state.dead[target_pid])
        notifier.send_multipart(
            encode_linked_crash_notification(requester_pid, target_pid, kill_msg)
        )
        state.emit_queue.put(link_retroactive(target_pid))
        return

    state.links.setdefault(requester_pid, set()).add(target_pid)
    state.links.setdefault(target_pid, set()).add(requester_pid)
    state.emit_queue.put(link_established(requester_pid))


def handle_monitor(
    state: BrokerState,
    notifier: "zmq.Socket[bytes]",
    router: "zmq.Socket[bytes]",
    requester: bytes,
    frames: list[bytes],
) -> None:
    target_pid = monitor.decode(frames)
    requester_pid = Pid.from_bytes(requester)
    reply(router, requester, Cmd.OK)

    if target_pid in state.dead:
        # Same retroactive delivery as handle_link — the monitor guarantee is
        # that you always receive exactly one notification, even if the target
        # died before you asked.
        crash_msg = ProcessCrashError(pid=target_pid, reason=state.dead[target_pid])
        notifier.send_multipart(
            encode_crash_notification(requester_pid, target_pid, crash_msg)
        )
        state.emit_queue.put(monitor_retroactive(target_pid))
        return

    state.monitors.setdefault(target_pid, set()).add(requester_pid)
    state.emit_queue.put(monitor_established(requester_pid))


def handle_emit(
    state: BrokerState,
    router: "zmq.Socket[bytes]",
    requester: bytes,
    frames: list[bytes],
) -> None:
    # Emitted values are surfaced to the host application via the queue rather
    # than being routed to another process, so they cross the VM boundary.
    state.emit_queue.put(emit.decode(frames))
    reply(router, requester, Cmd.OK)
