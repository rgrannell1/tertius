# Broker control handlers — register, whereis, link, monitor, emit.
from collections.abc import Generator
from typing import Any

import zmq

from tertius.constants import Cmd
from tertius.exceptions import LinkedCrashError, ProcessCrashError
from tertius.types import Pid
from tertius.vm.broker_effects import EEmitCmd, ELinkCmd, EMonitorCmd, ERegisterCmd, EWhereisCmd
from tertius.vm.broker_state import BrokerState
from tertius.vm.broker_utils import reply
from tertius.vm.events import (
    link_established,
    link_retroactive,
    monitor_established,
    monitor_retroactive,
    name_registered,
)
from tertius.vm.messages import encode_crash_notification, encode_linked_crash_notification, whereis_reply


def handle_register(
    state: BrokerState,
    router: "zmq.Socket[bytes]",
    effect: ERegisterCmd,
) -> Generator[None, Any, None]:
    # Names are process-scoped: the pid is derived from the requester identity
    # rather than the message body, so a process can only register itself.
    pid = Pid.from_bytes(effect.requester)
    state.names[effect.name] = pid
    state.emit_queue.put(name_registered(pid, effect.name))
    reply(router, effect.requester, Cmd.OK)
    return
    yield


def handle_whereis(
    state: BrokerState,
    router: "zmq.Socket[bytes]",
    effect: EWhereisCmd,
) -> Generator[None, Any, None]:
    pid = state.names.get(effect.name)
    reply(router, effect.requester, *whereis_reply.encode(pid))
    return
    yield


def handle_link(
    state: BrokerState,
    notifier: "zmq.Socket[bytes]",
    router: "zmq.Socket[bytes]",
    effect: ELinkCmd,
) -> Generator[None, Any, None]:
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
        yield

    state.links.setdefault(requester_pid, set()).add(effect.pid)
    state.links.setdefault(effect.pid, set()).add(requester_pid)
    state.emit_queue.put(link_established(requester_pid))
    return
    yield


def handle_monitor(
    state: BrokerState,
    notifier: "zmq.Socket[bytes]",
    router: "zmq.Socket[bytes]",
    effect: EMonitorCmd,
) -> Generator[None, Any, None]:
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
        yield

    state.monitors.setdefault(effect.pid, set()).add(requester_pid)
    state.emit_queue.put(monitor_established(requester_pid))
    return
    yield


def handle_emit(
    state: BrokerState,
    router: "zmq.Socket[bytes]",
    effect: EEmitCmd,
) -> Generator[None, Any, None]:
    # Emitted values are surfaced to the host application via the queue rather
    # than being routed to another process, so they cross the VM boundary.
    state.emit_queue.put(effect.body)
    reply(router, effect.requester, Cmd.OK)
    return
    yield
