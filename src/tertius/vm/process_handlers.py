"""Effect handlers for spawned processes — bridges algebraic effects to ZMQ socket calls."""

import pickle
import threading
import time
from collections.abc import Generator
from functools import partial
from typing import Any

import zmq

from tertius.constants import RECEIVE_POLL_INTERVAL_MS, Cmd
from tertius.effects import (
    EEmit,
    EKill,
    ELink,
    EMonitor,
    EReceive,
    EReceiveTimeout,
    ERegister,
    ESelf,
    ESend,
    ESleep,
    ESpawn,
    EWhereis,
)
from tertius.exceptions import KilledError, LinkedCrashError
from tertius.types import Envelope, Pid
from tertius.vm.broker_utils import ctrl_send
from tertius.vm.messages import (
    emit,
    envelope,
    kill,
    link,
    monitor,
    pid_reply,
    register,
    spawn,
    whereis,
    whereis_reply,
)


def _handle_self(pid: Pid, _effect: ESelf) -> Generator[None, Any, Pid]:
    """Return the current process's PID."""

    yield from ()
    return pid


def _handle_spawn(ctrl: "zmq.Socket[bytes]", effect: ESpawn) -> Generator[None, Any, Pid]:
    """Spawn a new process by sending a spawn request to the broker and returning the new PID."""

    yield from ()
    ctrl.send_multipart(spawn.encode(effect.fn_name, effect.args, effect.mode))
    reply = ctrl.recv_multipart()

    if reply[0] == Cmd.ERROR:
        raise pickle.loads(reply[1])

    return pid_reply.decode(reply)


def _handle_send(
    dealer: "zmq.Socket[bytes]", pid: Pid, effect: ESend
) -> Generator[None, Any, None]:
    """Send a message to the target PID.

    Encode it as an Envelope and send it to the broker.
    """

    yield from ()
    dealer.send_multipart(envelope.encode(effect.pid, pid, effect.body))
    return


def _handle_link(ctrl: "zmq.Socket[bytes]", effect: ELink) -> Generator[None, Any, None]:
    """Link the current process to the target PID by sending a message to the broker."""

    yield from ()
    ctrl_send(ctrl, *link.encode(effect.pid))
    return


def check_receive_killed(pid: Pid, kill_event: threading.Event | None) -> None:
    """Raise KilledError if a cooperative kill fired while this worker was receiving."""

    if kill_event is not None and kill_event.is_set():
        raise KilledError(pid)


def next_poll_ms(kill_event: threading.Event | None, deadline: float | None) -> int | None:
    """Poll window in ms: bounded when a kill event must be checked, else until the deadline.

    None means poll forever — only possible without a kill event or deadline.
    """

    remaining = None if deadline is None else max(int((deadline - time.monotonic()) * 1000), 0)
    if kill_event is None:
        return remaining
    if remaining is None:
        return RECEIVE_POLL_INTERVAL_MS

    return min(remaining, RECEIVE_POLL_INTERVAL_MS)


def wait_for_envelope(
    dealer: "zmq.Socket[bytes]",
    pid: Pid,
    kill_event: threading.Event | None,
    timeout_ms: int | None,
) -> Envelope | None:
    """Block until an envelope arrives, the timeout expires (None), or the kill event fires.

    Thread workers poll in short intervals so a cooperative kill interrupts a blocked
    receive; process workers poll in one window since they are killed by signal.
    """

    poller = zmq.Poller()
    poller.register(dealer, zmq.POLLIN)
    deadline = time.monotonic() + timeout_ms / 1000 if timeout_ms is not None else None

    while True:
        check_receive_killed(pid, kill_event)
        ready = dict(poller.poll(next_poll_ms(kill_event, deadline)))
        if dealer in ready:
            return envelope.decode(dealer.recv_multipart())
        if deadline is not None and time.monotonic() >= deadline:
            return None


def recv_until_killed(
    dealer: "zmq.Socket[bytes]", pid: Pid, kill_event: threading.Event
) -> Envelope:
    """Receive with no deadline — returns an envelope or raises KilledError."""

    env = wait_for_envelope(dealer, pid, kill_event, None)
    if env is None:
        raise RuntimeError("wait_for_envelope returned no envelope despite having no timeout")
    return env


def _handle_receive(
    dealer: "zmq.Socket[bytes]",
    pid: Pid,
    kill_event: threading.Event | None,
    _effect: EReceive,
) -> Generator[None, Any, Envelope]:
    """Wait for a message and return it as an Envelope."""

    yield from ()
    if kill_event is None:
        # Direct blocking recv so a socket-level RCVTIMEO (set by join) still applies.
        env = envelope.decode(dealer.recv_multipart())
    else:
        env = recv_until_killed(dealer, pid, kill_event)

    if isinstance(env.body, LinkedCrashError):
        raise env.body

    return env


def _handle_receive_timeout(
    dealer: "zmq.Socket[bytes]",
    pid: Pid,
    kill_event: threading.Event | None,
    effect: EReceiveTimeout,
) -> "Generator[None, Any, Envelope | None]":
    """Wait for a message with a timeout; return None if the timeout expires."""

    yield from ()
    env = wait_for_envelope(dealer, pid, kill_event, effect.timeout_ms)
    if env is None:
        return None

    if isinstance(env.body, LinkedCrashError):
        raise env.body

    return env


def _handle_register(ctrl: "zmq.Socket[bytes]", effect: ERegister) -> Generator[None, Any, None]:
    """Register the current process under the given name by sending a message to the broker."""

    yield from ()
    ctrl_send(ctrl, *register.encode(effect.name))
    return


def _handle_whereis(
    ctrl: "zmq.Socket[bytes]", effect: EWhereis
) -> Generator[None, Any, Pid | None]:
    """Query the broker for the PID registered under the given name, if any."""

    yield from ()
    ctrl.send_multipart(whereis.encode(effect.name))
    return whereis_reply.decode(ctrl.recv_multipart())


def _handle_monitor(ctrl: "zmq.Socket[bytes]", effect: EMonitor) -> Generator[None, Any, None]:
    """Notify the broker that this process wants to monitor the target PID"""

    yield from ()
    ctrl_send(ctrl, *monitor.encode(effect.pid))
    return


def _handle_sleep(effect: ESleep) -> Generator[None, Any, None]:
    """Simply sleep for a bit"""

    yield from ()
    time.sleep(effect.ms / 1000)
    return


def _handle_emit(ctrl: "zmq.Socket[bytes]", effect: EEmit) -> Generator[None, Any, None]:
    """Emit an event by sending it to the broker"""

    yield from ()
    ctrl_send(ctrl, *emit.encode(effect.body))
    return


def _handle_kill(ctrl: "zmq.Socket[bytes]", effect: EKill) -> Generator[None, Any, None]:
    yield from ()
    ctrl.send_multipart(kill.encode(effect.pid))
    response = ctrl.recv_multipart()
    if response[0] == Cmd.ERROR:
        raise pickle.loads(response[1])
    return


def make_handlers(
    pid: Pid,
    dealer: "zmq.Socket[bytes]",
    ctrl: "zmq.Socket[bytes]",
    kill_event: threading.Event | None,
) -> dict[str, Any]:
    """Factory function for process effect handlers"""

    return {
        "self": partial(_handle_self, pid),
        "spawn": partial(_handle_spawn, ctrl),
        "send": partial(_handle_send, dealer, pid),
        "receive": partial(_handle_receive, dealer, pid, kill_event),
        "receive_timeout": partial(_handle_receive_timeout, dealer, pid, kill_event),
        "link": partial(_handle_link, ctrl),
        "register": partial(_handle_register, ctrl),
        "whereis": partial(_handle_whereis, ctrl),
        "monitor": partial(_handle_monitor, ctrl),
        "sleep": _handle_sleep,
        "emit": partial(_handle_emit, ctrl),
        "kill": partial(_handle_kill, ctrl),
    }
