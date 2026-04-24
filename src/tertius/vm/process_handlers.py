# Effect handlers for spawned processes — bridges algebraic effects to ZMQ socket calls.
import pickle
import time
from collections.abc import Generator
from functools import partial
from typing import Any

import zmq

from tertius.constants import Cmd
from tertius.vm.broker_utils import ctrl_send
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
from tertius.exceptions import LinkedCrash
from tertius.types import Envelope, Pid
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

    return pid
    yield


def _handle_spawn(ctrl: "zmq.Socket[bytes]", effect: ESpawn) -> Generator[None, Any, Pid]:
    """Spawn a new process by sending a spawn request to the broker and returning the new PID."""

    ctrl.send_multipart(spawn.encode(effect.fn_name, effect.args))
    reply = ctrl.recv_multipart()

    if reply[0] == Cmd.ERROR:
        raise pickle.loads(reply[1])

    return pid_reply.decode(reply)
    yield


def _handle_send(dealer: "zmq.Socket[bytes]", pid: Pid, effect: ESend) -> Generator[None, Any, None]:
    """Send a message to the target PID by encoding it as an Envelope and sending it to the broker."""

    dealer.send_multipart(envelope.encode(effect.pid, pid, effect.body))
    return
    yield


def _handle_link(ctrl: "zmq.Socket[bytes]", effect: ELink) -> Generator[None, Any, None]:
    """Link the current process to the target PID by sending a message to the broker."""

    ctrl_send(ctrl, *link.encode(effect.pid))
    return
    yield


def _handle_receive(dealer: "zmq.Socket[bytes]", _effect: EReceive) -> Generator[None, Any, Envelope]:
    """Wait for a message and return it as an Envelope."""

    env = envelope.decode(dealer.recv_multipart())

    if isinstance(env.body, LinkedCrash):
        raise env.body
    return env
    yield


def _handle_register(ctrl: "zmq.Socket[bytes]", effect: ERegister) -> Generator[None, Any, None]:
    """Register the current process under the given name by sending a message to the broker."""

    ctrl_send(ctrl, *register.encode(effect.name))
    return
    yield


def _handle_whereis(ctrl: "zmq.Socket[bytes]", effect: EWhereis) -> Generator[None, Any, Pid | None]:
    """Query the broker for the PID registered under the given name, if any."""

    ctrl.send_multipart(whereis.encode(effect.name))
    return whereis_reply.decode(ctrl.recv_multipart())
    yield


def _handle_receive_timeout(
    dealer: "zmq.Socket[bytes]", effect: EReceiveTimeout
) -> "Generator[None, Any, Envelope | None]":
    """ "Wait for a message with a timeout; return None if the timeout expires."""

    poller = zmq.Poller()
    poller.register(dealer, zmq.POLLIN)
    ready = dict(poller.poll(effect.timeout_ms))

    if dealer not in ready:
        return None

    env = envelope.decode(dealer.recv_multipart())
    if isinstance(env.body, LinkedCrash):
        raise env.body

    return env
    yield


def _handle_monitor(ctrl: "zmq.Socket[bytes]", effect: EMonitor) -> Generator[None, Any, None]:
    """Notify the broker that this process wants to monitor the target PID"""

    ctrl_send(ctrl, *monitor.encode(effect.pid))
    return
    yield


def _handle_sleep(effect: ESleep) -> Generator[None, Any, None]:
    """Simply sleep for a bit"""

    time.sleep(effect.ms / 1000)
    return
    yield


def _handle_emit(ctrl: "zmq.Socket[bytes]", effect: EEmit) -> Generator[None, Any, None]:
    """Emit an event by sending it to the broker"""

    ctrl_send(ctrl, *emit.encode(effect.body))
    return
    yield


def _handle_kill(ctrl: "zmq.Socket[bytes]", effect: EKill) -> Generator[None, Any, None]:
    ctrl.send_multipart(kill.encode(effect.pid))
    response = ctrl.recv_multipart()
    if response[0] == Cmd.ERROR:
        raise pickle.loads(response[1])
    return
    yield


def make_handlers(
    pid: Pid,
    dealer: "zmq.Socket[bytes]",
    ctrl: "zmq.Socket[bytes]",
) -> dict[str, Any]:
    """Factory function for process effect handlers"""

    return {
        "self": partial(_handle_self, pid),
        "spawn": partial(_handle_spawn, ctrl),
        "send": partial(_handle_send, dealer, pid),
        "receive": partial(_handle_receive, dealer),
        "receive_timeout": partial(_handle_receive_timeout, dealer),
        "link": partial(_handle_link, ctrl),
        "register": partial(_handle_register, ctrl),
        "whereis": partial(_handle_whereis, ctrl),
        "monitor": partial(_handle_monitor, ctrl),
        "sleep": _handle_sleep,
        "emit": partial(_handle_emit, ctrl),
        "kill": partial(_handle_kill, ctrl),
    }
