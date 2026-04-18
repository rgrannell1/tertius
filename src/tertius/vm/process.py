import sys
import traceback
from collections.abc import Callable
from functools import partial
from typing import Any

import zmq
from orbis import complete

from tertius.constants import READY
from tertius.effects import EEmit, EKill, ELink, EMonitor, EReceive, EReceiveTimeout, ERegister, ESelf, ESend, ESpawn, EWhereis
from tertius.exceptions import LinkedCrash
from tertius.types import Envelope, Pid
from tertius.vm.messages import (
    decode_pid_reply,
    decode_received_envelope,
    decode_whereis_reply,
    encode_crash,
    encode_emit,
    encode_envelope,
    encode_kill,
    encode_link,
    encode_monitor,
    encode_register,
    encode_spawn,
    encode_whereis,
)


def _handle_self(pid: Pid, _effect: ESelf) -> Pid:
    """Handle the ESelf effect; return our PID"""
    return pid


def _handle_spawn(ctrl: "zmq.Socket[bytes]", effect: ESpawn) -> Pid:
    """Handle the ESpawn effect; spawn a new process"""
    ctrl.send_multipart(encode_spawn(effect.fn_name, effect.args))
    return decode_pid_reply(ctrl.recv_multipart())


def _handle_send(dealer: "zmq.Socket[bytes]", pid: Pid, effect: ESend) -> None:
    """Handle the ESend effect; send a message to a process"""
    dealer.send_multipart(encode_envelope(effect.pid, pid, effect.body))


def _handle_link(ctrl: "zmq.Socket[bytes]", effect: ELink) -> None:
    """Handle the ELink effect; register a bidirectional link with another process"""
    ctrl.send_multipart(encode_link(effect.pid))
    ctrl.recv_multipart()


def _handle_receive(dealer: "zmq.Socket[bytes]", _effect: EReceive) -> Envelope:
    """Handle the EReceive effect; receive a message from a process"""
    envelope = decode_received_envelope(dealer.recv_multipart())
    if isinstance(envelope.body, LinkedCrash):
        raise envelope.body
    return envelope


def _handle_register(ctrl: "zmq.Socket[bytes]", effect: ERegister) -> None:
    """Handle the ERegister effect; register a process name"""
    ctrl.send_multipart(encode_register(effect.name))
    ctrl.recv_multipart()


def _handle_whereis(ctrl: "zmq.Socket[bytes]", effect: EWhereis) -> Pid | None:
    """Handle the EWhereis effect; lookup a process by name"""
    ctrl.send_multipart(encode_whereis(effect.name))
    return decode_whereis_reply(ctrl.recv_multipart())


def _handle_receive_timeout(
    dealer: "zmq.Socket[bytes]", effect: EReceiveTimeout
) -> "Envelope | None":
    """Handle EReceiveTimeout via a poller — never mutates socket options."""

    poller = zmq.Poller()
    poller.register(dealer, zmq.POLLIN)
    ready = dict(poller.poll(effect.timeout_ms))

    if dealer not in ready:
        return None

    envelope = decode_received_envelope(dealer.recv_multipart())
    if isinstance(envelope.body, LinkedCrash):
        raise envelope.body
    return envelope


def _handle_monitor(ctrl: "zmq.Socket[bytes]", effect: EMonitor) -> None:
    """Handle the EMonitor effect; monitor a process for crashes"""

    ctrl.send_multipart(encode_monitor(effect.pid))
    ctrl.recv_multipart()


def _handle_emit(ctrl: "zmq.Socket[bytes]", effect: EEmit) -> None:
    """Handle the EEmit effect; forward event to the broker's emit queue."""

    ctrl.send_multipart(encode_emit(effect.body))
    ctrl.recv_multipart()


def _handle_kill(ctrl: "zmq.Socket[bytes]", effect: EKill) -> None:
    """Handle the EKill effect; terminate a process via the broker"""

    ctrl.send_multipart(encode_kill(effect.pid))
    ctrl.recv_multipart()


def make_handlers(
    pid: Pid,
    dealer: "zmq.Socket[bytes]",
    ctrl: "zmq.Socket[bytes]",
) -> dict[str, Any]:
    """Make a dictionary of handlers for the process"""

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
        "emit": partial(_handle_emit, ctrl),
        "kill": partial(_handle_kill, ctrl),
    }


def process_entry(
    pid_int: int,
    broker_addr: str,
    ctrl_addr: str,
    fn_name: str,
    args: tuple[Any, ...],
    scope: dict[str, Callable[..., Any]],
) -> None:
    """Entry point for each spawned OS process. Must be module-level to be picklable."""

    ctx = zmq.Context()
    pid = Pid(pid_int)

    dealer: zmq.Socket[bytes] = ctx.socket(zmq.DEALER)
    dealer.identity = bytes(pid)
    dealer.connect(broker_addr)

    ctrl: zmq.Socket[bytes] = ctx.socket(zmq.DEALER)
    ctrl.identity = bytes(pid)
    ctrl.connect(ctrl_addr)

    try:
        ctrl.send_multipart([READY])
        ctrl.recv_multipart()
        fn = scope[fn_name]
        complete(fn(*args), **make_handlers(pid, dealer, ctrl))
    except Exception as err:
        print(f"[tertius] process {pid} crashed: {err}", file=sys.stderr, flush=True)
        traceback.print_exc(file=sys.stderr)
        ctrl.send_multipart(encode_crash(err))
        ctrl.recv_multipart()
    finally:
        dealer.close()
        ctrl.close()
        ctx.term()
