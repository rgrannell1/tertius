"""Join a running Tertius VM from a separately started OS process.

A joined process connects to an existing broker (typically over TCP from another
host), handshakes, then runs a generator with the standard process handlers. It
can message any pid routed by that broker, register names, and look names up.
"""

import itertools
import os
import socket
from collections.abc import Callable
from typing import Any

import zmq
from orbis import complete

from tertius.constants import Cmd
from tertius.exceptions import JoinRejectedError, JoinTimeoutError
from tertius.transport_types import IpcTransport, Transport
from tertius.types import Pid
from tertius.vm.messages import join as join_codec
from tertius.vm.process import _on_crash, _on_normal_exit
from tertius.vm.process_handlers import make_handlers
from tertius.vm.runner import make_node_id
from tertius.vm.transport import build_addresses, make_dealer

# Distinguishes sequential joins from the same OS process, so a crashed join's
# pid tombstone in the broker never collides with a later join's pid.
_join_instance = itertools.count().__next__

# Block forever on receive unless the caller sets recv_timeout_ms.
_BLOCK_FOREVER = -1


def alloc_join_pid() -> Pid:
    """Allocate a pid for a joining process that cannot collide with broker pids.

    The node id hashes (hostname, os pid), so it differs from the broker's node id
    even on the same machine; the instance counter separates sequential joins.
    """

    node_id = make_node_id(socket.gethostname(), os.getpid())
    return Pid(node_id=node_id, id=_join_instance())


def resolve_broker_addresses(transport: Transport) -> tuple[str, str]:
    """Resolve the broker's data and control addresses for a joining process.

    IPC addresses without an explicit base path embed the broker's OS pid, which a
    separate process cannot derive — joining needs an address both sides agree on.
    """

    if isinstance(transport, IpcTransport) and transport.base_path is None:
        raise ValueError("join requires TcpTransport or IpcTransport with an explicit base_path")

    return build_addresses(transport, os.getpid(), 0)


def shake_hands(
    ctrl: "zmq.Socket[bytes]", ctrl_addr: str, handshake_timeout_ms: int
) -> None:
    """Send Cmd.JOIN and wait for the broker's ack, failing fast if unreachable."""

    ctrl.setsockopt(zmq.RCVTIMEO, handshake_timeout_ms)
    ctrl.send_multipart(join_codec.encode())
    try:
        reply = ctrl.recv_multipart()
    except zmq.Again:
        raise JoinTimeoutError(ctrl_addr, handshake_timeout_ms) from None

    if reply[0] != Cmd.OK:
        raise JoinRejectedError(ctrl_addr, reply[0])


def notify_exit(pid: Pid, ctrl: "zmq.Socket[bytes]", err: Exception | None) -> None:
    """Report normal exit or crash to the broker, tolerating a broker that has gone away.

    RCVTIMEO must be unset on ctrl before calling — the caller is responsible for
    resetting it — so a slow-but-live broker is not mistaken for a gone broker.
    """

    try:
        if err is None:
            _on_normal_exit(pid, ctrl)
        else:
            _on_crash(pid, ctrl, err)
    except zmq.ZMQError:
        return


def join(
    fn: Callable[..., Any],
    *args: Any,
    transport: Transport,
    handshake_timeout_ms: int = 5_000,
    recv_timeout_ms: int | None = None,
) -> Any:
    """Run a generator function as a process joined to an existing broker.

    Blocks until the generator finishes and returns its result. recv_timeout_ms
    bounds every receive (data and control): if the broker stops replying for that
    long the joined process raises rather than hanging forever. Leave it None for
    processes that legitimately wait indefinitely for messages.
    """

    broker_addr, ctrl_addr = resolve_broker_addresses(transport)
    pid = alloc_join_pid()
    ctx: zmq.Context[zmq.Socket[bytes]] = zmq.Context()
    dealer = make_dealer(ctx, pid, broker_addr, transport)
    ctrl = make_dealer(ctx, pid, ctrl_addr, transport)

    try:
        shake_hands(ctrl, ctrl_addr, handshake_timeout_ms)

        timeout = recv_timeout_ms if recv_timeout_ms is not None else _BLOCK_FOREVER
        ctrl.setsockopt(zmq.RCVTIMEO, timeout)
        dealer.setsockopt(zmq.RCVTIMEO, timeout)

        try:
            result = complete(fn(*args), **make_handlers(pid, dealer, ctrl))
        except Exception as err:
            ctrl.setsockopt(zmq.RCVTIMEO, _BLOCK_FOREVER)
            notify_exit(pid, ctrl, err)
            raise

        ctrl.setsockopt(zmq.RCVTIMEO, _BLOCK_FOREVER)
        notify_exit(pid, ctrl, None)
        return result
    finally:
        dealer.close()
        ctrl.close()
        ctx.term()
