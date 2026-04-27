# Process entry point and lifecycle — runs a generator inside a spawned OS process.
import sys
import traceback
from collections.abc import Callable
from typing import Any

import zmq
from orbis import complete

from tertius.constants import Cmd
from tertius.exceptions import NormalExitError
from tertius.types import Pid
from tertius.vm.broker_utils import ctrl_send
from tertius.vm.messages import crash
from tertius.vm.process_handlers import make_handlers


def _primed(gen: Any, ctrl: "zmq.Socket[bytes]") -> Any:
    """Wrap a generator so Cmd.READY is sent only after it survives its first step.

    If the generator raises before yielding, no Cmd.READY is sent — the broker detects
    the dead process via its recv timeout.
    """

    try:
        effect = next(gen)
    except StopIteration:
        ctrl_send(ctrl, Cmd.READY)
        return

    ctrl_send(ctrl, Cmd.READY)

    while True:
        try:
            send_val = yield effect
        except BaseException as exc:  # noqa: BLE001
            try:
                effect = gen.throw(exc)
            except StopIteration:
                return
        else:
            try:
                effect = gen.send(send_val)
            except StopIteration:
                return


def _on_normal_exit(pid: Pid, ctrl: "zmq.Socket[bytes]") -> None:
    ctrl_send(ctrl, *crash.encode(NormalExitError(pid)))


def _on_crash(pid: Pid, ctrl: "zmq.Socket[bytes]", err: Exception) -> None:
    """Handle process crashes: log the error and notify the broker."""

    print(f"[tertius] process {pid} crashed: {err}", file=sys.stderr, flush=True)
    traceback.print_exc(file=sys.stderr)
    ctrl_send(ctrl, *crash.encode(err))


def _on_exit(
    dealer: "zmq.Socket[bytes]",
    ctrl: "zmq.Socket[bytes]",
    ctx: "zmq.Context[zmq.Socket[bytes]]",
) -> None:
    """Process cleanup: close sockets and terminate context."""

    dealer.close()
    ctrl.close()
    ctx.term()


def _connect_dealer(
    ctx: "zmq.Context[zmq.Socket[bytes]]", pid: Pid, addr: str
) -> "zmq.Socket[bytes]":
    """Connect a DEALER socket to the given address, using the PID as identity."""

    sock: zmq.Socket[bytes] = ctx.socket(zmq.DEALER)
    sock.identity = bytes(pid)
    sock.connect(addr)
    return sock


def process_entry(
    node_id: int,
    pid_int: int,
    broker_addr: str,
    ctrl_addr: str,
    fn_name: str,
    args: tuple[Any, ...],
    scope: dict[str, Callable[..., Any]],
) -> None:
    """Entry point for each spawned OS process. Must be module-level to be picklable."""

    ctx = zmq.Context()
    pid = Pid(node_id=node_id, id=pid_int)
    dealer = _connect_dealer(ctx, pid, broker_addr)
    ctrl = _connect_dealer(ctx, pid, ctrl_addr)

    try:
        fn = scope[fn_name]
        complete(_primed(fn(*args), ctrl), **make_handlers(pid, dealer, ctrl))
        _on_normal_exit(pid, ctrl)
    except Exception as err:  # noqa: BLE001
        _on_crash(pid, ctrl, err)
    finally:
        _on_exit(dealer, ctrl, ctx)
