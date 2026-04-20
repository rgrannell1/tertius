import sys
import traceback
from collections.abc import Callable
from typing import Any

import zmq
from orbis import complete

from tertius.constants import READY
from tertius.types import Pid
from tertius.vm.messages import crash
from tertius.vm.process_handlers import make_handlers



def _primed(gen: Any, ctrl: "zmq.Socket[bytes]") -> Any:
    """Wrap a generator so READY is sent only after it survives its first step.

    If the generator raises before yielding, no READY is sent — the broker detects
    the dead process via its recv timeout.
    """

    try:
        effect = next(gen)
    except StopIteration:
        ctrl.send_multipart([READY])
        ctrl.recv_multipart()
        return

    ctrl.send_multipart([READY])
    ctrl.recv_multipart()

    pending_throw: BaseException | None = None

    while True:
        try:
            send_val = yield effect
            pending_throw = None
        except BaseException as exc:
            send_val = None
            pending_throw = exc
        try:
            if pending_throw is not None:
                effect = gen.throw(
                    type(pending_throw), pending_throw, pending_throw.__traceback__
                )
            else:
                effect = gen.send(send_val)
        except StopIteration:
            return


def _on_crash(pid: Pid, ctrl: "zmq.Socket[bytes]", err: Exception) -> None:
    """Handle process crashes: log the error and notify the broker."""

    print(f"[tertius] process {pid} crashed: {err}", file=sys.stderr, flush=True)
    traceback.print_exc(file=sys.stderr)
    ctrl.send_multipart(crash.encode(err))
    ctrl.recv_multipart()


def _on_exit(dealer: "zmq.Socket[bytes]", ctrl: "zmq.Socket[bytes]", ctx: "zmq.Context[bytes]") -> None:
    """Process cleanup: close sockets and terminate context."""

    dealer.close()
    ctrl.close()
    ctx.term()


def _connect_dealer(
    ctx: "zmq.Context[bytes]", pid: Pid, addr: str
) -> "zmq.Socket[bytes]":
    """Connect a DEALER socket to the given address, using the PID as identity."""

    sock: zmq.Socket[bytes] = ctx.socket(zmq.DEALER)
    sock.identity = bytes(pid)
    sock.connect(addr)
    return sock


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
    dealer = _connect_dealer(ctx, pid, broker_addr)
    ctrl = _connect_dealer(ctx, pid, ctrl_addr)

    try:
        fn = scope[fn_name]
        complete(_primed(fn(*args), ctrl), **make_handlers(pid, dealer, ctrl))
    except Exception as err:
        _on_crash(pid, ctrl, err)
    finally:
        _on_exit(dealer, ctrl, ctx)
