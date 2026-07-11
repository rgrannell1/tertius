"""Worker entry point and lifecycle — runs a generator as an OS-process or thread worker."""

import sys
import threading
import traceback
from collections.abc import Generator
from typing import Any

import zmq
from orbis import complete

from tertius.constants import Cmd
from tertius.exceptions import KilledError, NormalExitError
from tertius.types import Pid
from tertius.vm.broker_utils import ctrl_send
from tertius.vm.messages import crash
from tertius.vm.process_handlers import make_handlers
from tertius.vm.transport import make_dealer
from tertius.vm.worker_types import WorkerSpec


def advance_gen(gen: Any, send_val: Any, exc: BaseException | None) -> tuple[Any, bool]:
    """Step the generator with a value or exception; return (next effect, finished)."""

    try:
        if exc is not None:
            return gen.throw(exc), False
        return gen.send(send_val), False
    except StopIteration:
        return None, True


def check_killed(gen: Any, pid: Pid, kill_event: threading.Event | None) -> None:
    """Enforce a cooperative kill: close the generator and raise KilledError."""

    if kill_event is None or not kill_event.is_set():
        return

    # close() runs the generator's finally blocks; the kill still wins even if
    # the generator caught the KilledError delivered through its receive.
    gen.close()
    raise KilledError(pid)


def prime_gen(gen: Any, ctrl: "zmq.Socket[bytes]") -> tuple[Any, bool]:
    """Step the generator once, sending Cmd.READY only after it survives its first step.

    If the generator raises before yielding, no Cmd.READY is sent — the broker detects
    the dead worker via its recv timeout.
    """

    try:
        effect = next(gen)
    except StopIteration:
        ctrl_send(ctrl, Cmd.READY)
        return None, True

    ctrl_send(ctrl, Cmd.READY)
    return effect, False


def _primed_fn(
    gen: Any,
    ctrl: "zmq.Socket[bytes]",
    pid: Pid,
    kill_event: threading.Event | None,
) -> Generator[Any, Any, None]:
    """Drive a primed generator, checking the kill event before every step so a
    cooperative kill lands at the next effect yield."""

    effect, finished = prime_gen(gen, ctrl)
    while not finished:
        check_killed(gen, pid, kill_event)
        try:
            send_val = yield effect
        except BaseException as exc:  # noqa: BLE001
            effect, finished = advance_gen(gen, None, exc)
        else:
            effect, finished = advance_gen(gen, send_val, None)


def _on_normal_exit(pid: Pid, ctrl: "zmq.Socket[bytes]") -> None:
    """Handle normal process exit: notify the broker."""

    ctrl_send(ctrl, *crash.encode(NormalExitError(pid)))


def _on_crash(pid: Pid, ctrl: "zmq.Socket[bytes]", err: Exception) -> None:
    """Handle process crash: log the error and notify the broker."""

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


def drive_worker_gen(
    spec: WorkerSpec,
    dealer: "zmq.Socket[bytes]",
    ctrl: "zmq.Socket[bytes]",
    kill_event: threading.Event | None,
) -> None:
    """Run the worker generator to completion, reporting a normal exit unless killed."""

    gen = spec.scope[spec.fn_name](*spec.args)
    handlers = make_handlers(spec.pid, dealer, ctrl, kill_event)
    complete(_primed_fn(gen, ctrl, spec.pid, kill_event), **handlers)

    if kill_event is None or not kill_event.is_set():
        _on_normal_exit(spec.pid, ctrl)


def run_worker(spec: WorkerSpec, kill_event: threading.Event | None) -> None:
    """Run a worker generator against the broker until it exits, crashes, or is killed."""

    ctx: zmq.Context[zmq.Socket[bytes]] = zmq.Context()
    dealer = make_dealer(ctx, spec.pid, spec.broker_addr, spec.transport)
    ctrl = make_dealer(ctx, spec.pid, spec.ctrl_addr, spec.transport)

    try:
        drive_worker_gen(spec, dealer, ctrl, kill_event)
    except KilledError:
        # The broker initiated the kill and has already tombstoned this pid;
        # reporting a crash here would double-record the death.
        pass
    except Exception as err:  # noqa: BLE001
        _on_crash(spec.pid, ctrl, err)
    finally:
        _on_exit(dealer, ctrl, ctx)


def process_entry(spec: WorkerSpec) -> None:
    """Entry point for each spawned OS process. Must be module-level to be picklable."""

    run_worker(spec, None)
