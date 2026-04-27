"""Integration tests for VM and broker lifecycle — clean shutdown after completion."""
import threading
from collections.abc import Generator
from typing import Any

import zmq

from tertius.effects import EEmit, ESelf, ESend, ESleep, ESpawn
from tertius.types import Pid
from tertius.vm import VM, run
from tertius.vm.broker import _run_data_loop

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def sleeping_job() -> Generator[Any, Any, None]:
    # Stays alive long enough that the broker is still serving it when the root exits.
    yield ESleep(ms=2000)


def root_with_background_spawn() -> Generator[Any, Any, None]:
    yield ESpawn(fn_name="sleeping_job")
    yield EEmit("started")


def flood_self() -> Generator[Any, Any, None]:
    """Sends 500 messages to self, keeping the data router busy during broker teardown."""
    me: Pid = yield ESelf()
    for _ in range(500):
        yield ESend(pid=me, body="flood")


def root_with_flood() -> Generator[Any, Any, None]:
    """Spawn several flood workers then exit immediately; the router is still active during teardown."""
    for _ in range(4):
        yield ESpawn(fn_name="flood_self")


_SCOPE = {"sleeping_job": sleeping_job, "flood_self": flood_self, "root_with_flood": root_with_flood}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_data_loop_exits_cleanly_when_send_raises_context_terminated():
    """Proves _run_data_loop exits cleanly when ContextTerminated fires during send_multipart.

    This is a unit-level reproduction of the shutdown race:
    recv_multipart() succeeds → ctx.term() is called → send_multipart() raises ContextTerminated.
    Before the fix the exception propagates uncaught; after the fix it returns cleanly.
    """

    class _TerminatesOnSend:
        """Mock router socket: returns one message from recv, raises ETERM on send."""

        def recv_multipart(self) -> list[bytes]:
            return [b"sender", b"target", b"sender_pid", b"body"]

        def send_multipart(self, _frames: list[bytes]) -> None:
            raise zmq.ZMQError(zmq.ETERM)

    # Before fix: ZMQError from send_multipart propagates uncaught.
    # After fix:  _run_data_loop returns cleanly.
    _run_data_loop(_TerminatesOnSend())


def test_no_unhandled_thread_exception_under_high_message_traffic():
    """Proves the data loop doesn't leak ContextTerminated to the thread machinery under load.

    Spawns a worker that sends 500 self-messages so the data router is saturated when
    the VM shuts down, making the recv→send race window likely to be hit.
    """

    thread_exceptions: list[BaseException] = []
    original_hook = threading.excepthook

    def _capture(args: threading.ExceptHookArgs) -> None:
        thread_exceptions.append(args.exc_value)

    threading.excepthook = _capture
    try:
        list(run(root_with_flood, scope=_SCOPE))
    finally:
        threading.excepthook = original_hook

    zmq_errors = [exc for exc in thread_exceptions if isinstance(exc, zmq.ZMQError)]
    assert not zmq_errors, f"Unhandled ZMQ errors in broker threads: {zmq_errors}"


def test_broker_context_is_terminated_after_vm_completes():
    """Proves the broker's zmq context is terminated when the VM finishes."""

    # Without the fix, the context is never explicitly terminated — Python's GC
    # eventually calls ctx.term() while broker threads still have open sockets,
    # causing SIGABRT (reliably seen on Python 3.14 in zahir2).
    # The fix has VM.start() call broker.stop() which terminates the context
    # before the VM goes out of scope.
    vm = VM(scope=_SCOPE)
    events = list(vm.start(root_with_background_spawn, ()))
    assert "started" in events
    assert vm._broker._ctx.closed, "Broker context should be terminated after VM completes"
