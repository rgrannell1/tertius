"""Integration tests for VM and broker lifecycle — clean shutdown after completion."""
import zmq
from collections.abc import Generator
from typing import Any

from tertius.effects import EEmit, ESleep, ESpawn
from tertius.vm import VM, run


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def sleeping_job() -> Generator[Any, Any, None]:
    # Stays alive long enough that the broker is still serving it when the root exits.
    yield ESleep(ms=2000)


def root_with_background_spawn() -> Generator[Any, Any, None]:
    yield ESpawn(fn_name="sleeping_job")
    yield EEmit("started")


_SCOPE = {"sleeping_job": sleeping_job}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_broker_context_is_terminated_after_vm_completes():
    """Proves the broker's zmq context is terminated when the VM finishes."""

    # Without the fix, the context is never explicitly terminated — Python's GC
    # eventually calls ctx.term() while broker threads still have open sockets,
    # causing SIGABRT (reliably seen on Python 3.14 in zahir2).
    # The fix has VM.start() call broker.stop() which terminates the context
    # before the VM goes out of scope.
    vm = VM(scope=_SCOPE)
    results = list(vm.start(root_with_background_spawn, ()))
    assert results == ["started"]
    assert vm._broker._ctx.closed, "Broker context should be terminated after VM completes"
