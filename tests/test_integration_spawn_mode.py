"""Integration tests for spawn modes — thread workers, per-spawn overrides, cooperative kill."""

from collections.abc import Generator
from typing import Any

import pytest

from tertius.constants import SpawnMode
from tertius.effects import EKill, EMonitor, EReceive, ESleep, ESpawn
from tertius.exceptions import ProcessCrashError
from tertius.types import Envelope, Pid

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def wait_forever() -> Generator[Any, Any, None]:
    yield EReceive()


def nap_repeatedly() -> Generator[Any, Any, None]:
    """Never receives — only yields short sleeps, so a kill can only land at a yield."""
    while True:
        yield ESleep(ms=10)


_SCOPE = {"wait_forever": wait_forever, "nap_repeatedly": nap_repeatedly}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def _root_spawn_override(mode: SpawnMode) -> Generator[Any, Any, Any]:
    worker: Pid = yield ESpawn(fn_name="wait_forever", mode=mode)
    yield EMonitor(pid=worker)
    yield EKill(pid=worker)
    envelope: Envelope = yield EReceive()
    return envelope.body


@pytest.mark.parametrize("mode", [SpawnMode.PROCESS, SpawnMode.THREAD])
def test_per_spawn_mode_override(collect, mode):
    """Proves ESpawn's mode field overrides the VM default, with identical kill semantics."""

    result, _ = collect(_root_spawn_override, mode, scope=_SCOPE)
    assert isinstance(result, ProcessCrashError)
    assert str(result.reason) == "killed"


def test_spawn_events_record_the_mode(collect):
    """Proves spawn:started telemetry carries the spawn mode dimension."""

    _, events = collect(_root_spawn_override, SpawnMode.THREAD, scope=_SCOPE)
    started = [ev for ev in events if hasattr(ev, "dim") and ev.dim("tag") == "spawn:started"]
    assert started, "no spawn:started event emitted"
    assert started[0].dim("mode") == SpawnMode.THREAD.value


def _root_kill_busy_worker() -> Generator[Any, Any, Any]:
    worker: Pid = yield ESpawn(fn_name="nap_repeatedly")
    yield EMonitor(pid=worker)
    yield EKill(pid=worker)
    envelope: Envelope = yield EReceive()
    return envelope.body


def test_kill_lands_on_worker_that_never_receives(collect):
    """Proves cooperative kill reaches a worker that only sleeps and never blocks in receive."""

    result, _ = collect(_root_kill_busy_worker, scope=_SCOPE)
    assert isinstance(result, ProcessCrashError)
    assert str(result.reason) == "killed"


def _root_spawn_unpicklable() -> Generator[Any, Any, Any]:
    yield ESpawn(fn_name="local_fn")


def test_unpicklable_scope_fails_spawn_in_both_modes(collect):
    """Proves a scope function that cannot be pickled fails the spawn identically per mode."""

    def local_fn() -> Generator[Any, Any, None]:
        yield EReceive()

    with pytest.raises(Exception, match="pickle|Pickling"):
        collect(_root_spawn_unpicklable, scope={"local_fn": local_fn})
