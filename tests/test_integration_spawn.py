"""Integration tests for ESpawn — process spawning and failure detection."""

from collections.abc import Generator
from typing import Any

import pytest

from tertius.effects import EEmit, ESpawn
from tertius.vm import run


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def immediate_return() -> Generator[Any, Any, None]:
    return
    yield


def emitting_job() -> Generator[Any, Any, None]:
    yield EEmit("hello")


def crashing_job() -> Generator[Any, Any, None]:
    raise RuntimeError("intentional crash")
    yield


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def _root_spawn_and_emit() -> Generator[Any, Any, None]:
    yield ESpawn(fn_name="immediate_return")
    yield EEmit("done")


def test_spawn_starts_process_successfully():
    """Proves ESpawn starts a process that runs to completion."""

    result = next(
        run(_root_spawn_and_emit, scope={"immediate_return": immediate_return})
    )
    assert result == "done"


def _root_spawn_unknown() -> Generator[Any, Any, None]:
    yield ESpawn(fn_name="no_such_fn")
    yield EEmit("done")


def test_spawn_unknown_fn_raises():
    """Proves ESpawn raises KeyError when fn_name is not in scope."""

    with pytest.raises(KeyError, match="no_such_fn"):
        next(run(_root_spawn_unknown, scope={}))


def _root_spawn_crashing() -> Generator[Any, Any, None]:
    yield ESpawn(fn_name="crashing_job")
    yield EEmit("done")


def test_spawn_process_that_crashes_raises_runtime_error():
    """Proves ESpawn raises RuntimeError when the spawned process dies before sending READY."""

    with pytest.raises(RuntimeError, match="crashing_job.*died before sending READY"):
        next(run(_root_spawn_crashing, scope={"crashing_job": crashing_job}))
