"""Integration tests for EReceiveTimeout."""

import time
from collections.abc import Generator
from typing import Any

from tertius.effects import EEmit, EReceiveTimeout, ESelf, ESpawn
from tertius.genserver import mcast
from tertius.types import CastMsg, Envelope, Pid
from tertius.vm import run


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def send_after_delay(
    target_pid_bytes: bytes, delay_ms: int, body: Any
) -> Generator[Any, Any, None]:
    """Sleep then send a message. Uses a busy-wait to avoid importing time in generator."""

    time.sleep(delay_ms / 1000)
    yield from mcast(Pid.from_bytes(target_pid_bytes), body)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def _root_no_message() -> Generator[Any, Any, None]:
    result = yield EReceiveTimeout(timeout_ms=50)
    yield EEmit(result)


_SCOPE = {"send_after_delay": send_after_delay}


def test_receive_timeout_returns_none_when_no_message():
    """Proves that EReceiveTimeout returns None when no message arrives within the window."""

    result = next(run(_root_no_message, scope=_SCOPE))
    assert result is None


def _root_message_arrives_in_time() -> Generator[Any, Any, None]:
    me: Pid = yield ESelf()

    yield ESpawn(
        fn_name="send_after_delay",
        args=(bytes(me), 20, "on-time"),
    )

    envelope: Envelope | None = yield EReceiveTimeout(timeout_ms=500)

    match envelope:
        case Envelope(body=CastMsg(body=body)):
            yield EEmit(body)
        case None:
            yield EEmit("timed-out")


def test_receive_timeout_returns_message_when_it_arrives():
    """Proves that EReceiveTimeout returns the envelope when a message arrives before the deadline."""

    result = next(run(_root_message_arrives_in_time, scope=_SCOPE))
    assert result == "on-time"
