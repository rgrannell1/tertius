"""Integration tests for join() — a separately started OS process joining a VM over TCP."""

import multiprocessing
import socket
from collections.abc import Generator
from typing import Any

import pytest

from tertius.effects import EReceive, ERegister, ESend, ESleep, EWhereis
from tertius.exceptions import JoinTimeoutError
from tertius.genserver import mcall, mcast
from tertius.transport_types import TcpTransport
from tertius.types import CallMsg, CastMsg, Envelope, Pid, ReplyMsg
from tertius.vm.join import join

_SPAWN_CTX = multiprocessing.get_context("spawn")

_ROOT_NAME = "join-test-root"
_WHEREIS_POLL_MS = 50

# ---------------------------------------------------------------------------
# Fixtures (module-level so the spawn context can pickle them)
# ---------------------------------------------------------------------------


def free_ports(count: int) -> list[int]:
    """Find free localhost TCP ports by binding then releasing them.

    Note: there is a TOCTOU window between close() and ZMQ's bind(). In practice
    this is negligible in a single-machine test environment, but the race is real.
    SO_REUSEADDR reduces TIME_WAIT holdover but does not eliminate the window.
    """

    sockets = [socket.socket() for _ in range(count)]
    for sock in sockets:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("127.0.0.1", 0))
    ports = [sock.getsockname()[1] for sock in sockets]
    for sock in sockets:
        sock.close()
    return ports


def _root_serve_one_call() -> Generator[Any, Any, str]:
    """Register a name, answer one call from the joiner, then wait for its done signal."""

    yield ERegister(name=_ROOT_NAME)

    while True:
        envelope: Envelope = yield EReceive()
        match envelope.body:
            case CallMsg(ref=ref, body="ping"):
                yield ESend(envelope.sender, ReplyMsg(ref=ref, body="pong"))
            case CastMsg(body="done"):
                return "root-done"


def _joiner_body() -> Generator[Any, Any, str]:
    """Resolve the root by name, call it, then signal completion."""

    while True:
        root_pid: Pid | None = yield EWhereis(name=_ROOT_NAME)
        if root_pid is not None:
            break
        yield ESleep(ms=_WHEREIS_POLL_MS)

    reply = yield from mcall(root_pid, "ping")
    yield from mcast(root_pid, "done")
    return reply


def _run_joiner(data_port: int, control_port: int, results: Any) -> None:
    """Joiner OS-process entry: join the broker over TCP and report the call reply."""

    transport = TcpTransport(host="127.0.0.1", data_port=data_port, control_port=control_port)
    reply = join(
        _joiner_body,
        transport=transport,
        handshake_timeout_ms=10_000,
        recv_timeout_ms=10_000,
    )
    results.put(reply)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def _event_tags(events: list[Any]) -> set[str]:
    tags: set[str] = set()
    for event in events:
        dims = getattr(event, "dims", None) or {}
        tags.update(dims.get("tag", []))
    return tags


def test_joined_process_calls_into_vm_over_tcp(collect: Any):
    """Proves a separately started OS process can join over TCP, resolve a name, and mcall it."""

    data_port, control_port = free_ports(2)
    results = _SPAWN_CTX.Queue()
    joiner = _SPAWN_CTX.Process(
        target=_run_joiner, args=(data_port, control_port, results), daemon=True
    )
    joiner.start()

    transport = TcpTransport(host="127.0.0.1", data_port=data_port, control_port=control_port)
    root_result, events = collect(_root_serve_one_call, transport=transport)

    assert root_result == "root-done"
    assert results.get(timeout=10) == "pong"
    assert "process:joined" in _event_tags(events)
    joiner.join(timeout=10)


def test_join_times_out_when_no_broker_listens():
    """Proves join() fails fast with JoinTimeoutError when no broker is reachable."""

    data_port, control_port = free_ports(2)
    transport = TcpTransport(host="127.0.0.1", data_port=data_port, control_port=control_port)

    with pytest.raises(JoinTimeoutError):
        join(_joiner_body, transport=transport, handshake_timeout_ms=300)
