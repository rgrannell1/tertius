# GenServer abstraction — builds stateful process loops from generator handler functions.
from functools import partial
from itertools import count
from typing import Any

from tertius.effects import EReceive, EReceiveTimeout, ESend
from tertius.genserver_types import (
    CallHandler,
    CastHandler,
    InfoHandler,
    InitHandler,
    McallGen,
    McallTimeoutGen,
    McastGen,
    ServerFactory,
    ServerGen,
)
from tertius.types import CallMsg, CastMsg, Envelope, Pid, ReplyMsg

_ref_counter = count()


def _gen_server_loop[StateT](
    init: InitHandler[StateT],
    handle_cast: CastHandler[StateT] | None,
    handle_call: CallHandler[StateT],
    handle_info: InfoHandler[StateT] | None,
    *args: Any,
) -> ServerGen:
    state = yield from init(*args)

    while True:
        envelope = yield EReceive()
        if envelope is None:
            raise RuntimeError("EReceive yielded None — broker sent no envelope")

        match envelope.body:
            case CastMsg(body=body):
                if handle_cast is not None:
                    state = yield from handle_cast(state, body)

            case CallMsg(ref=ref, body=body):
                state, reply = yield from handle_call(state, body)
                yield ESend(envelope.sender, ReplyMsg(ref=ref, body=reply))

            case _:
                if handle_info is not None:
                    state = yield from handle_info(state, envelope.body)


def gen_server[StateT](
    init: InitHandler[StateT],
    *,
    handle_cast: CastHandler[StateT] | None = None,
    handle_call: CallHandler[StateT],
    handle_info: InfoHandler[StateT] | None = None,
) -> ServerFactory:
    """Build a stateful process loop from generator handler functions.

    Returns a callable that, when called with init args, yields a generator
    suitable for running inside a tertius process.
    """

    return partial(_gen_server_loop, init, handle_cast, handle_call, handle_info)


def mcall(pid: Pid, body: Any) -> McallGen:
    """Synchronous request — sends a CallMsg and blocks until a matching ReplyMsg arrives"""

    ref = next(_ref_counter)

    yield ESend(pid, CallMsg(ref=ref, body=body))

    while True:
        envelope = yield EReceive()
        assert envelope is not None

        if isinstance(envelope.body, ReplyMsg) and envelope.body.ref == ref:
            return envelope.body.body


def mcall_timeout(
    pid: Pid, body: Any, timeout_ms: int
) -> McallTimeoutGen:
    """Synchronous request with a deadline — returns the reply body, or None on timeout."""

    ref = next(_ref_counter)

    yield ESend(pid, CallMsg(ref=ref, body=body))

    while True:
        envelope: Envelope | None = yield EReceiveTimeout(timeout_ms=timeout_ms)

        if envelope is None:
            return None

        if isinstance(envelope.body, ReplyMsg) and envelope.body.ref == ref:
            return envelope.body.body


def mcast(pid: Pid, body: Any) -> McastGen:
    """Fire-and-forget — no reply expected."""

    yield ESend(pid, CastMsg(body=body))
