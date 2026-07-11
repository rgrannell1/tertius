# GenServer abstraction — builds stateful process loops from generator handler functions.
from collections.abc import Generator
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
    ServerHandlers,
)
from tertius.types import CallMsg, CastMsg, Envelope, Pid, ReplyMsg

_ref_counter = count()


def dispatch_msg[StateT](
    handlers: ServerHandlers[StateT],
    state: StateT,
    envelope: Envelope,
) -> Generator[Any, Any, StateT]:
    """Route one envelope to the matching handler, returning the updated state."""

    match envelope.body:
        case CastMsg(body=body):
            if handlers.handle_cast is None:
                return state
            return (yield from handlers.handle_cast(state, body))

        case CallMsg(ref=ref, body=body):
            state, reply = yield from handlers.handle_call(state, body)
            yield ESend(envelope.sender, ReplyMsg(ref=ref, body=reply))
            return state

        case _:
            if handlers.handle_info is None:
                return state
            return (yield from handlers.handle_info(state, envelope.body))


def _gen_server_loop[StateT](
    handlers: ServerHandlers[StateT],
    *args: Any,
) -> ServerGen:
    state = yield from handlers.init(*args)

    while True:
        envelope = yield EReceive()
        if envelope is None:
            raise RuntimeError("EReceive yielded None — broker sent no envelope")

        state = yield from dispatch_msg(handlers, state, envelope)


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

    handlers = ServerHandlers(
        init=init, handle_cast=handle_cast, handle_call=handle_call, handle_info=handle_info
    )
    return partial(_gen_server_loop, handlers)


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
