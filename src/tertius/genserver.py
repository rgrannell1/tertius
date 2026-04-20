import inspect
from collections.abc import Callable, Generator
from itertools import count
from typing import Any

from tertius.effects import EReceive, EReceiveTimeout, ESend
from tertius.types import CallMsg, CastMsg, Envelope, Pid, ReplyMsg

_ref_counter = count()


def gen_server[StateT](
    init: Callable[..., StateT],
    *,
    handle_cast: Callable[[StateT, Any], Any] | None = None,
    handle_call: Callable[[StateT, Any], Any],
    handle_info: Callable[[StateT, Any], Any] | None = None,
) -> Callable[..., Generator[EReceive | ESend, Envelope | None, None]]:
    """Build a stateful process loop from handler functions.

    Returns a callable that, when called with init args, yields a generator
    suitable for running inside a tertius process.
    """

    def loop(*args: Any) -> Generator[EReceive | ESend, Envelope | None, None]:
        state = init(*args)

        while True:
            envelope = yield EReceive()
            if envelope is None:
                raise RuntimeError("EReceive yielded None — broker sent no envelope")

            match envelope.body:
                case CastMsg(body=body):
                    if handle_cast is not None:
                        result = handle_cast(state, body)
                        state = (
                            (yield from result)
                            if inspect.isgenerator(result)
                            else result
                        )

                case CallMsg(ref=ref, body=body):
                    result = handle_call(state, body)
                    state, reply = (
                        (yield from result) if inspect.isgenerator(result) else result
                    )
                    yield ESend(envelope.sender, ReplyMsg(ref=ref, body=reply))

                case _:
                    if handle_info is not None:
                        result = handle_info(state, envelope.body)
                        state = (
                            (yield from result)
                            if inspect.isgenerator(result)
                            else result
                        )

    return loop


def mcall(pid: Pid, body: Any) -> Generator[ESend | EReceive, None | Envelope, Any]:
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
) -> Generator[ESend | EReceiveTimeout, None | Envelope, Any]:
    """Synchronous request with a deadline — returns the reply body, or None on timeout."""

    ref = next(_ref_counter)

    yield ESend(pid, CallMsg(ref=ref, body=body))

    while True:
        envelope: Envelope | None = yield EReceiveTimeout(timeout_ms=timeout_ms)

        if envelope is None:
            return None

        if isinstance(envelope.body, ReplyMsg) and envelope.body.ref == ref:
            return envelope.body.body


def mcast(pid: Pid, body: Any) -> Generator[ESend, None, None]:
    """Fire-and-forget — no reply expected."""

    yield ESend(pid, CastMsg(body=body))
