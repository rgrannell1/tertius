from collections.abc import Generator
from itertools import count
from typing import Any

from tertius.effects import EReceive, EReceiveTimeout, ESend
from tertius.types import CallMsg, CastMsg, Envelope, Pid, ReplyMsg

_ref_counter = count()


class GenServer[StateT]:
    """Base class for stateful processes. Subclass this to implement a process."""

    def init(self, *args: Any) -> StateT:
        raise NotImplementedError

    def handle_cast(self, state: StateT, body: Any) -> Generator[Any, Any, StateT]:
        """Handle a cast message. Return the new state."""

        yield

    def handle_call(
        self, state: StateT, body: Any
    ) -> Generator[Any, Any, tuple[StateT, Any]]:
        """Handle a call message. Return the new state and the reply to the caller"""

        raise NotImplementedError
        yield

    def loop(self, *args: Any) -> Generator[EReceive | ESend, Envelope | None, None]:
        """The main loop for the process. Receive messages, and handle them appropriately."""

        state = self.init(*args)

        while True:
            envelope: Envelope = yield EReceive()

            match envelope.body:
                case CastMsg(body=body):
                    state = yield from self.handle_cast(state, body)

                case CallMsg(ref=ref, body=body):
                    state, reply = yield from self.handle_call(state, body)
                    yield ESend(envelope.sender, ReplyMsg(ref=ref, body=reply))


def mcall(pid: Pid, body: Any) -> Generator[ESend | EReceive, None | Envelope, Any]:
    """Synchronous request — sends a CallMsg and blocks until a matching ReplyMsg arrives"""

    ref = next(_ref_counter)

    yield ESend(pid, CallMsg(ref=ref, body=body))

    while True:
        envelope: Envelope = yield EReceive()

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
