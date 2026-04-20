from collections.abc import Generator
from typing import Any

from hypothesis import given
from hypothesis import strategies as st
from orbis import complete

from tertius.genserver import GenServer, mcall, mcall_timeout, mcast
from tertius.effects import EReceive, EReceiveTimeout, ESend
from tertius.types import CallMsg, CastMsg, Envelope, Pid, ReplyMsg


# ---------------------------------------------------------------------------
# A minimal Counter GenServer used across all tests
# ---------------------------------------------------------------------------


class Counter(GenServer[int]):
    def init(self, initial: int = 0, *_: Any) -> int:
        return initial

    def handle_cast(self, state: int, body: Any) -> Generator[Any, Any, int]:
        match body:
            case ("inc", n):
                return state + n
            case _:
                return state
        yield

    def handle_call(
        self, state: int, body: Any
    ) -> Generator[Any, Any, tuple[int, Any]]:
        match body:
            case "get":
                return state, state
        raise NotImplementedError(body)
        yield


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SENDER = Pid(99)


def drive(server: GenServer, initial: Any, messages: list[Any]) -> list[Any]:
    """Drive a GenServer loop with a fixed sequence of messages.

    Returns whatever the server sent back. Stops cleanly when messages run out.
    """

    inbox = [Envelope(sender=SENDER, body=msg) for msg in messages]
    sent: list[Any] = []

    try:
        complete(
            server.loop(initial),
            receive=lambda effect: inbox.pop(0),
            send=lambda effect: sent.append(effect.body),
        )
    except IndexError:
        pass  # inbox exhausted — expected termination

    return sent


# ---------------------------------------------------------------------------
# Initialisation
# ---------------------------------------------------------------------------


def test_init_sets_initial_state():
    """Proves that State from a get call immediately after init equals the initial value."""

    sent = drive(Counter(), 42, [CallMsg(ref=0, body="get")])
    assert sent == [ReplyMsg(ref=0, body=42)]


# ---------------------------------------------------------------------------
# handle_cast
# ---------------------------------------------------------------------------


@given(st.integers(min_value=0, max_value=1000))
def test_single_increment(n):
    """Proves that increment message cast updates state by the correct amount."""

    sent = drive(
        Counter(),
        0,
        [
            CastMsg(body=("inc", n)),
            CallMsg(ref=0, body="get"),
        ],
    )
    assert sent == [ReplyMsg(ref=0, body=n)]


@given(st.lists(st.integers(min_value=0, max_value=100), min_size=1, max_size=20))
def test_increments_accumulate(increments):
    """Proves that State after N increments equals the sum of all increments."""

    messages = [CastMsg(body=("inc", n)) for n in increments] + [
        CallMsg(ref=0, body="get")
    ]
    sent = drive(Counter(), 0, messages)
    assert sent == [ReplyMsg(ref=0, body=sum(increments))]


def test_unknown_cast_leaves_state_unchanged():
    """Proves that an unrecognised cast body does not change state."""

    sent = drive(
        Counter(),
        7,
        [
            CastMsg(body="unknown"),
            CallMsg(ref=0, body="get"),
        ],
    )
    assert sent == [ReplyMsg(ref=0, body=7)]


# ---------------------------------------------------------------------------
# handle_call
# ---------------------------------------------------------------------------


def test_call_reply_matches_state():
    """Proves that the reply from a get call equals the current state."""

    sent = drive(Counter(), 5, [CallMsg(ref=1, body="get")])
    assert sent == [ReplyMsg(ref=1, body=5)]


def test_call_ref_is_preserved():
    """Proves that ReplyMsg carries the same ref as the CallMsg that triggered it."""

    ref = 12345
    sent = drive(Counter(), 0, [CallMsg(ref=ref, body="get")])
    assert sent[0].ref == ref


def test_state_unchanged_after_call():
    """Proves two gets are idempotent"""

    sent = drive(
        Counter(),
        3,
        [
            CallMsg(ref=0, body="get"),
            CallMsg(ref=1, body="get"),
        ],
    )
    assert sent[0].body == sent[1].body == 3


# ---------------------------------------------------------------------------
# call() and cast() helpers
# ---------------------------------------------------------------------------


def test_cast_helper_sends_castmsg():
    """Proves that cast() yields an ESend wrapping a CastMsg."""

    sent = []
    complete(
        mcast(SENDER, ("inc", 1)),
        send=lambda effect: sent.append(effect.body),
    )
    assert sent == [CastMsg(body=("inc", 1))]


def test_call_helper_returns_reply_body():
    """Proves that call() returns the body of the matching ReplyMsg."""

    ref_holder: list[int] = []

    def stub_send(effect: ESend) -> None:
        assert isinstance(effect.body, CallMsg)
        ref_holder.append(effect.body.ref)

    def stub_receive(effect: EReceive) -> Envelope:
        return Envelope(sender=SENDER, body=ReplyMsg(ref=ref_holder[0], body="pong"))

    result = complete(
        mcall(SENDER, "ping"),
        send=stub_send,
        receive=stub_receive,
    )
    assert result == "pong"


def test_call_helper_ignores_non_matching_replies():
    """Proves that call() discards envelopes whose ref does not match, then waits for the correct one."""

    ref_holder: list[int] = []
    call_count = 0

    def stub_send(effect: ESend) -> None:
        ref_holder.append(effect.body.ref)

    def stub_receive(effect: EReceive) -> Envelope:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return Envelope(
                sender=SENDER, body=ReplyMsg(ref=ref_holder[0] + 1, body="wrong")
            )
        return Envelope(sender=SENDER, body=ReplyMsg(ref=ref_holder[0], body="right"))

    result = complete(
        mcall(SENDER, "ping"),
        send=stub_send,
        receive=stub_receive,
    )
    assert result == "right"
    assert call_count == 2


def test_call_refs_are_unique():
    """Proves that successive call() invocations use distinct refs."""

    refs: list[int] = []

    def stub_send(effect: ESend) -> None:
        refs.append(effect.body.ref)

    def stub_receive(effect: EReceive) -> Envelope:
        return Envelope(sender=SENDER, body=ReplyMsg(ref=refs[-1], body=None))

    for _ in range(3):
        complete(mcall(SENDER, "ping"), send=stub_send, receive=stub_receive)

    assert len(set(refs)) == 3


# ---------------------------------------------------------------------------
# call_timeout() helper
# ---------------------------------------------------------------------------


def test_call_timeout_returns_reply_when_server_responds():
    """Proves that call_timeout() returns the reply body when it arrives in time."""

    ref_holder: list[int] = []

    def stub_send(effect: ESend) -> None:
        ref_holder.append(effect.body.ref)

    def stub_receive_timeout(effect: EReceiveTimeout) -> Envelope:
        return Envelope(sender=SENDER, body=ReplyMsg(ref=ref_holder[0], body="pong"))

    result = complete(
        mcall_timeout(SENDER, "ping", timeout_ms=1000),
        send=stub_send,
        receive_timeout=stub_receive_timeout,
    )
    assert result == "pong"


def test_call_timeout_returns_none_on_timeout():
    """Proves that call_timeout() returns None when no reply arrives within the deadline."""

    def stub_send(effect: ESend) -> None:
        pass

    def stub_receive_timeout(effect: EReceiveTimeout) -> None:
        return None

    result = complete(
        mcall_timeout(SENDER, "ping", timeout_ms=50),
        send=stub_send,
        receive_timeout=stub_receive_timeout,
    )
    assert result is None
