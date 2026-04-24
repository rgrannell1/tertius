"""Tests for gen_server — stateful process loops built from handler functions."""

from collections.abc import Callable, Generator
from dataclasses import dataclass
from functools import partial
from typing import Any, ClassVar, LiteralString

import pytest
from hypothesis import given
from hypothesis import strategies as st
from orbis import Effect, UnhandledEffect, complete

from tertius.genserver import gen_server, mcall, mcall_timeout, mcast
from tertius.effects import EReceive, EReceiveTimeout, ESend
from tertius.types import CallMsg, CastMsg, Envelope, Pid, ReplyMsg


# ---------------------------------------------------------------------------
# A minimal custom effect used in effectful-handler tests
# ---------------------------------------------------------------------------


@dataclass
class EDouble(Effect[int]):
    """Test effect — handler returns double the given value."""

    tag: ClassVar[LiteralString] = "double"
    value: int


def _handle_double(effect: EDouble) -> int:
    return effect.value * 2


# ---------------------------------------------------------------------------
# A minimal counter process used across all tests
# ---------------------------------------------------------------------------


def _init(initial: int = 0) -> Generator[Any, Any, int]:
    return initial
    yield


def _cast(state: int, body: Any) -> Generator[Any, Any, int]:
    match body:
        case ("inc", n):
            return state + n
        case _:
            return state
    yield


def _call(state: int, body: Any) -> Generator[Any, Any, tuple[int, Any]]:
    match body:
        case "get":
            return state, state
    raise NotImplementedError(body)
    yield


counter = gen_server(init=_init, handle_cast=_cast, handle_call=_call)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SENDER = Pid(node_id=0, id=99)


def _pop_inbox(inbox: list, effect: EReceive) -> Envelope:
    return inbox.pop(0)


def _record_send(sent: list, effect: ESend) -> None:
    sent.append(effect.body)


def drive(
    server: Callable[..., Generator],
    initial: Any,
    messages: list[Any],
    **extra_handlers: Any,
) -> list[Any]:
    """Drive a gen_server loop with a fixed sequence of messages.

    Returns whatever the server sent back. Stops cleanly when messages run out.
    Extra keyword arguments are forwarded to complete() as additional effect handlers.
    """

    inbox = [Envelope(sender=SENDER, body=msg) for msg in messages]
    sent: list[Any] = []

    try:
        complete(
            server(initial),
            receive=partial(_pop_inbox, inbox),
            send=partial(_record_send, sent),
            **extra_handlers,
        )
    except IndexError:
        pass  # inbox exhausted — expected termination

    return sent


# ---------------------------------------------------------------------------
# Initialisation
# ---------------------------------------------------------------------------


def test_init_sets_initial_state():
    """Proves that State from a get call immediately after init equals the initial value."""

    sent = drive(counter, 42, [CallMsg(ref=0, body="get")])
    assert sent == [ReplyMsg(ref=0, body=42)]


# ---------------------------------------------------------------------------
# handle_cast
# ---------------------------------------------------------------------------


@given(st.integers(min_value=0, max_value=1000))
def test_single_increment(increment):
    """Proves that increment message cast updates state by the correct amount."""

    sent = drive(
        counter,
        0,
        [
            CastMsg(body=("inc", increment)),
            CallMsg(ref=0, body="get"),
        ],
    )
    assert sent == [ReplyMsg(ref=0, body=increment)]


@given(st.lists(st.integers(min_value=0, max_value=100), min_size=1, max_size=20))
def test_increments_accumulate(increments):
    """Proves that State after N increments equals the sum of all increments."""

    messages = [CastMsg(body=("inc", n)) for n in increments] + [
        CallMsg(ref=0, body="get")
    ]
    sent = drive(counter, 0, messages)
    assert sent == [ReplyMsg(ref=0, body=sum(increments))]


def test_unknown_cast_leaves_state_unchanged():
    """Proves that an unrecognised cast body does not change state."""

    sent = drive(
        counter,
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

    sent = drive(counter, 5, [CallMsg(ref=1, body="get")])
    assert sent == [ReplyMsg(ref=1, body=5)]


def test_call_ref_is_preserved():
    """Proves that ReplyMsg carries the same ref as the CallMsg that triggered it."""

    ref = 12345
    sent = drive(counter, 0, [CallMsg(ref=ref, body="get")])
    assert sent[0].ref == ref


def test_state_unchanged_after_call():
    """Proves two gets are idempotent"""

    sent = drive(
        counter,
        3,
        [
            CallMsg(ref=0, body="get"),
            CallMsg(ref=1, body="get"),
        ],
    )
    assert sent[0].body == sent[1].body == 3


# ---------------------------------------------------------------------------
# handler exceptions
# ---------------------------------------------------------------------------


def _exploding_cast(state: int, body: Any) -> Generator[Any, Any, int]:
    raise ValueError("cast exploded")
    yield


def _init_zero(*_: Any) -> Generator[Any, Any, int]:
    return 0
    yield


def _return_state(state: int, _body: Any) -> Generator[Any, Any, tuple[int, int]]:
    return state, state
    yield


def test_handle_cast_exception_propagates():
    """Proves that an exception raised in handle_cast propagates out of the loop."""

    server = gen_server(
        init=_init_zero,
        handle_cast=_exploding_cast,
        handle_call=_return_state,
    )

    with pytest.raises(ValueError, match="cast exploded"):
        drive(server, 0, [CastMsg(body="anything")])


def _exploding_call(state: int, body: Any) -> Generator[Any, Any, tuple[int, Any]]:
    raise RuntimeError("call exploded")
    yield


def test_handle_call_exception_propagates():
    """Proves that an exception raised in handle_call propagates out of the loop."""

    server = gen_server(
        init=_init_zero,
        handle_call=_exploding_call,
    )

    with pytest.raises(RuntimeError, match="call exploded"):
        drive(server, 0, [CallMsg(ref=0, body="anything")])


# ---------------------------------------------------------------------------
# call() and cast() helpers
# ---------------------------------------------------------------------------


def _record_body(sent: list, effect: ESend) -> None:
    sent.append(effect.body)


def test_cast_helper_sends_castmsg():
    """Proves that cast() yields an ESend wrapping a CastMsg."""

    sent = []
    complete(
        mcast(SENDER, ("inc", 1)),
        send=partial(_record_body, sent),
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


# ---------------------------------------------------------------------------
# Effectful handlers — cast, call, and init may yield effects
# ---------------------------------------------------------------------------


def _doubling_cast(state: int, body: Any) -> Generator[Any, Any, int]:
    # generator cast: yields EDouble to compute the increment, then adds it to state
    match body:
        case ("double_add", n):
            doubled = yield EDouble(value=n)
            return state + doubled
        case _:
            return state


def _doubling_call(state: int, body: Any) -> Generator[Any, Any, tuple[int, Any]]:
    # generator call: yields EDouble to compute the reply value
    match body:
        case "doubled_get":
            doubled = yield EDouble(value=state)
            return state, doubled
        case "get":
            return state, state
    raise NotImplementedError(body)


def _doubling_init(initial: int) -> Generator[Any, Any, int]:
    # generator init: yields EDouble to compute initial state
    doubled = yield EDouble(value=initial)
    return doubled


_effectful_cast_server = gen_server(
    init=_init, handle_cast=_doubling_cast, handle_call=_call
)
_effectful_call_server = gen_server(
    init=_init, handle_cast=_cast, handle_call=_doubling_call
)
_effectful_init_server = gen_server(
    init=_doubling_init, handle_cast=_cast, handle_call=_call
)


def test_effectful_cast_updates_state_via_yielded_effect():
    """Proves that a generator handle_cast can yield an effect and use its return value to update state."""

    sent = drive(
        _effectful_cast_server,
        0,
        [CastMsg(body=("double_add", 5)), CallMsg(ref=0, body="get")],
        double=_handle_double,
    )
    assert sent == [ReplyMsg(ref=0, body=10)]  # 0 + double(5) = 10


def test_effectful_cast_state_accumulates_across_messages():
    """Proves that state from successive effectful casts accumulates correctly."""

    sent = drive(
        _effectful_cast_server,
        0,
        [
            CastMsg(body=("double_add", 3)),
            CastMsg(body=("double_add", 4)),
            CallMsg(ref=0, body="get"),
        ],
        double=_handle_double,
    )
    assert sent == [ReplyMsg(ref=0, body=14)]  # double(3) + double(4) = 6 + 8 = 14


def test_effectful_call_reply_uses_yielded_effect():
    """Proves that a generator handle_call can yield an effect and include its return value in the reply."""

    sent = drive(
        _effectful_call_server,
        7,
        [CallMsg(ref=0, body="doubled_get")],
        double=_handle_double,
    )
    assert sent == [ReplyMsg(ref=0, body=14)]  # double(7) = 14


def test_effectful_call_does_not_mutate_state():
    """Proves that an effectful handle_call leaves state unchanged after the reply."""

    sent = drive(
        _effectful_call_server,
        7,
        [CallMsg(ref=0, body="doubled_get"), CallMsg(ref=1, body="get")],
        double=_handle_double,
    )
    assert sent == [ReplyMsg(ref=0, body=14), ReplyMsg(ref=1, body=7)]


def test_effectful_init_computes_initial_state_via_effect():
    """Proves that a generator init can yield an effect to derive the initial state."""

    sent = drive(
        _effectful_init_server,
        5,
        [CallMsg(ref=0, body="get")],
        double=_handle_double,
    )
    assert sent == [ReplyMsg(ref=0, body=10)]  # initial = double(5) = 10


def test_unhandled_effect_from_cast_propagates_out():
    """Proves that an effect yielded by a handler and not handled by the runner propagates as UnhandledEffect."""

    with pytest.raises(UnhandledEffect):
        # no 'double' handler passed — EDouble must bubble out of the gen_server loop
        drive(
            _effectful_cast_server,
            0,
            [CastMsg(body=("double_add", 5))],
        )


def test_unhandled_effect_from_call_propagates_out():
    """Proves that an unhandled effect from handle_call propagates as UnhandledEffect."""

    with pytest.raises(UnhandledEffect):
        drive(
            _effectful_call_server,
            7,
            [CallMsg(ref=0, body="doubled_get")],
        )


def test_unhandled_effect_from_init_propagates_out():
    """Proves that an unhandled effect from init propagates as UnhandledEffect."""

    with pytest.raises(UnhandledEffect):
        drive(_effectful_init_server, 5, [])
