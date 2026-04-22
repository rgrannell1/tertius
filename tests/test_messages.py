"""Tests for ZMQ wire format encoding and decoding — all message types roundtrip correctly."""
from hypothesis import given
from hypothesis import strategies as st

from tertius.constants import SPAWN
from tertius.types import Pid
from tertius.vm.messages import (
    crash,
    envelope,
    frame_command,
    frame_id,
    frame_payload,
    monitor,
    pid_reply,
    register,
    spawn,
    whereis,
    whereis_reply,
)

# Fuzz strategies

pids = st.builds(
    Pid,
    node_id=st.integers(min_value=0, max_value=2**32 - 1),
    id=st.integers(min_value=0, max_value=2**63 - 1),
)
names = st.text(
    min_size=1,
    max_size=64,
    alphabet=st.characters(
        whitelist_categories=("Ll", "Lu", "Nd"), whitelist_characters="_"
    ),
)
fn_names = st.from_regex(
    r"[a-z][a-z0-9_]*\.[a-z][a-z0-9_.]*:[a-z][a-z0-9_]*", fullmatch=True
)
simple_args = st.tuples(st.integers(), st.text(), st.booleans())


def router_wrap(encoded: list[bytes], identity: bytes = b"identity") -> list[bytes]:
    """Simulate what a ROUTER socket does: prepend sender identity to received frames."""

    return [identity] + encoded


# ---------------------------------------------------------------------------
# Pid serialisation
# ---------------------------------------------------------------------------


@given(pids)
def test_pid_roundtrips_through_bytes(pid):
    """Proves that pid serialises to bytes and back without loss."""

    assert Pid.from_bytes(bytes(pid)) == pid


# ---------------------------------------------------------------------------
# Frame accessors
# ---------------------------------------------------------------------------


@given(pids, fn_names, simple_args)
def test_frame_accessors_decompose_correctly(pid, fn_name, args):
    """Proves that frame_id, frame_command, frame_payload correctly decompose a ROUTER-received frame."""

    frames = router_wrap(spawn.encode(fn_name, args), identity=bytes(pid))
    assert frame_id(frames) == bytes(pid)
    assert frame_command(frames) == SPAWN
    assert frame_payload(frames) == frames[2:]


# ---------------------------------------------------------------------------
# Control message roundtrips
# ---------------------------------------------------------------------------


@given(fn_names, simple_args)
def test_spawn_roundtrips(fn_name, args):
    """Proves that encode_spawn / decode_spawn roundtrip preserves fn_name and args."""

    frames = router_wrap(spawn.encode(fn_name, args))
    decoded_name, decoded_args = spawn.decode(frames)
    assert decoded_name == fn_name
    assert decoded_args == args


@given(names)
def test_register_roundtrips(name):
    """Proves that encode_register / decode_register roundtrip preserves name."""

    frames = router_wrap(register.encode(name))
    assert register.decode(frames) == name


@given(names)
def test_whereis_roundtrips(name):
    """Proves that encode_whereis / decode_whereis roundtrip preserves name."""

    frames = router_wrap(whereis.encode(name))
    assert whereis.decode(frames) == name


@given(pids)
def test_monitor_roundtrips(pid):
    """Proves that encode_monitor / decode_monitor roundtrip preserves target pid."""

    frames = router_wrap(monitor.encode(pid))
    assert monitor.decode(frames) == pid


@given(st.text())
def test_crash_roundtrips(message):
    """Proves that encode_crash / decode_crash roundtrip preserves exception message."""

    reason = ValueError(message)
    frames = router_wrap(crash.encode(reason))
    decoded = crash.decode(frames)
    assert type(decoded) is ValueError
    assert str(decoded) == message


# ---------------------------------------------------------------------------
# Reply roundtrips
# ---------------------------------------------------------------------------


@given(pids)
def test_pid_reply_roundtrips(pid):
    """Proves that encode_pid_reply / decode_pid_reply roundtrip preserves pid."""

    assert pid_reply.decode(pid_reply.encode(pid)) == pid


@given(st.one_of(pids, st.none()))
def test_whereis_reply_roundtrips(pid):
    """Proves that encode_whereis_reply / decode_whereis_reply roundtrip preserves pid or None."""

    assert whereis_reply.decode(whereis_reply.encode(pid)) == pid


# ---------------------------------------------------------------------------
# Data broker message roundtrips
# ---------------------------------------------------------------------------


@given(pids, pids, st.integers() | st.text() | st.binary())
def test_envelope_roundtrips(target, sender, body):
    """Proves that encode_envelope body survives serialisation; sender pid is preserved on receipt."""

    encoded = envelope.encode(target, sender, body)
    # Simulate ROUTER routing: strip target identity, leaving [sender_pid, body]
    received = encoded[1:]
    env = envelope.decode(received)
    assert env.sender == sender
    assert env.body == body
