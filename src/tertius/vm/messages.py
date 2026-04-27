"""Wire format encoding and decoding for all ZMQ messages.

There are two ZMQ sockets, each with a distinct message contract:

Data broker (broker_addr) — routes process-to-process messages (ESend / EReceive).
  DEALER sends:    [target_pid, sender_pid, body]
  ROUTER receives: [sender_identity, target_pid, sender_pid, body]  (identity prepended by ROUTER)
  ROUTER routes:   [target_pid, sender_pid, body]  → target DEALER
  DEALER receives: [sender_pid, body]  (routing frame stripped)

Control socket (ctrl_addr) — request / reply between processes and the VM (ESpawn, ERegister, etc.).
  DEALER sends:    [command, ...payload]
  ROUTER receives: [sender_identity, command, ...payload]  (identity prepended by ROUTER)
  ROUTER replies:  [sender_identity, ...response]

The frames we pass have the layout (ROUTER-received):
  frames[0]  - sender identity (requester PID, prepended by ROUTER)
  frames[1]  - command (SPAWN, REGISTER, etc.)
  frames[2:] - payload arguments
"""

import pickle
from typing import Any

from tertius.constants import Cmd
from tertius.exceptions import LinkedCrashError, ProcessCrashError
from tertius.types import Codec, Envelope, Pid

# ---------------------------------------------------------------------------
# Frame accessors
# ---------------------------------------------------------------------------


def frame_id(frames: list[bytes]) -> bytes:
    return frames[0]


def frame_command(frames: list[bytes]) -> bytes:
    return frames[1]


def frame_payload(frames: list[bytes]) -> list[bytes]:
    return frames[2:]


# ---------------------------------------------------------------------------
# Data broker messages
# ---------------------------------------------------------------------------


def _encode_envelope(target: Pid, sender: Pid, body: Any) -> list[bytes]:
    return [bytes(target), bytes(sender), pickle.dumps(body)]


def _decode_envelope(frames: list[bytes]) -> Envelope:
    return Envelope(sender=Pid.from_bytes(frames[0]), body=pickle.loads(frames[1]))


envelope: Codec[Envelope] = Codec(encode=_encode_envelope, decode=_decode_envelope)


def encode_crash_notification(
    target: Pid, sender: Pid, crash: ProcessCrashError
) -> list[bytes]:
    return [bytes(target), bytes(sender), pickle.dumps(crash)]


def encode_linked_crash_notification(
    target: Pid, sender: Pid, crash: LinkedCrashError
) -> list[bytes]:
    return [bytes(target), bytes(sender), pickle.dumps(crash)]


# ---------------------------------------------------------------------------
# Control messages (process → VM)
# ---------------------------------------------------------------------------


def _encode_spawn(fn_name: str, args: tuple[Any, ...]) -> list[bytes]:
    return [Cmd.SPAWN, fn_name.encode(), pickle.dumps(args)]


def _decode_spawn(frames: list[bytes]) -> tuple[str, tuple[Any, ...]]:
    payload = frame_payload(frames)
    return payload[0].decode(), pickle.loads(payload[1])


spawn: Codec[tuple[str, tuple[Any, ...]]] = Codec(
    encode=_encode_spawn, decode=_decode_spawn
)


def _encode_register(name: str) -> list[bytes]:
    return [Cmd.REGISTER, name.encode()]


def _decode_register(frames: list[bytes]) -> str:
    return frame_payload(frames)[0].decode()


register: Codec[str] = Codec(encode=_encode_register, decode=_decode_register)


def _encode_whereis(name: str) -> list[bytes]:
    return [Cmd.WHEREIS, name.encode()]


def _decode_whereis(frames: list[bytes]) -> str:
    return frame_payload(frames)[0].decode()


whereis: Codec[str] = Codec(encode=_encode_whereis, decode=_decode_whereis)


def _encode_link(target: Pid) -> list[bytes]:
    return [Cmd.LINK, bytes(target)]


def _decode_link(frames: list[bytes]) -> Pid:
    return Pid.from_bytes(frame_payload(frames)[0])


link: Codec[Pid] = Codec(encode=_encode_link, decode=_decode_link)


def _encode_monitor(target: Pid) -> list[bytes]:
    return [Cmd.MONITOR, bytes(target)]


def _decode_monitor(frames: list[bytes]) -> Pid:
    return Pid.from_bytes(frame_payload(frames)[0])


monitor: Codec[Pid] = Codec(encode=_encode_monitor, decode=_decode_monitor)


def _encode_emit(body: Any) -> list[bytes]:
    return [Cmd.EMIT, pickle.dumps(body)]


def _decode_emit(frames: list[bytes]) -> Any:
    return pickle.loads(frame_payload(frames)[0])


emit: Codec[Any] = Codec(encode=_encode_emit, decode=_decode_emit)


def _encode_kill(target: Pid) -> list[bytes]:
    return [Cmd.KILL, bytes(target)]


def _decode_kill(frames: list[bytes]) -> Pid:
    return Pid.from_bytes(frame_payload(frames)[0])


kill: Codec[Pid] = Codec(encode=_encode_kill, decode=_decode_kill)


def _encode_crash(reason: Exception) -> list[bytes]:
    return [Cmd.CRASH, pickle.dumps(reason)]


def _decode_crash(frames: list[bytes]) -> Exception:
    return pickle.loads(frame_payload(frames)[0])


crash: Codec[Exception] = Codec(encode=_encode_crash, decode=_decode_crash)


# ---------------------------------------------------------------------------
# Control replies (VM → process)
# ---------------------------------------------------------------------------


def _encode_pid_reply(pid: Pid) -> list[bytes]:
    return [bytes(pid)]


def _decode_pid_reply(frames: list[bytes]) -> Pid:
    return Pid.from_bytes(frames[0])


pid_reply: Codec[Pid] = Codec(encode=_encode_pid_reply, decode=_decode_pid_reply)


def _encode_whereis_reply(pid: Pid | None) -> list[bytes]:
    return [bytes(pid) if pid else b""]


def _decode_whereis_reply(frames: list[bytes]) -> Pid | None:
    return Pid.from_bytes(frames[0]) if frames[0] else None


whereis_reply: Codec[Pid | None] = Codec(
    encode=_encode_whereis_reply, decode=_decode_whereis_reply
)
