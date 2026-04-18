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

from tertius.constants import CRASH, MONITOR, REGISTER, SPAWN, WHEREIS
from tertius.exceptions import ProcessCrash
from tertius.types import Envelope, Pid

# Frame Accessors


def frame_id(frames: list[bytes]) -> bytes:
    return frames[0]


def frame_command(frames: list[bytes]) -> bytes:
    return frames[1]


def frame_payload(frames: list[bytes]) -> list[bytes]:
    return frames[2:]


# ---------------------------------------------------------------------------
# Data broker messages
# ---------------------------------------------------------------------------


def encode_envelope(target: Pid, sender: Pid, body: Any) -> list[bytes]:
    """Encode an envelope as sent by a DEALER."""

    return [bytes(target), bytes(sender), pickle.dumps(body)]


def decode_received_envelope(frames: list[bytes]) -> Envelope:
    """Decode an envelope as received by a DEALER (routing frame already stripped)."""

    return Envelope(sender=Pid.from_bytes(frames[0]), body=pickle.loads(frames[1]))


def encode_crash_notification(
    target: Pid, sender: Pid, crash: ProcessCrash
) -> list[bytes]:
    """Encode a crash notification as sent by a DEALER."""

    return [bytes(target), bytes(sender), pickle.dumps(crash)]


# ---------------------------------------------------------------------------
# Control messages (process → VM)
# ---------------------------------------------------------------------------


def encode_spawn(fn_name: str, args: tuple[Any, ...]) -> list[bytes]:
    """Encode a spawn request as sent by a DEALER."""

    return [SPAWN, fn_name.encode(), pickle.dumps(args)]


def decode_spawn(frames: list[bytes]) -> tuple[str, tuple[Any, ...]]:
    """Decode a spawn request as received by a DEALER."""

    payload = frame_payload(frames)
    return payload[0].decode(), pickle.loads(payload[1])


def encode_register(name: str) -> list[bytes]:
    """Encode a register request as sent by a DEALER."""

    return [REGISTER, name.encode()]


def decode_register(frames: list[bytes]) -> str:
    """Decode a register request as received by a DEALER."""

    return frame_payload(frames)[0].decode()


def encode_whereis(name: str) -> list[bytes]:
    """Encode a whereis request as sent by a DEALER."""

    return [WHEREIS, name.encode()]


def decode_whereis(frames: list[bytes]) -> str:
    """Decode a whereis request as received by a DEALER."""

    return frame_payload(frames)[0].decode()


def encode_monitor(target: Pid) -> list[bytes]:
    """Encode a monitor request as sent by a DEALER."""

    return [MONITOR, bytes(target)]


def decode_monitor(frames: list[bytes]) -> Pid:
    """Decode a monitor request as received by a DEALER."""

    return Pid.from_bytes(frame_payload(frames)[0])


def encode_crash(reason: Exception) -> list[bytes]:
    """Encode a crash request as sent by a DEALER."""

    return [CRASH, pickle.dumps(reason)]


def decode_crash(frames: list[bytes]) -> Exception:
    """Decode a crash request as received by a DEALER."""

    return pickle.loads(frame_payload(frames)[0])


# ---------------------------------------------------------------------------
# Control replies (VM → process)
# ---------------------------------------------------------------------------


def encode_pid_reply(pid: Pid) -> list[bytes]:
    """Encode a PID reply as sent by a DEALER."""

    return [bytes(pid)]


def decode_pid_reply(frames: list[bytes]) -> Pid:
    """Decode a PID reply as received by a DEALER."""

    return Pid.from_bytes(frames[0])


def encode_whereis_reply(pid: Pid | None) -> list[bytes]:
    """Encode a whereis reply as sent by a DEALER."""

    return [bytes(pid) if pid else b""]


def decode_whereis_reply(frames: list[bytes]) -> Pid | None:
    """Decode a whereis reply as received by a DEALER."""

    return Pid.from_bytes(frames[0]) if frames[0] else None
