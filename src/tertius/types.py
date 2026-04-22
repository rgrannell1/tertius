"""Types for the Tertius runtime"""

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

# Mapping of function name to callable — defines what processes a VM can spawn.
Scope = dict[str, Callable[..., Any]]


@dataclass(frozen=True)
class Codec[T]:
    encode: Callable[..., list[bytes]]
    decode: Callable[[list[bytes]], T]


@dataclass(frozen=True)
class Pid:
    """Our process identifiers"""

    node_id: int
    id: int

    def __bytes__(self) -> bytes:
        # 4-byte node identifier followed by 8-byte local process id
        return self.node_id.to_bytes(4, "big") + self.id.to_bytes(8, "big")

    def __repr__(self) -> str:
        return f"<Pid {self.node_id}:{self.id}>"

    @classmethod
    def from_bytes(cls, data: bytes) -> "Pid":
        if not data:
            raise ValueError("cannot construct Pid from empty bytes")
        return cls(node_id=int.from_bytes(data[:4], "big"), id=int.from_bytes(data[4:], "big"))


@dataclass(frozen=True)
class Envelope:
    """A message sent from one process to another"""

    sender: Pid
    body: Any


@dataclass(frozen=True)
class CallMsg:
    """Synchronous request; sender expects a reply."""

    ref: int
    body: Any


@dataclass(frozen=True)
class CastMsg:
    """Fire-and-forget message."""

    body: Any


@dataclass(frozen=True)
class ReplyMsg:
    """Response to a CallMsg."""

    ref: int
    body: Any
