"""Types for the Uqbar runtime"""

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Pid:
    """Our process identifiers"""

    id: int

    def __bytes__(self) -> bytes:
        return self.id.to_bytes(8, "big")

    def __repr__(self) -> str:
        return f"<Pid {self.id}>"

    @classmethod
    def from_bytes(cls, data: bytes) -> "Pid":
        return cls(int.from_bytes(data, "big"))


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
