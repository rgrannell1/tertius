from dataclasses import dataclass, field
from typing import Any, ClassVar

from orbis import Effect, Event

from tertius.types import Envelope, Pid


@dataclass
class ESpawn(Effect[Pid]):
    """Spawn a new process"""

    tag: ClassVar[str] = "spawn"
    # Since we're in a multiprocessing environment, functions need to be looked up by name
    fn_name: str
    # Arguments to pass to the function
    args: tuple[Any, ...] = field(default_factory=tuple)


@dataclass
class ESelf(Effect[Pid]):
    """The current process identifier"""

    tag: ClassVar[str] = "self"


@dataclass
class ESend[BodyT](Event):
    """Send a message to a process"""

    tag: ClassVar[str] = "send"
    # The process to send the message to
    pid: Pid
    # The message to send
    body: BodyT


@dataclass
class EReceive(Effect[Envelope]):
    """Receive a message from a process"""

    tag: ClassVar[str] = "receive"


@dataclass
class ERegister(Event):
    """Register a process name"""

    tag: ClassVar[str] = "register"
    name: str


@dataclass
class EWhereis(Effect[Pid | None]):
    """Lookup a process by name"""

    tag: ClassVar[str] = "whereis"
    name: str


@dataclass
class EMonitor(Event):
    """Monitor a process for crashes"""

    tag: ClassVar[str] = "monitor"
    pid: Pid


@dataclass
class EReceiveTimeout(Effect[Envelope | None]):
    """Receive a message, or None if timeout_ms elapses first."""

    tag: ClassVar[str] = "receive_timeout"
    timeout_ms: int
