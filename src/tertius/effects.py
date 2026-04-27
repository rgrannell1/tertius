# Effect definitions for the Tertius runtime — process lifecycle, messaging, and observability.
from dataclasses import dataclass, field
from typing import Any, ClassVar, LiteralString

from orbis import Effect, Event

from tertius.types import Envelope, Pid


@dataclass
class ESpawn(Effect[Pid]):
    """Spawn a new process"""

    tag: ClassVar[LiteralString] = "spawn"
    fn_name: str
    args: tuple[Any, ...] = field(default=())


@dataclass
class ESelf(Effect[Pid]):
    """The current process identifier"""

    tag: ClassVar[LiteralString] = "self"


@dataclass
class ESend[BodyT](Event):
    """Send a message to a process"""

    tag: ClassVar[LiteralString] = "send"
    pid: Pid
    body: BodyT


@dataclass
class EReceive(Effect[Envelope]):
    """Receive a message from a process"""

    tag: ClassVar[LiteralString] = "receive"


@dataclass
class ERegister(Event):
    """Register a process name"""

    tag: ClassVar[LiteralString] = "register"
    name: str


@dataclass
class EWhereis(Effect[Pid | None]):
    """Lookup a process by name"""

    tag: ClassVar[LiteralString] = "whereis"
    name: str


@dataclass
class ELink(Event):
    """Bidirectionally link to a process — if either crashes, the other dies too"""

    tag: ClassVar[LiteralString] = "link"
    pid: Pid


@dataclass
class EMonitor(Event):
    """Monitor a process for crashes"""

    tag: ClassVar[LiteralString] = "monitor"
    pid: Pid


@dataclass
class EReceiveTimeout(Effect[Envelope | None]):
    """Receive a message, or None if timeout_ms elapses first."""

    tag: ClassVar[LiteralString] = "receive_timeout"
    timeout_ms: int


@dataclass
class ESleep(Effect[None]):
    """Suspend the process for ms milliseconds."""

    tag: ClassVar[LiteralString] = "sleep"
    ms: int


@dataclass
class EEmit[BodyT](Event):
    """Emit an event to the caller of run()."""

    tag: ClassVar[LiteralString] = "emit"
    body: BodyT


@dataclass
class EKill(Event):
    """Terminate a process — delivers ProcessCrashError to its monitors."""

    tag: ClassVar[LiteralString] = "kill"
    pid: Pid
