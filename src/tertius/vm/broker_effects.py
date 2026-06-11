"""Broker command effects — one dataclass per control command, plus decode_frame."""

from dataclasses import dataclass
from typing import Any, ClassVar, LiteralString

from orbis import Event

from tertius.constants import Cmd
from tertius.types import Pid
from tertius.vm.messages import (
    crash,
    emit,
    frame_command,
    frame_id,
    kill,
    link,
    monitor,
    register,
    spawn,
    whereis,
)


@dataclass
class ESpawnCmd(Event):
    tag: ClassVar[LiteralString] = "spawn_cmd"
    fn_name: str
    args: tuple[Any, ...]
    requester: bytes


@dataclass
class ERegisterCmd(Event):
    tag: ClassVar[LiteralString] = "register_cmd"
    name: str
    requester: bytes


@dataclass
class EWhereisCmd(Event):
    tag: ClassVar[LiteralString] = "whereis_cmd"
    name: str
    requester: bytes


@dataclass
class ELinkCmd(Event):
    tag: ClassVar[LiteralString] = "link_cmd"
    pid: Pid
    requester: bytes


@dataclass
class EMonitorCmd(Event):
    tag: ClassVar[LiteralString] = "monitor_cmd"
    pid: Pid
    requester: bytes


@dataclass
class EEmitCmd(Event):
    tag: ClassVar[LiteralString] = "emit_cmd"
    body: Any
    requester: bytes


@dataclass
class EKillCmd(Event):
    tag: ClassVar[LiteralString] = "kill_cmd"
    target: Pid
    requester: bytes


@dataclass
class EJoinCmd(Event):
    tag: ClassVar[LiteralString] = "join_cmd"
    requester: bytes


@dataclass
class ECrashCmd(Event):
    tag: ClassVar[LiteralString] = "crash_cmd"
    pid: Pid
    reason: Exception
    requester: bytes


# Broker command effect types
type BrokerCmd = (
    ESpawnCmd
    | ERegisterCmd
    | EWhereisCmd
    | ELinkCmd
    | EMonitorCmd
    | EEmitCmd
    | EKillCmd
    | ECrashCmd
    | EJoinCmd
)


def decode_frame(frames: list[bytes]) -> BrokerCmd | None:
    """Decode raw ZMQ control frames into a broker command effect, or None for unknown commands."""

    requester = frame_id(frames)
    command = frame_command(frames)

    match command:
        case Cmd.SPAWN:
            fn_name, args = spawn.decode(frames)
            return ESpawnCmd(fn_name=fn_name, args=args, requester=requester)
        case Cmd.REGISTER:
            return ERegisterCmd(name=register.decode(frames), requester=requester)
        case Cmd.WHEREIS:
            return EWhereisCmd(name=whereis.decode(frames), requester=requester)
        case Cmd.LINK:
            return ELinkCmd(pid=link.decode(frames), requester=requester)
        case Cmd.MONITOR:
            return EMonitorCmd(pid=monitor.decode(frames), requester=requester)
        case Cmd.EMIT:
            return EEmitCmd(body=emit.decode(frames), requester=requester)
        case Cmd.KILL:
            return EKillCmd(target=kill.decode(frames), requester=requester)
        case Cmd.CRASH:
            pid = Pid.from_bytes(requester)
            reason = crash.decode(frames)
            return ECrashCmd(pid=pid, reason=reason, requester=requester)
        case Cmd.JOIN:
            return EJoinCmd(requester=requester)
        case _:
            return None
