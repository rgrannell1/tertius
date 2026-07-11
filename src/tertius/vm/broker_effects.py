"""Broker command effects — one dataclass per control command, plus decode_frame."""

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, ClassVar, LiteralString

from orbis import Event

from tertius.constants import Cmd, SpawnMode
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
    mode: SpawnMode | None
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


def decode_spawn_cmd(frames: list[bytes]) -> BrokerCmd:
    fn_name, args, mode = spawn.decode(frames)
    return ESpawnCmd(fn_name=fn_name, args=args, mode=mode, requester=frame_id(frames))


def decode_register_cmd(frames: list[bytes]) -> BrokerCmd:
    return ERegisterCmd(name=register.decode(frames), requester=frame_id(frames))


def decode_whereis_cmd(frames: list[bytes]) -> BrokerCmd:
    return EWhereisCmd(name=whereis.decode(frames), requester=frame_id(frames))


def decode_link_cmd(frames: list[bytes]) -> BrokerCmd:
    return ELinkCmd(pid=link.decode(frames), requester=frame_id(frames))


def decode_monitor_cmd(frames: list[bytes]) -> BrokerCmd:
    return EMonitorCmd(pid=monitor.decode(frames), requester=frame_id(frames))


def decode_emit_cmd(frames: list[bytes]) -> BrokerCmd:
    return EEmitCmd(body=emit.decode(frames), requester=frame_id(frames))


def decode_kill_cmd(frames: list[bytes]) -> BrokerCmd:
    return EKillCmd(target=kill.decode(frames), requester=frame_id(frames))


def decode_crash_cmd(frames: list[bytes]) -> BrokerCmd:
    requester = frame_id(frames)
    reason = crash.decode(frames)
    return ECrashCmd(pid=Pid.from_bytes(requester), reason=reason, requester=requester)


def decode_join_cmd(frames: list[bytes]) -> BrokerCmd:
    return EJoinCmd(requester=frame_id(frames))


# Command byte -> effect decoder
DECODERS: dict[bytes, Callable[[list[bytes]], BrokerCmd]] = {
    Cmd.SPAWN: decode_spawn_cmd,
    Cmd.REGISTER: decode_register_cmd,
    Cmd.WHEREIS: decode_whereis_cmd,
    Cmd.LINK: decode_link_cmd,
    Cmd.MONITOR: decode_monitor_cmd,
    Cmd.EMIT: decode_emit_cmd,
    Cmd.KILL: decode_kill_cmd,
    Cmd.CRASH: decode_crash_cmd,
    Cmd.JOIN: decode_join_cmd,
}


def decode_frame(frames: list[bytes]) -> BrokerCmd | None:
    """Decode raw ZMQ control frames into a broker command effect, or None for unknown commands."""

    decoder = DECODERS.get(frame_command(frames))
    if decoder is None:
        return None

    return decoder(frames)
