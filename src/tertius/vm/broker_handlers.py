import queue
from typing import Any

import zmq

from tertius.constants import OK
from tertius.exceptions import LinkedCrash, ProcessCrash
from tertius.types import Pid
from tertius.vm.messages import (
    decode_emit,
    decode_link,
    decode_monitor,
    decode_register,
    decode_whereis,
    encode_crash_notification,
    encode_linked_crash_notification,
    encode_whereis_reply,
)


def handle_register(
    names: dict[str, Pid],
    router: "zmq.Socket[bytes]",
    requester: bytes,
    frames: list[bytes],
) -> None:
    names[decode_register(frames)] = Pid.from_bytes(requester)
    router.send_multipart([requester, OK])


def handle_whereis(
    names: dict[str, Pid],
    router: "zmq.Socket[bytes]",
    requester: bytes,
    frames: list[bytes],
) -> None:
    pid = names.get(decode_whereis(frames))
    router.send_multipart([requester] + encode_whereis_reply(pid))


def handle_link(
    links: dict[Pid, list[Pid]],
    dead: dict[Pid, Exception],
    notifier: "zmq.Socket[bytes]",
    router: "zmq.Socket[bytes]",
    requester: bytes,
    frames: list[bytes],
) -> None:
    requester_pid = Pid.from_bytes(requester)
    target_pid = decode_link(frames)
    router.send_multipart([requester, OK])

    if target_pid in dead:
        kill_msg = LinkedCrash(pid=target_pid, reason=dead[target_pid])
        notifier.send_multipart(
            encode_linked_crash_notification(requester_pid, target_pid, kill_msg)
        )
        return

    links.setdefault(requester_pid, []).append(target_pid)
    links.setdefault(target_pid, []).append(requester_pid)


def handle_monitor(
    monitors: dict[Pid, list[Pid]],
    dead: dict[Pid, Exception],
    notifier: "zmq.Socket[bytes]",
    router: "zmq.Socket[bytes]",
    requester: bytes,
    frames: list[bytes],
) -> None:
    target_pid = decode_monitor(frames)
    requester_pid = Pid.from_bytes(requester)
    router.send_multipart([requester, OK])

    if target_pid in dead:
        crash_msg = ProcessCrash(pid=target_pid, reason=dead[target_pid])
        notifier.send_multipart(
            encode_crash_notification(requester_pid, target_pid, crash_msg)
        )
        return

    monitors.setdefault(target_pid, []).append(requester_pid)


def handle_emit(
    emit_queue: "queue.Queue[Any]",
    router: "zmq.Socket[bytes]",
    requester: bytes,
    frames: list[bytes],
) -> None:
    emit_queue.put(decode_emit(frames))
    router.send_multipart([requester, OK])
