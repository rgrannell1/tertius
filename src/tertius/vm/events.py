# VM observability events — constructors for bookman Events emitted at key lifecycle points.
import time

from bookman.create import point, span
from bookman.events import Event
from bookman.primitives import Message

from tertius.types import Pid
from tertius.vm.broker_utils import pid_hex


def spawn_started(pid: Pid) -> Event:
    return point(dims={"id": [pid_hex(pid)], "tag": ["spawn:started"]})


def spawn_ready(pid: Pid, at: float) -> Event:
    return span(dims={"id": [pid_hex(pid)], "tag": ["spawn:ready"]}, at=at, until=time.time())


def spawn_timeout(pid: Pid, fn_name: str, exit_code: int | None) -> Event:
    return point(
        dims={"id": [pid_hex(pid)], "tag": ["spawn:timeout"]},
        value=Message(f"process {fn_name!r} died before sending READY (exit code {exit_code})"),
    )


def process_exited(pid: Pid) -> Event:
    return point(dims={"id": [pid_hex(pid)], "tag": ["process:exit"]})


def process_crashed(pid: Pid, reason: Exception) -> Event:
    return point(dims={"id": [pid_hex(pid)], "tag": ["process:crash"]}, value=Message(str(reason)))


def name_registered(pid: Pid, name: str) -> Event:
    return point(dims={"id": [pid_hex(pid)], "tag": ["name:registered"], "name": [name]})


def name_unbound(pid: Pid, name: str) -> Event:
    return point(dims={"id": [pid_hex(pid)], "tag": ["name:unbound"], "name": [name]})


def link_established(pid: Pid) -> Event:
    return point(dims={"id": [pid_hex(pid)], "tag": ["link:established"]})


def link_retroactive(pid: Pid) -> Event:
    return point(dims={"id": [pid_hex(pid)], "tag": ["link:retroactive"]})


def link_delivered(pid: Pid) -> Event:
    return point(dims={"id": [pid_hex(pid)], "tag": ["link:delivered"]})


def monitor_established(pid: Pid) -> Event:
    return point(dims={"id": [pid_hex(pid)], "tag": ["monitor:established"]})


def monitor_retroactive(pid: Pid) -> Event:
    return point(dims={"id": [pid_hex(pid)], "tag": ["monitor:retroactive"]})


def monitor_delivered(pid: Pid) -> Event:
    return point(dims={"id": [pid_hex(pid)], "tag": ["monitor:delivered"]})
