"""Project-wide constants."""
from enum import Enum, StrEnum


class Cmd(bytes, Enum):
    # Broker control commands — sent by processes via the control socket
    SPAWN = b"spawn"
    EMIT = b"emit"
    KILL = b"kill"
    LINK = b"link"
    REGISTER = b"register"
    WHEREIS = b"whereis"
    MONITOR = b"monitor"
    CRASH = b"crash"
    JOIN = b"join"

    # Process lifecycle signal — sent by a new process once it survives its first step
    READY = b"ready"

    # Broker response codes
    OK = b"ok"
    ERROR = b"error"


class SpawnMode(StrEnum):
    """Backend a spawned actor runs on — an OS process or a thread in the VM process."""

    PROCESS = "process"
    THREAD = "thread"


# Milliseconds to wait for a spawned process to signal readiness
SPAWN_READY_TIMEOUT_MS = 1000

# Seconds a cooperatively killed thread worker may linger before the watchdog logs it
KILL_LINGER_GRACE_SECONDS = 1.0

# How often a blocked thread-worker receive wakes to check its kill event
RECEIVE_POLL_INTERVAL_MS = 100
