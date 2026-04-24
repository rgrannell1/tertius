"""Project-wide constants."""
from enum import Enum


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

    # Process lifecycle signal — sent by a new process once it survives its first step
    READY = b"ready"

    # Broker response codes
    OK = b"ok"
    ERROR = b"error"


# Milliseconds to wait for a spawned process to signal readiness
SPAWN_READY_TIMEOUT_MS = 1000
