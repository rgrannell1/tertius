from tertius.types import Pid


class TertiusError(Exception):
    """An error that occurred in the Uqbar runtime"""

    pass


class ProcessCrash(TertiusError):
    """A process that has crashed"""

    def __init__(self, pid: Pid, reason: Exception) -> None:
        self.pid = pid
        self.reason = reason
        super().__init__(f"Process {pid} crashed: {reason}")

    def __reduce__(self) -> tuple:
        return (self.__class__, (self.pid, self.reason))


class DeadProcess(TertiusError):
    """A process that has exited"""

    def __init__(self, pid: Pid) -> None:
        self.pid = pid
        super().__init__(f"Process {pid} is an ex-process")
