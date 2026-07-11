"""Worker handles — one lifecycle interface over OS-process and thread-backed actors."""

import multiprocessing
import pickle
import sys
import threading
from functools import partial
from typing import Protocol

from tertius.constants import KILL_LINGER_GRACE_SECONDS, SpawnMode
from tertius.types import Pid
from tertius.vm.process import process_entry, run_worker
from tertius.vm.worker_types import WorkerSpec

# spawn (not fork) avoids inheriting the parent's ZMQ IO threads, which
# causes libzmq to abort() when the child later calls zmq_msg_recv.
_SPAWN_CTX = multiprocessing.get_context("spawn")


class WorkerHandle(Protocol):
    """Uniform lifecycle surface the broker uses, whatever backs the worker."""

    def start(self) -> None: ...

    def is_alive(self) -> bool: ...

    def exit_status(self) -> int | None: ...

    def terminate(self) -> None: ...


def log_lingering_worker(thread: threading.Thread, pid: Pid) -> None:
    """Watchdog: report a thread worker that outlives its cooperative kill."""

    thread.join(KILL_LINGER_GRACE_SECONDS)
    if not thread.is_alive():
        return

    print(
        f"[tertius] killed thread worker {pid} still running after "
        f"{KILL_LINGER_GRACE_SECONDS}s; thread kills are cooperative",
        file=sys.stderr,
        flush=True,
    )


class ProcessWorker:
    """OS-process worker. Kill is a hard SIGTERM."""

    def __init__(self, spec: WorkerSpec) -> None:
        # Daemon so children don't outlive the broker if it exits uncleanly.
        self.proc = _SPAWN_CTX.Process(target=process_entry, args=(spec,), daemon=True)

    def start(self) -> None:
        self.proc.start()

    def is_alive(self) -> bool:
        return self.proc.is_alive()

    def exit_status(self) -> int | None:
        return self.proc.exitcode

    def terminate(self) -> None:
        self.proc.terminate()


class ThreadWorker:
    """Thread worker inside the VM process.

    Kill is cooperative: terminate() sets an event the worker observes at its next
    effect yield or receive poll.
    """

    def __init__(self, spec: WorkerSpec) -> None:
        self.spec = spec
        self.payload = b""
        self.kill_event = threading.Event()
        self.status: int | None = None
        self.thread = threading.Thread(target=self.run_from_payload, daemon=True)

    def start(self) -> None:
        # The pickle round-trip copies spawn args and scope, so thread workers get
        # the same isolation and spawn-time pickling failures as OS-process workers.
        self.payload = pickle.dumps(self.spec)
        self.thread.start()

    def run_from_payload(self) -> None:
        spec: WorkerSpec = pickle.loads(self.payload)
        run_worker(spec, self.kill_event)
        self.status = 0

    def is_alive(self) -> bool:
        return self.thread.is_alive()

    def exit_status(self) -> int | None:
        return self.status

    def terminate(self) -> None:
        self.kill_event.set()
        watchdog = partial(log_lingering_worker, self.thread, self.spec.pid)
        threading.Thread(target=watchdog, daemon=True).start()


def make_worker(mode: SpawnMode, spec: WorkerSpec) -> WorkerHandle:
    """Construct the worker handle for the requested spawn mode."""

    if mode is SpawnMode.THREAD:
        return ThreadWorker(spec)

    return ProcessWorker(spec)
