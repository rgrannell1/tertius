"""Action generators, executors, and root process for the Tertius VM fuzzer."""
import random
from collections.abc import Generator
from typing import Any

from tertius.effects import EEmit, EKill, ELink, EMonitor, EReceiveTimeout, ERegister, ESend, ESpawn, EWhereis
from tertius.exceptions import DeadProcess
from tertius.types import Envelope, Pid

from .fuzz_types import (
    EmitAction,
    FuzzAction,
    FuzzRunState,
    KillAction,
    LinkAction,
    MonitorAction,
    RegisterAction,
    SendAction,
    SpawnAction,
    WhereisAction,
)
from .fuzz_workers import WORKER_FN_NAMES


# Maximum total process spawns per fuzz run
MAX_SPAWNS = 10

# Representative message bodies sent during fuzzing
FUZZ_BODIES: list[Any] = [42, "hello", None, [], {}, b"bytes"]


def _available_action_types(state: FuzzRunState) -> list[str]:
    """Return action type names valid given current fuzz state."""
    types = ["register", "whereis", "emit"]
    if state.spawn_count < MAX_SPAWNS:
        types.append("spawn")
    if state.pid_pool:
        types.extend(["kill", "send", "monitor", "link"])
    return types


def _parameterize_action(rng: random.Random, state: FuzzRunState, action_type: str) -> FuzzAction:
    """Build a concrete FuzzAction from a type name and current state."""
    match action_type:
        case "spawn":
            return SpawnAction(fn_name=rng.choice(WORKER_FN_NAMES))
        case "kill":
            return KillAction(target_idx=rng.randrange(len(state.pid_pool)))
        case "send":
            return SendAction(
                target_idx=rng.randrange(len(state.pid_pool)),
                body=rng.choice(FUZZ_BODIES),
            )
        case "monitor":
            return MonitorAction(target_idx=rng.randrange(len(state.pid_pool)))
        case "link":
            return LinkAction(target_idx=rng.randrange(len(state.pid_pool)))
        case "register":
            return RegisterAction(name=f"fuzz_{rng.randint(0, 19)}")
        case "whereis":
            return WhereisAction(name=f"fuzz_{rng.randint(0, 19)}")
        case "emit":
            return EmitAction(body=rng.choice(FUZZ_BODIES))
        case _:
            raise ValueError(f"unknown action type: {action_type!r}")


def generate_next_action(rng: random.Random, state: FuzzRunState) -> FuzzAction:
    """Pick and parameterize the next fuzz action from current state."""
    action_type = rng.choice(_available_action_types(state))
    return _parameterize_action(rng, state, action_type)


def execute_spawn(state: FuzzRunState, fn_name: str) -> Generator[Any, Any, None]:
    """Spawn a worker process and record its PID in the pool."""
    pid: Pid = yield ESpawn(fn_name=fn_name)
    state.pid_pool.append(pid)
    state.spawn_count += 1


def execute_kill(state: FuzzRunState, target_idx: int) -> Generator[Any, Any, None]:
    """Kill a process; swallows DeadProcess if it already exited."""
    target = state.pid_pool[target_idx]
    try:
        yield EKill(pid=target)
    except DeadProcess:
        pass


def execute_send(state: FuzzRunState, target_idx: int, body: Any) -> Generator[Any, Any, None]:
    """Send a message; silently dropped by the broker if the target is dead."""
    target = state.pid_pool[target_idx]
    yield ESend(pid=target, body=body)


def execute_monitor(state: FuzzRunState, target_idx: int) -> Generator[Any, Any, None]:
    """Set a one-shot monitor; retroactive ProcessCrash delivered as a message if already dead."""
    target = state.pid_pool[target_idx]
    yield EMonitor(pid=target)


def execute_link(state: FuzzRunState, target_idx: int) -> Generator[Any, Any, None]:
    """Bidirectionally link to a process; retroactive LinkedCrash queued as a message if already dead."""
    target = state.pid_pool[target_idx]
    yield ELink(pid=target)


def execute_register(name: str) -> Generator[Any, Any, None]:
    """Register the root process under a fuzz name."""
    yield ERegister(name=name)


def execute_whereis(name: str) -> Generator[Any, Any, None]:
    """Look up a fuzz name; result is discarded."""
    yield EWhereis(name=name)


def execute_emit(body: Any) -> Generator[Any, Any, None]:
    """Emit a value to the test host."""
    yield EEmit(body=body)


def execute_action(state: FuzzRunState, action: FuzzAction) -> Generator[Any, Any, None]:
    """Dispatch a fuzz action to its executor generator."""
    match action:
        case SpawnAction(fn_name=fn_name):
            yield from execute_spawn(state, fn_name)
        case KillAction(target_idx=idx):
            yield from execute_kill(state, idx)
        case SendAction(target_idx=idx, body=body):
            yield from execute_send(state, idx, body)
        case MonitorAction(target_idx=idx):
            yield from execute_monitor(state, idx)
        case LinkAction(target_idx=idx):
            yield from execute_link(state, idx)
        case RegisterAction(name=name):
            yield from execute_register(name)
        case WhereisAction(name=name):
            yield from execute_whereis(name)
        case EmitAction(body=body):
            yield from execute_emit(body)


def drain_notifications(timeout_ms: int) -> Generator[Any, Any, None]:
    """Drain all pending messages from the root mailbox until a timeout elapses.

    Retroactive LinkedCrash or ProcessCrash notifications from monitors and
    links accumulate here and are consumed so the root process exits cleanly.
    """
    while True:
        envelope: Envelope | None = yield EReceiveTimeout(timeout_ms)
        if envelope is None:
            return


def cleanup_processes(state: FuzzRunState) -> Generator[Any, Any, None]:
    """Kill every PID in the pool before the root exits.

    Ensures no spawned processes are still alive when the VM context is
    destroyed, avoiding a ZMQ race in the broker's shutdown path.
    """
    for pid in state.pid_pool:
        try:
            yield EKill(pid=pid)
        except DeadProcess:
            pass


def fuzz_root(seed: int, num_steps: int) -> Generator[Any, Any, None]:
    """Root process: drives a random sequence of fuzz actions against the VM.

    Generates each action online from the current observed state so actions
    always have a plausible target — though the target may be dead, which is
    intentional.
    """
    rng = random.Random(seed)
    state = FuzzRunState()
    for _ in range(num_steps):
        action = generate_next_action(rng, state)
        yield from execute_action(state, action)
    yield from cleanup_processes(state)
    yield from drain_notifications(timeout_ms=50)
