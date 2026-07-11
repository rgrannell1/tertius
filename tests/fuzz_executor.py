"""Action generators, executors, and root process for the Tertius VM fuzzer."""
import contextlib
import random
from collections.abc import Callable, Generator
from typing import Any

from tertius.effects import (
    EEmit,
    EKill,
    ELink,
    EMonitor,
    EReceiveTimeout,
    ERegister,
    ESelf,
    ESend,
    ESleep,
    ESpawn,
    EWhereis,
)
from tertius.exceptions import DeadProcessError
from tertius.types import Envelope, Pid

from .fuzz_types import (
    EmitAction,
    FakePidAction,
    FuzzAction,
    FuzzRunState,
    GetSelfAction,
    KillAction,
    LinkAction,
    MonitorAction,
    RegisterAction,
    SendAction,
    SleepAction,
    SpawnAction,
    SpawnLinkerAction,
    WhereisAction,
)
from .fuzz_workers import WORKER_FN_NAMES

# Maximum total process spawns per fuzz run
MAX_SPAWNS = 10

# Representative message bodies sent during fuzzing
FUZZ_BODIES: list[Any] = [42, "hello", None, [], {}, b"bytes"]


def _available_action_types(state: FuzzRunState) -> list[str]:
    """Return action type names valid given current fuzz state."""
    types = ["register", "whereis", "emit", "get_self", "fake_pid"]
    if state.spawn_count < MAX_SPAWNS:
        types.append("spawn")
    if state.pid_pool:
        types.extend(["kill", "send", "monitor", "link"])
    if state.pid_pool and state.spawn_count < MAX_SPAWNS:
        types.append("spawn_linker")
    types.append("sleep")
    return types


def make_spawn_action(rng: random.Random, _state: FuzzRunState) -> FuzzAction:
    return SpawnAction(fn_name=rng.choice(WORKER_FN_NAMES))


def make_kill_action(rng: random.Random, state: FuzzRunState) -> FuzzAction:
    return KillAction(target_idx=rng.randrange(len(state.pid_pool)))


def make_send_action(rng: random.Random, state: FuzzRunState) -> FuzzAction:
    return SendAction(
        target_idx=rng.randrange(len(state.pid_pool)),
        body=rng.choice(FUZZ_BODIES),
    )


def make_monitor_action(rng: random.Random, state: FuzzRunState) -> FuzzAction:
    return MonitorAction(target_idx=rng.randrange(len(state.pid_pool)))


def make_link_action(rng: random.Random, state: FuzzRunState) -> FuzzAction:
    return LinkAction(target_idx=rng.randrange(len(state.pid_pool)))


def make_register_action(rng: random.Random, _state: FuzzRunState) -> FuzzAction:
    return RegisterAction(name=f"fuzz_{rng.randint(0, 19)}")


def make_whereis_action(rng: random.Random, _state: FuzzRunState) -> FuzzAction:
    return WhereisAction(name=f"fuzz_{rng.randint(0, 19)}")


def make_emit_action(rng: random.Random, _state: FuzzRunState) -> FuzzAction:
    return EmitAction(body=rng.choice(FUZZ_BODIES))


def make_get_self_action(_rng: random.Random, _state: FuzzRunState) -> FuzzAction:
    return GetSelfAction()


def make_fake_pid_action(rng: random.Random, _state: FuzzRunState) -> FuzzAction:
    # Fabricate a PID with a plausible node_id but an id that was never allocated.
    node_id = rng.randint(0, 2**32 - 1)
    pid_id = rng.randint(10_000, 2**63)
    return FakePidAction(node_id=node_id, pid_id=pid_id)


def make_spawn_linker_action(rng: random.Random, state: FuzzRunState) -> FuzzAction:
    return SpawnLinkerAction(target_idx=rng.randrange(len(state.pid_pool)))


def make_sleep_action(rng: random.Random, _state: FuzzRunState) -> FuzzAction:
    return SleepAction(ms=rng.randint(10, 80))


# Action type name -> parameterized action builder
ACTION_BUILDERS: dict[str, Callable[[random.Random, FuzzRunState], FuzzAction]] = {
    "spawn": make_spawn_action,
    "kill": make_kill_action,
    "send": make_send_action,
    "monitor": make_monitor_action,
    "link": make_link_action,
    "register": make_register_action,
    "whereis": make_whereis_action,
    "emit": make_emit_action,
    "get_self": make_get_self_action,
    "fake_pid": make_fake_pid_action,
    "spawn_linker": make_spawn_linker_action,
    "sleep": make_sleep_action,
}


def _parameterize_action(rng: random.Random, state: FuzzRunState, action_type: str) -> FuzzAction:
    """Build a concrete FuzzAction from a type name and current state."""
    builder = ACTION_BUILDERS.get(action_type)
    if builder is None:
        raise ValueError(f"unknown action type: {action_type!r}")
    return builder(rng, state)


def generate_next_action(rng: random.Random, state: FuzzRunState) -> FuzzAction:
    """Pick and parameterize the next fuzz action from current state."""
    action_type = rng.choice(_available_action_types(state))
    return _parameterize_action(rng, state, action_type)


def execute_spawn(state: FuzzRunState, action: SpawnAction) -> Generator[Any, Any, None]:
    """Spawn a worker process and record its PID in the pool."""
    pid: Pid = yield ESpawn(fn_name=action.fn_name)
    state.pid_pool.append(pid)
    state.spawn_count += 1


def execute_kill(state: FuzzRunState, action: KillAction) -> Generator[Any, Any, None]:
    """Kill a process; swallows DeadProcessError if it already exited."""
    target = state.pid_pool[action.target_idx]
    with contextlib.suppress(DeadProcessError):
        yield EKill(pid=target)


def execute_send(state: FuzzRunState, action: SendAction) -> Generator[Any, Any, None]:
    """Send a message; silently dropped by the broker if the target is dead."""
    target = state.pid_pool[action.target_idx]
    yield ESend(pid=target, body=action.body)


def execute_monitor(state: FuzzRunState, action: MonitorAction) -> Generator[Any, Any, None]:
    """Set a one-shot monitor.

    retroactive ProcessCrashError delivered as a message if already dead.
    """
    target = state.pid_pool[action.target_idx]
    yield EMonitor(pid=target)


def execute_link(state: FuzzRunState, action: LinkAction) -> Generator[Any, Any, None]:
    """Bidirectionally link to a process.

    retroactive LinkedCrashError queued as a message if already dead.
    """
    target = state.pid_pool[action.target_idx]
    yield ELink(pid=target)


def execute_register(_state: FuzzRunState, action: RegisterAction) -> Generator[Any, Any, None]:
    """Register the root process under a fuzz name."""
    yield ERegister(name=action.name)


def execute_whereis(_state: FuzzRunState, action: WhereisAction) -> Generator[Any, Any, None]:
    """Look up a fuzz name; result is discarded."""
    yield EWhereis(name=action.name)


def execute_emit(_state: FuzzRunState, action: EmitAction) -> Generator[Any, Any, None]:
    """Emit a value to the test host."""
    yield EEmit(body=action.body)


def execute_get_self(state: FuzzRunState, _action: GetSelfAction) -> Generator[Any, Any, None]:
    """Get root's own PID and add it to the pool.

    Enables self-targeting: kill root (zombie), link root (self-link), monitor
    root (self-watch). All create broker state the unit tests don't exercise.
    """
    self_pid: Pid = yield ESelf()
    if self_pid not in state.pid_pool:
        state.pid_pool.append(self_pid)


def execute_fake_pid(state: FuzzRunState, action: FakePidAction) -> Generator[Any, Any, None]:
    """Insert a fabricated PID into the pool without spawning anything.

    Exercises ghost-kill (killing a PID with no process and no tombstone),
    dangling-link (linking to a PID that will never crash), and non-existent
    monitor paths.
    """
    fake = Pid(node_id=action.node_id, id=action.pid_id)
    state.pid_pool.append(fake)
    return
    yield


def execute_spawn_linker(
    state: FuzzRunState, action: SpawnLinkerAction
) -> Generator[Any, Any, None]:
    """Spawn a linker_worker targeting a PID from the pool, then record its PID.

    Creates worker-to-worker links so crash cascades route through the broker's
    link notification path, not just through root-to-worker links.
    """
    target = state.pid_pool[action.target_idx]
    pid: Pid = yield ESpawn(fn_name="linker_worker", args=(bytes(target),))
    state.pid_pool.append(pid)
    state.spawn_count += 1


def execute_sleep(_state: FuzzRunState, action: SleepAction) -> Generator[Any, Any, None]:
    """Sleep the root process, letting background worker operations advance."""
    yield ESleep(ms=action.ms)


# Action dataclass -> executor generator taking (state, action)
ACTION_EXECUTORS: dict[type, Callable[[FuzzRunState, Any], Generator[Any, Any, None]]] = {
    SpawnAction: execute_spawn,
    KillAction: execute_kill,
    SendAction: execute_send,
    MonitorAction: execute_monitor,
    LinkAction: execute_link,
    RegisterAction: execute_register,
    WhereisAction: execute_whereis,
    EmitAction: execute_emit,
    GetSelfAction: execute_get_self,
    FakePidAction: execute_fake_pid,
    SpawnLinkerAction: execute_spawn_linker,
    SleepAction: execute_sleep,
}


def execute_action(state: FuzzRunState, action: FuzzAction) -> Generator[Any, Any, None]:
    """Dispatch a fuzz action to its executor generator."""
    executor = ACTION_EXECUTORS[type(action)]
    yield from executor(state, action)


def drain_notifications(timeout_ms: int) -> Generator[Any, Any, None]:
    """Drain all pending messages from the root mailbox until a timeout elapses.

    Retroactive LinkedCrashError or ProcessCrashError notifications from monitors and
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
        with contextlib.suppress(DeadProcessError):
            yield EKill(pid=pid)


def _advance_simulated_state(state: FuzzRunState, action: FuzzAction) -> None:
    """Mirror the pid_pool and spawn_count mutations from execute_action using placeholder Pids.

    Keeps _available_action_types accurate so the regenerated sequence matches
    what actually ran, without executing any effects.
    """
    match action:
        case SpawnAction() | SpawnLinkerAction():
            state.pid_pool.append(Pid(node_id=0, id=state.spawn_count))
            state.spawn_count += 1
        case GetSelfAction():
            placeholder = Pid(node_id=0, id=0)
            if placeholder not in state.pid_pool:
                state.pid_pool.append(placeholder)
        case FakePidAction(node_id=nid, pid_id=pid_id):
            state.pid_pool.append(Pid(node_id=nid, id=pid_id))


def generate_action_sequence(seed: int, num_steps: int) -> list[FuzzAction]:
    """Reconstruct the deterministic action sequence for a seed without executing any effects."""
    rng = random.Random(seed)
    state = FuzzRunState()
    actions: list[FuzzAction] = []
    for _ in range(num_steps):
        action = generate_next_action(rng, state)
        actions.append(action)
        _advance_simulated_state(state, action)
    return actions


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
