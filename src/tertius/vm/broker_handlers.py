"""All broker command handlers — spawn, register, whereis, link, monitor, emit, kill, crash."""

import pickle
import time
from collections.abc import Generator
from dataclasses import dataclass
from typing import Any

import zmq

from tertius.constants import SPAWN_READY_TIMEOUT_MS, Cmd, SpawnMode
from tertius.exceptions import (
    DeadProcessError,
    LinkedCrashError,
    NormalExitError,
    ProcessCrashError,
)
from tertius.types import Pid, Scope
from tertius.vm.broker_effects import (
    ECrashCmd,
    EEmitCmd,
    EJoinCmd,
    EKillCmd,
    ELinkCmd,
    EMonitorCmd,
    ERegisterCmd,
    ESpawnCmd,
    EWhereisCmd,
    decode_frame,
)
from tertius.vm.broker_state import BrokerState
from tertius.vm.broker_types import SpawnContext
from tertius.vm.broker_utils import reply
from tertius.vm.events import (
    link_delivered,
    link_established,
    link_retroactive,
    monitor_delivered,
    monitor_established,
    monitor_retroactive,
    name_registered,
    name_unbound,
    process_crashed,
    process_exited,
    process_joined,
    spawn_ready,
    spawn_started,
    spawn_timeout,
)
from tertius.vm.messages import (
    encode_crash_notification,
    encode_linked_crash_notification,
    pid_reply,
    whereis_reply,
)
from tertius.vm.worker import WorkerHandle, make_worker
from tertius.vm.worker_types import WorkerSpec


@dataclass(frozen=True)
class CrashFanout:
    """Who was affected by a crash: unbound names, notified monitors and links."""

    unbound: list[str]
    watchers: list[Pid]
    peers: list[Pid]


def handle_register(
    state: BrokerState,
    router: "zmq.Socket[bytes]",
    effect: ERegisterCmd,
) -> Generator[None, Any, None]:
    """Register a process name."""

    yield from ()
    # Names are process-scoped: the pid is derived from the requester identity
    # rather than the message body, so a process can only register itself.
    pid = Pid.from_bytes(effect.requester)
    state.names[effect.name] = pid
    state.emit_queue.put(name_registered(pid, effect.name))
    reply(router, effect.requester, Cmd.OK)
    return


def handle_whereis(
    state: BrokerState,
    router: "zmq.Socket[bytes]",
    effect: EWhereisCmd,
) -> Generator[None, Any, None]:
    """Lookup a process by name."""

    yield from ()
    pid = state.names.get(effect.name)
    reply(router, effect.requester, *whereis_reply.encode(pid))
    return


def handle_join(
    state: BrokerState,
    router: "zmq.Socket[bytes]",
    effect: EJoinCmd,
) -> Generator[None, Any, None]:
    """Acknowledge an externally started process joining the VM.

    Joined processes were not spawned by this broker — the handshake confirms the
    broker is reachable before the joiner starts sending real traffic.
    """

    yield from ()
    pid = Pid.from_bytes(effect.requester)
    state.emit_queue.put(process_joined(pid))
    reply(router, effect.requester, Cmd.OK)
    return


def handle_link(
    state: BrokerState,
    notifier: "zmq.Socket[bytes]",
    router: "zmq.Socket[bytes]",
    effect: ELinkCmd,
) -> Generator[None, Any, None]:
    """Bidirectionally link two processes, so that if one crashes the other is notified."""

    yield from ()
    requester_pid = Pid.from_bytes(effect.requester)
    # Ack immediately so the requester isn't blocked while we check the tombstone.
    reply(router, effect.requester, Cmd.OK)

    if effect.pid in state.dead:
        # Target already gone — deliver the crash signal retroactively so the
        # caller behaves consistently whether the link races with a crash or not.
        kill_msg = LinkedCrashError(pid=effect.pid, reason=state.dead[effect.pid])
        notifier.send_multipart(
            encode_linked_crash_notification(requester_pid, effect.pid, kill_msg)
        )
        state.emit_queue.put(link_retroactive(effect.pid))
        return

    state.links.setdefault(requester_pid, set()).add(effect.pid)
    state.links.setdefault(effect.pid, set()).add(requester_pid)
    state.emit_queue.put(link_established(requester_pid))
    return


def handle_monitor(
    state: BrokerState,
    notifier: "zmq.Socket[bytes]",
    router: "zmq.Socket[bytes]",
    effect: EMonitorCmd,
) -> Generator[None, Any, None]:
    """Notify the broker that this process wants to monitor the target PID."""

    yield from ()
    requester_pid = Pid.from_bytes(effect.requester)
    reply(router, effect.requester, Cmd.OK)

    if effect.pid in state.dead:
        # Same retroactive delivery as handle_link — the monitor guarantee is
        # that you always receive exactly one notification, even if the target
        # died before you asked.
        crash_msg = ProcessCrashError(pid=effect.pid, reason=state.dead[effect.pid])
        notifier.send_multipart(
            encode_crash_notification(requester_pid, effect.pid, crash_msg)
        )
        state.emit_queue.put(monitor_retroactive(effect.pid))
        return

    state.monitors.setdefault(effect.pid, set()).add(requester_pid)
    state.emit_queue.put(monitor_established(requester_pid))
    return


def handle_emit(
    state: BrokerState,
    router: "zmq.Socket[bytes]",
    effect: EEmitCmd,
) -> Generator[None, Any, None]:
    """Emitted values are surfaced to the host application via the queue rather
    than being routed to another process, so they cross the VM boundary."""

    yield from ()
    state.emit_queue.put(effect.body)
    reply(router, effect.requester, Cmd.OK)
    return


def _notify_monitors(
    state: BrokerState,
    notifier: "zmq.Socket[bytes]",
    pid: Pid,
    reason: Exception,
) -> list[Pid]:
    """Notify the monitors of a process crash."""

    crash_msg = ProcessCrashError(pid=pid, reason=reason)
    watchers = list(state.monitors.pop(pid, []))

    for watcher in watchers:
        notifier.send_multipart(encode_crash_notification(watcher, pid, crash_msg))

    return watchers


def _notify_links(
    state: BrokerState,
    notifier: "zmq.Socket[bytes]",
    pid: Pid,
    reason: Exception,
) -> list[Pid]:
    """Notify the links of a process crash."""

    if isinstance(reason, NormalExitError):
        state.links.pop(pid, None)
        return []

    # Links are bidirectional: when one end dies the other gets a LinkedCrashError
    # signal and the back-reference is cleaned up so the surviving process isn't
    # notified again if it subsequently dies itself.
    kill_msg = LinkedCrashError(pid=pid, reason=reason)
    peers = list(state.links.pop(pid, set()))

    for peer in peers:
        if peer in state.links:
            state.links[peer].discard(pid)
        notifier.send_multipart(encode_linked_crash_notification(peer, pid, kill_msg))

    return peers


def _record_crash(
    state: BrokerState,
    notifier: "zmq.Socket[bytes]",
    pid: Pid,
    reason: Exception,
) -> CrashFanout:
    """Mark a process as dead and propagate the crash to any observers.

    The pid is kept in `dead` as a tombstone so that future link/monitor
    requests against it can be answered immediately rather than hanging.
    Registered names are unbound so they can be reclaimed by a replacement process.
    """
    state.dead[pid] = reason

    unbound = [name for name, owner in state.names.items() if owner == pid]
    for name in unbound:
        del state.names[name]

    watchers = _notify_monitors(state, notifier, pid, reason)
    peers = _notify_links(state, notifier, pid, reason)

    return CrashFanout(unbound=unbound, watchers=watchers, peers=peers)


def _emit_crash_events(state: BrokerState, pid: Pid, fanout: CrashFanout) -> None:
    """Emit crash events for a process."""

    for name in fanout.unbound:
        state.emit_queue.put(name_unbound(pid, name))

    for _watcher in fanout.watchers:
        state.emit_queue.put(monitor_delivered(pid))

    for _peer in fanout.peers:
        state.emit_queue.put(link_delivered(pid))


def handle_kill(
    state: BrokerState,
    notifier: "zmq.Socket[bytes]",
    router: "zmq.Socket[bytes]",
    effect: EKillCmd,
) -> Generator[None, Any, None]:
    """Kill a process."""

    yield from ()
    if effect.target in state.dead:
        reply(router, effect.requester, Cmd.ERROR, pickle.dumps(DeadProcessError(effect.target)))
        return

    # Ack before terminating so the caller isn't blocked waiting on a process
    # that may take a moment to actually die. Thread workers observe the kill
    # cooperatively, at their next effect yield or receive poll.
    reply(router, effect.requester, Cmd.OK)
    worker = state.procs.pop(effect.target, None)
    if worker is not None:
        worker.terminate()

    # Treat an external kill as a crash so monitors and links are notified
    # through the same path as a natural process failure.
    killed_reason = RuntimeError("killed")
    state.emit_queue.put(process_crashed(effect.target, killed_reason))
    fanout = _record_crash(state, notifier, effect.target, killed_reason)
    _emit_crash_events(state, effect.target, fanout)
    return


def handle_crash(
    state: BrokerState,
    notifier: "zmq.Socket[bytes]",
    router: "zmq.Socket[bytes]",
    effect: ECrashCmd,
) -> Generator[None, Any, None]:
    """Handle a process crash."""

    yield from ()
    # A process reports its own crash rather than the broker detecting it via
    # polling, so the reason is accurate and propagation is synchronous.
    if isinstance(effect.reason, NormalExitError):
        state.emit_queue.put(process_exited(effect.pid))
    else:
        state.emit_queue.put(process_crashed(effect.pid, effect.reason))

    fanout = _record_crash(state, notifier, effect.pid, effect.reason)
    _emit_crash_events(state, effect.pid, fanout)
    reply(router, effect.requester, Cmd.OK)
    return


def check_in_scope(scope: Scope, fn_name: str) -> None:
    """Reject spawns of functions the VM does not know."""

    if fn_name not in scope:
        raise KeyError(f"ESpawn: {fn_name!r} not in scope; available: {sorted(scope)}")


def start_worker(
    spawn_ctx: SpawnContext,
    mode: SpawnMode,
    pid: Pid,
    effect: ESpawnCmd,
) -> tuple[WorkerHandle, WorkerSpec]:
    """Build the worker spec and start a worker for it in the requested mode."""

    spec = WorkerSpec(
        pid=pid,
        fn_name=effect.fn_name,
        args=effect.args,
        scope=spawn_ctx.scope,
        broker_addr=spawn_ctx.broker_addr,
        ctrl_addr=spawn_ctx.ctrl_addr,
        transport=spawn_ctx.transport,
    )
    worker = make_worker(mode, spec)
    worker.start()
    return worker, spec


def claim_pending_ready(
    router: "zmq.Socket[bytes]",
    pid_bytes: bytes,
    pending_readies: dict[bytes, None],
) -> bool:
    """Claim a READY that a nested await_ready_gen consumed and stashed for us."""

    if pid_bytes not in pending_readies:
        return False

    del pending_readies[pid_bytes]
    reply(router, pid_bytes, Cmd.OK)
    return True


def check_worker_alive(worker: WorkerHandle, fn_name: str) -> None:
    """Raise if the worker died before sending READY."""

    if worker.is_alive():
        return

    raise RuntimeError(
        f"ESpawn: process {fn_name!r} died before sending READY "
        f"(exit code {worker.exit_status()})"
    )


def recv_ready_frames(
    router: "zmq.Socket[bytes]",
    worker: WorkerHandle,
    fn_name: str,
) -> list[bytes] | None:
    """Receive control frames, checking worker liveness on each recv timeout."""

    try:
        return router.recv_multipart()
    except zmq.Again:
        check_worker_alive(worker, fn_name)
        return None


def decode_effects(frames: list[bytes]) -> Generator[Any, Any, None]:
    """Yield the decoded broker command, or nothing for unknown frames."""

    effect = decode_frame(frames)
    if effect is not None:
        yield effect


def stash_or_ack_ready(
    router: "zmq.Socket[bytes]",
    requester: bytes,
    pid_bytes: bytes,
    pending_readies: dict[bytes, None],
) -> bool:
    """Ack a READY for our pid; stash READYs belonging to other pending spawns."""

    if requester == pid_bytes:
        reply(router, requester, Cmd.OK)
        return True

    pending_readies[requester] = None
    return False


def ready_wait_loop(
    router: "zmq.Socket[bytes]",
    worker: WorkerHandle,
    spec: WorkerSpec,
    pending_readies: dict[bytes, None],
) -> Generator[Any, Any, None]:
    """Wait for the worker's READY, dispatching other commands that arrive meanwhile.

    A nested spawn handler may consume and stash our READY while waiting for its own;
    we check pending_readies at each iteration so we don't miss it.
    """

    pid_bytes = bytes(spec.pid)
    while True:
        if claim_pending_ready(router, pid_bytes, pending_readies):
            return

        frames = recv_ready_frames(router, worker, spec.fn_name)
        if frames is None:
            continue

        if frames[1] != Cmd.READY:
            yield from decode_effects(frames)
            continue

        if stash_or_ack_ready(router, frames[0], pid_bytes, pending_readies):
            return


def await_ready_gen(
    router: "zmq.Socket[bytes]",
    worker: WorkerHandle,
    spec: WorkerSpec,
    pending_readies: dict[bytes, None],
) -> Generator[Any, Any, None]:
    """Bound the READY wait with a recv timeout, restoring the previous timeout after.

    RCVTIMEO is saved and restored so nested calls don't accidentally disable the
    outer timeout.
    """

    prev_timeout = router.getsockopt(zmq.RCVTIMEO)
    router.setsockopt(zmq.RCVTIMEO, SPAWN_READY_TIMEOUT_MS)
    try:
        yield from ready_wait_loop(router, worker, spec, pending_readies)
    finally:
        router.setsockopt(zmq.RCVTIMEO, prev_timeout)


def await_ready_or_timeout(
    router: "zmq.Socket[bytes]",
    worker: WorkerHandle,
    spec: WorkerSpec,
    state: BrokerState,
) -> Generator[Any, Any, None]:
    """Wait for READY, emitting a spawn_timeout event if the worker never sends it."""

    try:
        yield from await_ready_gen(router, worker, spec, state.pending_readies)
    except (RuntimeError, zmq.ZMQError):
        # ZMQError covers broker shutdown racing with await_ready_gen — still emit
        # the timeout event so the telemetry stream is always closed.
        state.emit_queue.put(spawn_timeout(spec.pid, spec.fn_name, worker.exit_status()))
        raise


def handle_spawn(
    spawn_ctx: SpawnContext,
    state: BrokerState,
    router: "zmq.Socket[bytes]",
    effect: ESpawnCmd,
) -> Generator[Any, Any, None]:
    """Spawn a new worker in the requested (or VM default) mode."""

    yield from ()
    check_in_scope(spawn_ctx.scope, effect.fn_name)

    mode = effect.mode or spawn_ctx.default_mode
    new_pid = spawn_ctx.alloc_pid()
    spawn_start = time.time()
    worker, spec = start_worker(spawn_ctx, mode, new_pid, effect)
    state.procs[new_pid] = worker
    state.emit_queue.put(spawn_started(new_pid, mode))

    # Block until the worker is ready before replying to the caller, so the
    # caller can safely send to the new pid immediately after receiving its pid back.
    yield from await_ready_or_timeout(router, worker, spec, state)

    state.emit_queue.put(spawn_ready(new_pid, spawn_start))
    reply(router, effect.requester, *pid_reply.encode(new_pid))
