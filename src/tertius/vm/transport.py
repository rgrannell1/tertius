"""Socket construction helpers for Tertius VM transports."""

import os

import zmq

from tertius.transport_types import (
    CurveClientKeys,
    CurveServerKeys,
    IpcTransport,
    TcpTransport,
    Transport,
)
from tertius.types import Pid


def build_addresses(transport: Transport, vm_pid: int, vm_instance: int) -> tuple[str, str]:
    """Build data and control addresses for the configured transport."""

    match transport:
        case IpcTransport(base_path=base_path):
            return build_ipc_addresses(base_path, vm_pid, vm_instance)
        case TcpTransport(host=host, data_port=data_port, control_port=control_port):
            data_addr = f"tcp://{host}:{data_port}"
            control_addr = f"tcp://{host}:{control_port}"
            return data_addr, control_addr


def build_ipc_addresses(
    base_path: str | None, vm_pid: int, vm_instance: int
) -> tuple[str, str]:
    """Build IPC socket addresses using the existing /tmp naming scheme."""

    base = base_path or os.path.join("/tmp", f"tertius-{vm_pid}-{vm_instance}")
    return f"ipc://{base}-data.sock", f"ipc://{base}-ctrl.sock"


def make_router(
    ctx: "zmq.Context[zmq.Socket[bytes]]",
    addr: str,
    transport: Transport,
) -> "zmq.Socket[bytes]":
    """Create and bind a ROUTER socket for broker-side VM traffic."""

    router: zmq.Socket[bytes] = ctx.socket(zmq.ROUTER)
    router.setsockopt(zmq.LINGER, transport.linger_ms)
    configure_server_security(router, transport)
    router.bind(addr)
    return router


def make_dealer(
    ctx: "zmq.Context[zmq.Socket[bytes]]",
    pid: Pid,
    addr: str,
    transport: Transport,
) -> "zmq.Socket[bytes]":
    """Create and connect a DEALER socket for process-side VM traffic."""

    dealer: zmq.Socket[bytes] = ctx.socket(zmq.DEALER)
    dealer.setsockopt(zmq.LINGER, transport.linger_ms)
    dealer.setsockopt(zmq.IDENTITY, bytes(pid))
    configure_client_security(dealer, transport)
    dealer.connect(addr)
    return dealer


def make_notifier(
    ctx: "zmq.Context[zmq.Socket[bytes]]",
    broker_addr: str,
    transport: Transport,
) -> "zmq.Socket[bytes]":
    """Create the broker notifier DEALER for crash propagation."""

    notifier: zmq.Socket[bytes] = ctx.socket(zmq.DEALER)
    notifier.setsockopt(zmq.LINGER, transport.linger_ms)
    notifier.setsockopt(zmq.IDENTITY, b"vm-notifier")
    configure_client_security(notifier, transport)
    notifier.connect(broker_addr)
    return notifier


def configure_server_security(socket: "zmq.Socket[bytes]", transport: Transport) -> None:
    """Apply server-side security options to a ZMQ socket."""

    match transport:
        case TcpTransport(security=security) if security is not None:
            apply_curve_server(socket, security.broker)
        case _:
            return


def configure_client_security(socket: "zmq.Socket[bytes]", transport: Transport) -> None:
    """Apply client-side security options to a ZMQ socket."""

    match transport:
        case TcpTransport(security=security) if security is not None:
            apply_curve_client(socket, security.process)
        case _:
            return


def apply_curve_server(socket: "zmq.Socket[bytes]", keys: CurveServerKeys) -> None:
    """Apply CURVE server keys to a ZMQ socket."""

    socket.setsockopt(zmq.CURVE_PUBLICKEY, keys.public_key)
    socket.setsockopt(zmq.CURVE_SECRETKEY, keys.secret_key)
    socket.setsockopt(zmq.CURVE_SERVER, 1)


def apply_curve_client(socket: "zmq.Socket[bytes]", keys: CurveClientKeys) -> None:
    """Apply CURVE client keys to a ZMQ socket."""

    socket.setsockopt(zmq.CURVE_PUBLICKEY, keys.public_key)
    socket.setsockopt(zmq.CURVE_SECRETKEY, keys.secret_key)
    socket.setsockopt(zmq.CURVE_SERVERKEY, keys.server_public_key)
