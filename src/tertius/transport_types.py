"""Typed transport configuration for Tertius VM sockets."""

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class CurveServerKeys:
    """CURVE keys used by broker ROUTER sockets."""

    public_key: bytes
    secret_key: bytes


@dataclass(frozen=True)
class CurveClientKeys:
    """CURVE keys used by process DEALER sockets."""

    public_key: bytes
    secret_key: bytes
    server_public_key: bytes


@dataclass(frozen=True)
class CurveSecurity:
    """CURVE configuration for broker and process socket roles."""

    broker: CurveServerKeys
    process: CurveClientKeys


@dataclass(frozen=True)
class IpcTransport:
    """Local IPC transport matching the historical Tertius default."""

    base_path: str | None = None
    linger_ms: int = 0


@dataclass(frozen=True)
class TcpTransport:
    """TCP transport for explicit host and port based VM communication."""

    data_port: int
    control_port: int
    host: str = "127.0.0.1"
    security: CurveSecurity | None = None
    linger_ms: int = 0


type Transport = IpcTransport | TcpTransport


def read_key_bytes(path: str | Path) -> bytes:
    """Read a CURVE key file and trim trailing whitespace."""

    return Path(path).read_bytes().strip()


def load_curve_server_keys(
    public_key_path: str | Path, secret_key_path: str | Path
) -> CurveServerKeys:
    """Load broker-side CURVE keys from files."""

    return CurveServerKeys(
        public_key=read_key_bytes(public_key_path),
        secret_key=read_key_bytes(secret_key_path),
    )


def load_curve_client_keys(
    public_key_path: str | Path,
    secret_key_path: str | Path,
    server_public_key_path: str | Path,
) -> CurveClientKeys:
    """Load process-side CURVE keys from files."""

    return CurveClientKeys(
        public_key=read_key_bytes(public_key_path),
        secret_key=read_key_bytes(secret_key_path),
        server_public_key=read_key_bytes(server_public_key_path),
    )


def load_curve_security(
    broker_public_key_path: str | Path,
    broker_secret_key_path: str | Path,
    process_public_key_path: str | Path,
    process_secret_key_path: str | Path,
    server_public_key_path: str | Path,
) -> CurveSecurity:
    """Load the broker and process CURVE keys for a TCP transport."""

    return CurveSecurity(
        broker=load_curve_server_keys(broker_public_key_path, broker_secret_key_path),
        process=load_curve_client_keys(
            process_public_key_path,
            process_secret_key_path,
            server_public_key_path,
        ),
    )
