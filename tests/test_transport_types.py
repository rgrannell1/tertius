"""Tests for transport types and CURVE key loading."""

from pathlib import Path

from tertius import (
    CurveClientKeys,
    CurveKeyPaths,
    CurveSecurity,
    CurveServerKeys,
    load_curve_client_keys,
    load_curve_security,
    load_curve_server_keys,
)


def write_key(path: Path, value: bytes) -> None:
    path.write_bytes(value + b"\n")


def test_load_curve_server_keys_reads_key_files(tmp_path):
    """Proves broker CURVE keys load from files without keeping trailing newlines."""

    public_key_path = tmp_path / "broker.key_public"
    secret_key_path = tmp_path / "broker.key_secret"
    write_key(public_key_path, b"broker-public")
    write_key(secret_key_path, b"broker-secret")

    keys = load_curve_server_keys(public_key_path, secret_key_path)

    assert keys == CurveServerKeys(
        public_key=b"broker-public",
        secret_key=b"broker-secret",
    )


def test_load_curve_client_keys_reads_key_files(tmp_path):
    """Proves process CURVE keys load from files without keeping trailing newlines."""

    public_key_path = tmp_path / "client.key_public"
    secret_key_path = tmp_path / "client.key_secret"
    server_public_key_path = tmp_path / "server.key_public"
    write_key(public_key_path, b"client-public")
    write_key(secret_key_path, b"client-secret")
    write_key(server_public_key_path, b"server-public")

    keys = load_curve_client_keys(
        public_key_path,
        secret_key_path,
        server_public_key_path,
    )

    assert keys == CurveClientKeys(
        public_key=b"client-public",
        secret_key=b"client-secret",
        server_public_key=b"server-public",
    )


def test_load_curve_security_builds_broker_and_process_keys(tmp_path):
    """Proves a full CURVE transport config can be loaded from file paths."""

    broker_public_key_path = tmp_path / "broker.key_public"
    broker_secret_key_path = tmp_path / "broker.key_secret"
    process_public_key_path = tmp_path / "process.key_public"
    process_secret_key_path = tmp_path / "process.key_secret"
    server_public_key_path = tmp_path / "server.key_public"
    write_key(broker_public_key_path, b"broker-public")
    write_key(broker_secret_key_path, b"broker-secret")
    write_key(process_public_key_path, b"process-public")
    write_key(process_secret_key_path, b"process-secret")
    write_key(server_public_key_path, b"server-public")

    security = load_curve_security(
        CurveKeyPaths(
            broker_public_key=broker_public_key_path,
            broker_secret_key=broker_secret_key_path,
            process_public_key=process_public_key_path,
            process_secret_key=process_secret_key_path,
            server_public_key=server_public_key_path,
        )
    )

    assert security == CurveSecurity(
        broker=CurveServerKeys(
            public_key=b"broker-public",
            secret_key=b"broker-secret",
        ),
        process=CurveClientKeys(
            public_key=b"process-public",
            secret_key=b"process-secret",
            server_public_key=b"server-public",
        ),
    )
