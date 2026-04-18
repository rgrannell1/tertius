"""Module-level process functions resolvable via tertius scope."""
from collections.abc import Generator
from dataclasses import dataclass
from typing import Any, ClassVar

from orbis import Effect, complete

from tertius.genserver import mcast
from tertius.effects import EReceive
from tertius.types import Envelope, Pid


@dataclass
class EMarco(Effect[int]):
    tag: ClassVar[str] = "marco"
    count: int


@dataclass
class EPolo(Effect[int]):
    tag: ClassVar[str] = "polo"
    count: int


def handle_marco(effect: EMarco) -> int:
    return effect.count + 1


def handle_polo(effect: EPolo) -> int:
    return effect.count + 1


def marco_polo() -> Generator[EMarco | EPolo, int, tuple[int, int]]:
    marco, polo = 0, 0

    while True:
        marco = yield EMarco(marco)
        polo = yield EPolo(polo)

        if marco >= 3 and polo >= 3:
            return (marco, polo)


def run_marco_polo_and_report(collector_pid_bytes: bytes) -> Generator[Any, Any, None]:
    """Run marco polo, then send the result to the collector."""

    collector = Pid.from_bytes(collector_pid_bytes)
    result = complete(marco_polo(), marco=handle_marco, polo=handle_polo)
    yield from mcast(collector, result)


def collect_results(expected: int) -> Generator[Any, Any, list[Any]]:
    """Receive `expected` messages and return them."""

    results = []
    for _ in range(expected):
        envelope: Envelope = yield EReceive()
        results.append(envelope.body)

    return results
