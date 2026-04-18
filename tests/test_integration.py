"""Integration test: three processes each run marco polo and report to a collector."""
from collections.abc import Generator
from typing import Any

from tertius.effects import EEmit, EReceive, ESelf, ESpawn
from tertius.types import CastMsg, Envelope, Pid
from tertius.vm import run
from tests.fixtures import run_marco_polo_and_report

_SCOPE = {"run_marco_polo_and_report": run_marco_polo_and_report}


def root(n: int) -> Generator[Any, Any, None]:
    """Spawn n worker processes, collect their results, emit them."""

    me: Pid = yield ESelf()

    for _ in range(n):
        yield ESpawn(
            fn_name="run_marco_polo_and_report",
            args=(bytes(me),),
        )

    results = []
    for _ in range(n):
        envelope: Envelope = yield EReceive()

        match envelope.body:
            case CastMsg(body=body):
                results.append(body)

    yield EEmit(results)


def test_three_marco_polo_processes():
    """Proves three independent processes each produce (3, 3) via marco polo."""

    results = next(run(root, 3, scope=_SCOPE))
    assert len(results) == 3
    assert all(res == (3, 3) for res in results), results
