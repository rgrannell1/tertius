"""Integration test: three processes each run marco polo and report to a collector."""

from collections.abc import Generator
from typing import Any

from tertius.effects import EReceive, ESelf, ESpawn
from tertius.types import CastMsg, Envelope, Pid
from tests.fixtures import MARCO_POLO_ROUNDS, run_marco_polo_and_report

_SCOPE = {"run_marco_polo_and_report": run_marco_polo_and_report}

# Worker processes spawned by the root
NUM_WORKERS = 3


def root(num_workers: int) -> Generator[Any, Any, list[Any]]:
    """Spawn n worker processes, collect their results, return them."""

    me: Pid = yield ESelf()

    for _ in range(num_workers):
        yield ESpawn(
            fn_name="run_marco_polo_and_report",
            args=(bytes(me),),
        )

    results = []
    for _ in range(num_workers):
        envelope: Envelope = yield EReceive()

        match envelope.body:
            case CastMsg(body=body):
                results.append(body)

    return results


def test_three_marco_polo_processes(collect):
    """Proves three independent processes each produce (3, 3) via marco polo."""

    expected = (MARCO_POLO_ROUNDS, MARCO_POLO_ROUNDS)
    results, _ = collect(root, NUM_WORKERS, scope=_SCOPE)
    assert len(results) == NUM_WORKERS
    assert all(res == expected for res in results), results
