"""Fuzz tests for the Tertius VM — asserts no combination of process operations crashes the VM."""
from functools import partial

import pytest

from tertius.exceptions import LinkedCrash

from .fuzz_executor import fuzz_root
from .fuzz_workers import WORKER_SCOPE


# Steps per fuzz run
SEQUENCE_LENGTH = 50

# Number of distinct seeds to exercise
NUM_SEEDS = 50


@pytest.mark.parametrize("seed", range(NUM_SEEDS))
def test_fuzz_vm_survives_random_action_sequence(collect, seed):
    """VM must not crash under any random sequence of process operations.

    LinkedCrash propagating to the root is expected Erlang-style behaviour (the
    root linked to a process that crashed) and is not a failure.
    """
    try:
        collect(
            partial(fuzz_root, seed, SEQUENCE_LENGTH),
            scope=WORKER_SCOPE,
        )
    except LinkedCrash:
        pass
