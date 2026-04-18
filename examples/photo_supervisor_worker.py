"""
Supervisor/worker example: encode and store 10 photos across 3 worker processes.

Each worker uses orbis effects locally for its processing pipeline.
The stub handlers below can be swapped for real implementations without
changing the worker logic.
"""

from dataclasses import dataclass
from typing import Any, ClassVar, Generator

from orbis import Effect, Event, handle

from tertius import EReceive, ESpawn, ESelf, mcast, run
from tertius.types import CastMsg, Envelope, Pid

# some example data
PHOTOS = [f"photos/{name}.jpg" for name in [
    "beach", "mountain", "forest", "city", "desert",
    "river", "canyon", "glacier", "savanna", "tundra",
]]
N_WORKERS = 3

# -------- Process-Local Orbis Effects -------- #

@dataclass
class EEncode(Effect[bytes]):
    """Encode the image at path, returning the encoded bytes."""
    tag: ClassVar[str] = "encode"
    path: str


@dataclass
class EWriteDB(Event):
    """Write encoded image bytes to the database."""
    tag: ClassVar[str] = "write_db"
    path: str
    data: bytes


# -------- Stub Handlers -------- #

def handle_encode(effect: EEncode) -> bytes:
    return f"<encoded:{effect.path}>".encode()


def handle_write_db(effect: EWriteDB) -> None:
    print(f"  stored {effect.path} ({len(effect.data)} bytes)")


# -------- An example worker Orbis task -------- #

def process_photo(path: str) -> Generator[EEncode | EWriteDB, bytes | None, None]:
    data: bytes = yield EEncode(path)
    yield EWriteDB(path=path, data=data)


# ---------------------------------------------------------------------------
# Worker — tertius for receiving work, orbis for doing it
# ---------------------------------------------------------------------------


def worker(supervisor_pid_bytes: bytes) -> Generator[Any, Any, None]:
    supervisor = Pid.from_bytes(supervisor_pid_bytes)

    while True:
        envelope: Envelope = yield EReceive()

        match envelope.body:
            case CastMsg(body=("process", path)):
                yield from handle(
                    process_photo(path),
                    encode=handle_encode,
                    write_db=handle_write_db,
                )
                yield from mcast(supervisor, ("done", path))
            case CastMsg(body="stop"):
                return


def supervisor() -> Generator[Any, Any, None]:
    """Supervise, spawn workers, distribute photos, collect results."""

    me: Pid = yield ESelf()

    pool: list[Pid] = []

    for _ in range(N_WORKERS):
        pid: Pid = yield ESpawn(
            fn_name="worker",
            args=(bytes(me),),
        )
        pool.append(pid)

    print(f"spawned {N_WORKERS} workers, distributing {len(PHOTOS)} photos")

    for idx, path in enumerate(PHOTOS):
        yield from mcast(pool[idx % N_WORKERS], ("process", path))

    for _ in PHOTOS:
        envelope: Envelope = yield EReceive()

        match envelope.body:
            case CastMsg(body=("done", path)):
                print(f"  ✓ {path}")

    for pid in pool:
        yield from mcast(pid, "stop")


if __name__ == "__main__":
    run(supervisor, scope={"worker": worker})
