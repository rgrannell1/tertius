# Named type aliases for gen_server handler and generator signatures.
from collections.abc import Callable, Generator
from typing import Any

from tertius.effects import EReceive, EReceiveTimeout, ESend
from tertius.types import Envelope

# Init handler: called once with user args, yields initial state.
type InitHandler[StateT] = Callable[..., Generator[Any, Any, StateT]]

# Cast handler: receives state and a fire-and-forget body, yields updated state.
type CastHandler[StateT] = Callable[[StateT, Any], Generator[Any, Any, StateT]]

# Call handler: receives state and a request body, yields (new state, reply).
type CallHandler[StateT] = Callable[[StateT, Any], Generator[Any, Any, tuple[StateT, Any]]]

# Info handler: receives state and an unmatched envelope body, yields updated state.
type InfoHandler[StateT] = Callable[[StateT, Any], Generator[Any, Any, StateT]]

# Generator produced by a running gen_server loop.
type ServerGen = Generator[EReceive | ESend, Envelope | None, None]

# Callable returned by gen_server() — accepts init args and returns a ServerGen.
type ServerFactory = Callable[..., ServerGen]

# Generator produced by mcall() — send a request and await the matching reply.
type McallGen = Generator[ESend | EReceive, None | Envelope, Any]

# Generator produced by mcall_timeout() — like McallGen but with a deadline.
type McallTimeoutGen = Generator[ESend | EReceiveTimeout, None | Envelope, Any]

# Generator produced by mcast() — fire-and-forget, no reply.
type McastGen = Generator[ESend, None, None]
