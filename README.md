# Tertius

[![CI](https://github.com/rgrannell1/tertius/actions/workflows/ci.yml/badge.svg)](https://github.com/rgrannell1/tertius/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/rgrannell1/tertius/graph/badge.svg?token=gGULQLuaTn)](https://codecov.io/gh/rgrannell1/tertius)

An effects-based multiprocessing runtime for Python, inspired by Erlang. Tertius uses [orbis](https://github.com/rgrannell1/orbis) algebraic effects within a process, and processes then intercommunicate using message-passing over ZMQ sockets.


```
effects.py               effects for intercommunication
genserver.py             a minimal server that receives and sends messages
constants.py             project-wide constants
exceptions.py            exception types
types.py                 shared type definitions
vm/
  broker.py              VM entry-point; event loop over broker and control sockets
  broker_crash.py        crash/kill/link propagation logic
  broker_handlers.py     control-message handlers (spawn, register, whereis, link, monitor)
  broker_spawn.py        process spawning helpers
  broker_state.py        mutable VM state (pid registry, links, monitors)
  broker_utils.py        low-level ZMQ reply helpers
  messages.py            IPC message encoding
  process.py             process bootstrap and effect dispatch loop
  process_handlers.py    per-effect handlers run inside a process
```

## Topology

Tertius sets up intercommunicating processes that interact via the actor pattern; they send and receive messages.

Each process gets two `DEALER` sockets (with their PID as their ID).

- `broker address socket`: a relay for sending and receiving messages from other processes
- `control address socket`: a stateful loop for VM-level events; spawning, registering, identity registry / lookup, linking processes, killing processes, and marking as ready / crashed. This connection maintains application state

A `notifier` socket is to announce processes were killed / crashed on behalf of the dead processes.

`gen_server` is a user-interface wrapping the lower level transport details. It takes plain handler functions and returns a process loop factory:

- `handle_cast`: handle fire-and-forget messages, return new state
- `handle_call`: handle request/reply messages, return `(new_state, reply)`
- `handle_info`: handle any other message category, return new state

```python
counter = gen_server(
    init=lambda initial=0: initial,
    handle_cast=lambda state, body: state + body[1] if body[0] == "inc" else state,
    handle_call=lambda state, body: (state, state) if body == "get" else ...,
)

# inside a process:
yield from counter(0)
```

It abstracts away the ZMQ details so processes focus purely on application messages.

## Effects

| Effect | Description |
|---|---|
| `ESpawn(fn_name, args)` | Spawn a new OS process by scope key; blocks until it is ready |
| `ESelf()` | Return the current process pid |
| `ESend(pid, body)` | Send a message to another process |
| `EReceive()` | Block until a message arrives |
| `EReceiveTimeout(timeout_ms)` | Receive a message or `None` on timeout |
| `ERegister(name)` | Register the current process under a name |
| `EWhereis(name)` | Look up a pid by name; returns `None` if not found |
| `ELink(pid)` | Bidirectionally link to a process — if either crashes, the other dies too |
| `EMonitor(pid)` | Receive a `ProcessCrash` if the target process crashes |

## Build

```sh
rs install   # install dependencies
rs test      # run tests
rs lint      # lint
rs format    # format
```

## Licence

Copyright © 2026 Róisín Grannell

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
