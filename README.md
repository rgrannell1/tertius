# Tertius

[![CI](https://github.com/rgrannell1/tertius/actions/workflows/ci.yml/badge.svg)](https://github.com/rgrannell1/tertius/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/rgrannell1/tertius/graph/badge.svg?token=gGULQLuaTn)](https://codecov.io/gh/rgrannell1/tertius)

An effects-based multiprocessing runtime for Python, inspired by Erlang. Tertius uses [orbis](https://github.com/rgrannell1/orbis) algebraic effects within a process, and processes then intercommunicate using message-passing over ZMQ sockets.


```
effects.py      effects for intercommunication
genserver.py    a minimal server that receives and sends messages
vm/
  broker.py     VM runtime; allocates pids, routes messages, handles control operations, tracks state.
  messages.py   IPC message encoding.
  process.py    process-level handlers.
```

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
