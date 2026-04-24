All process-level effect handlers in `process_handlers.py` are generator functions, consistent with the coordination handlers (`InitHandler`, `CastHandler`, `CallHandler`, `InfoHandler`) defined in `genserver_types.py`. Where a handler has no effects to yield, a dangling unreachable `yield` makes the function a generator. The observable behaviour of the VM is unchanged.

Tertius emits structured observability events at key points in the VM lifecycle. This lets callers and tests trace what the VM is doing without relying on stderr or timing-sensitive polling.

Events use the `bookman` library's `Event` type, constructed via its `point` and `span` helpers and wrapped in `EEmit` so they surface through the normal emit queue. `bookman` from `../sand` is an editable runtime dependency.

The VM emits events for: process spawn starting, spawn ready, spawn timeout, normal exit, crash (with error message), name registration and unbinding, link and monitor establishment, retroactive link delivery, and crash notification delivery to monitors and links.

Events from inside the broker (outside a generator) are placed directly onto the emit queue rather than yielded.

Each event has at minimum `dims["id"]` (the pid in hex) and `dims["tag"]` (the event category, e.g. `spawn:ready` or `process:crash`). Name events also carry `dims["name"]`. Spans are used when both a start and end time are available; points are used for instantaneous observations.
