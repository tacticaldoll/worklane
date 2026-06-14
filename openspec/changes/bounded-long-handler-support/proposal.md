## Why

Under concurrency (shipped in `add-concurrent-worker`), a handler that runs
longer than its reservation lease is redelivered and runs a second time
(at-least-once duplication), and its later resolution is rejected as stale. The
backlog's sequencing step 5 calls for a heartbeat that holds the lease so a
legitimately slow handler is not treated as dead. But a naive heartbeat would
make a *hung* handler immortal: today a stuck handler is self-limiting (its lease
expires, the job is redelivered, and `attempts` eventually exhausts to
dead-letter), whereas an unconditional heartbeat would hold its slot forever and
never dead-letter it. The heartbeat therefore needs a bound — a handler timeout —
so long handlers are supported *and* stuck ones still die.

## What Changes

- Add a worker **handler timeout**: an opt-in maximum a single handler may run.
  While a handler runs within its timeout, the worker **heartbeats** to extend
  the reservation lease so the job is not redelivered. The heartbeat exists only
  under a timeout, so a stuck handler can never be held indefinitely.
- At the timeout, the worker stops the handler's hold and routes the job through
  the existing failure path (retry while attempts remain, else dead-letter),
  keeping a stuck handler mortal.
- Add `Broker::extend(receipt, …)` to re-apply the visibility lease to a
  currently-held reservation, rejecting a stale/expired/superseded receipt
  exactly as `ack`/`retry`/`fail` do. **BREAKING** for any external `Broker`
  implementor (a new required trait method); acceptable now because the trait was
  durable-validated specifically so its first deliberate change is made while
  cheap, and the only implementors are in-repo.
- Surface the lease window to the worker so it can time the heartbeat without
  reading the broker's clock (additive field on `Reservation`).
- Default behavior is unchanged: with no handler timeout configured, the worker
  neither heartbeats nor times out (lease expiry still redelivers, as today).

## Capabilities

### New Capabilities
<!-- None: this extends existing worker and broker behavior. -->

### Modified Capabilities
- `broker`: add a **Lease extension** requirement (`extend` re-applies the lease
  to a held reservation; stale/expired/superseded receipts are rejected without
  mutation, like every other resolution).
- `worker`: add a **Bounded long-handler support** requirement (heartbeat to hold
  the lease while a handler runs within its timeout; at the timeout, route to the
  existing retry/dead-letter path).

## Impact

- `worklane-core`: `Broker` trait gains `extend` (breaking for external impls);
  `Reservation` gains an additive lease field. An ADR records the trait change.
- `worklane-memory` and `worklane-sqlite`: implement `extend`; both re-run the
  shared `worklane-test` conformance suite. The SQLite path is a guarded
  `UPDATE … WHERE receipt = ? AND leased_until > now` (the Broker design gate
  answer goes in `design.md`).
- `worklane`: `Worker` gains a `with_handler_timeout` builder and heartbeat/
  timeout logic in `run`. The per-job lifecycle is extracted into a
  `worker/execution.rs` module as the first consumer of that seam (structure,
  not behavior — detailed in `design.md`/`tasks.md`).
- `worklane-test`: add conformance scenarios for `extend`.
