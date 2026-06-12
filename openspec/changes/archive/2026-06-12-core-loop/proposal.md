## Why

Rust web services need a simple, type-safe way to move work off the request
path — sending email, media processing, follow-up tasks — with retries and
explicit failure handling. Existing options are heavyweight, untyped, or bolt
the queue to one backend. worklane provides a small, typed, broker-pluggable
core. This change establishes that core (the v0.1 proof-of-use) so the job
lifecycle semantics are pinned down before any durable backend is added.

## What Changes

- Introduce the worklane job model: a typed `Job` trait, `JobId`, `JobEnvelope`
  (opaque payload), and `NewJob`.
- Introduce the `Broker` trait (enqueue / reserve / ack / retry / fail) and an
  in-memory implementation for development and tests.
- Introduce the consume side: a `Worker` that registers handlers by job kind and
  runs the reserve → dispatch → run → ack/retry/fail loop, with retry until max
  attempts and dead-letter on final failure.
- Introduce the enqueue side: a typed `Client::enqueue`.
- Thread serde payload (de)serialization and tracing logs through the loop.
- Add a runnable example and basic tests for the success and failure paths.
- Out of scope: durable brokers, scheduling, priorities, multiple lanes — these
  stay in `BACKLOG.md`.

## Capabilities

### New Capabilities

- `job-model`: job identity (`JobId`), the opaque `JobEnvelope`, `NewJob`, the
  typed `Job` trait, and payload (de)serialization.
- `broker`: the `Broker` trait contract and its state transitions — reserve with
  a visibility lease, retry re-enqueue, ack removal, and dead-letter at max
  attempts — plus the in-memory implementation.
- `worker`: the handler registry keyed by job kind, the reserve → dispatch → run
  loop, ack/retry/fail orchestration, the concurrency model, and unknown-kind
  handling.
- `client`: the typed enqueue API.

### Modified Capabilities

<!-- None — greenfield. openspec/specs/ is currently empty. -->

## Impact

- New code across the three v0.1 crates: `worklane-core` (job model, `Broker`
  trait, error type), `worklane-memory` (in-memory broker), `worklane` (client,
  worker, registry).
- New dependencies (finalized in design): `tokio`, `serde`, a JSON serializer,
  `async-trait`, `tracing`, an ID source for `JobId`, and an error-handling
  crate.
- New `examples/basic/` and tests covering enqueue/reserve/ack, retry attempt
  increments, dead-letter at max attempts, and unknown-kind failure.
- Establishes the lifecycle semantics that durable brokers (Redis, etc.) must
  later honor.
