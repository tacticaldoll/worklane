## Context

This is worklane's first change: the v0.1 proof-of-use core loop. There is no
existing code beyond empty crate stubs. The hard part is not the Rust — it is
fixing the job lifecycle semantics (reserve, ack, retry, dead-letter) precisely
enough that a durable broker (Redis, etc.) can later honor the same contract.
The decisions below pin those semantics. Motivation is in `proposal.md`;
observable behavior is in `specs/`.

## Goals / Non-Goals

**Goals:**

- A typed enqueue → reserve → dispatch → run → ack/retry/fail/dead-letter loop.
- A `Broker` trait whose contract is backend-agnostic and an in-memory
  implementation of it.
- Semantics solid enough that durable brokers can implement the same trait
  without changing worker behavior.

**Non-Goals:**

- Durable/networked brokers, scheduling, priorities, multiple lanes, dedup,
  concurrency limits (all in `BACKLOG.md`).
- Production throughput. v0.1 optimizes for a correct, legible core.

## Decisions

### D1. Reservation is a visibility lease (at-least-once)

`reserve` makes a job invisible for a lease duration instead of removing it. If
the worker does not `ack`/`retry`/`fail` before the lease expires, the job
becomes visible again. This gives at-least-once delivery and matches durable
brokers (SQS-style).
- *Alternative — remove on reserve (at-most-once):* simpler, but a worker crash
  loses the job and sets a poor contract for durable brokers. Rejected.
- *Consequence:* handlers must be idempotent (documented; dedup is backlog).

### D2. Retry uses exponential backoff via a `RetryPolicy`

`RetryPolicy { base, factor = 2, cap, max_attempts }` computes
`delay = min(base * factor^(attempts), cap)`. The worker applies the policy; the
broker re-enqueues with the computed delay.
- *Alternative — fixed delay:* too naive for real failures. *Pluggable policy
  trait:* premature API surface for v0.1. Rejected (revisit later).

### D3. Worker runs a single sequential loop (v0.1)

reserve one → run to completion → ack/retry/fail → reserve next. No concurrency.
- *Alternative — bounded concurrency:* more realistic but adds task management
  and ordering concerns. Deferred as a fast-follow; per-job concurrency is
  already in `BACKLOG.md`.
- *Consequence:* throughput is one job at a time; acceptable for proof-of-use.

### D4. Dead-letter is a separate, inspectable store

On final failure the broker moves the job to a dead-letter store that retains
the last error, queryable for tests and recovery.
- *Alternative — log-and-drop:* no inspection/recovery, weakens the "explicit
  failure handling" value. *Status flag in the live store:* mixes live and dead
  jobs. Rejected.

### D5. The broker owns state transitions; the worker owns the policy

The broker is mechanical: `enqueue`, `reserve` (with lease), `ack` (remove),
`retry` (increment attempts, re-enqueue after delay), `fail` (move to
dead-letter). The worker decides retry-vs-fail from `envelope.attempts` and the
`RetryPolicy`. This keeps the broker backend-agnostic and the policy in core.

### D6. Dependencies and core types

- `JobId`: newtype over a UUID v4 (`uuid`).
- Payload: `serde` + `serde_json`; `JobEnvelope.payload` is `Vec<u8>` (opaque to
  the broker). The client serializes `Job::Payload`; the worker deserializes by
  kind.
- Async traits: `async-trait` (enables `dyn Broker` for pluggability).
- Errors: a `thiserror` enum `worklane_core::Error` with a `Result<T>` alias.
- Logging: `tracing`.
- Time: `std::time::Duration` for delays/leases; the in-memory broker tracks
  availability with a monotonic clock. No `chrono` in v0.1.

### D7. Single default lane

The `Broker` trait keeps a `lane: &str` parameter for forward-compatibility, but
v0.1 uses a single `"default"` lane. Multiple lanes are backlog.

## Risks / Trade-offs

- At-least-once ⇒ duplicate execution on lease expiry or crash → **handlers must
  be idempotent.** Mitigation: document clearly; dedup is backlog.
- Time-based leases make tests flaky. Mitigation: inject a clock seam into the
  in-memory broker so tests advance time deterministically and use small leases.
- Single sequential loop limits throughput. Mitigation: documented fast-follow;
  the trait does not preclude a concurrent worker later.
- Resolution (`ack`/`retry`/`fail`) is keyed by `JobId` only, not by a lease
  token, so it is not validated against the current reservation. This is safe
  under the single sequential worker (one resolver, no re-reservation), but
  concurrent workers or durable brokers will need a reservation/receipt token to
  reject stale or superseded resolutions. Deferred with concurrency (see
  `BACKLOG.md`).

## Open Questions

- Default values: lease duration, `RetryPolicy` base/cap, and default
  `max_attempts`. Proposed: lease 30s, base 1s, cap 60s, max_attempts 5
  (overridable per job via `NewJob`). To be finalized in specs/tasks.
- Whether the clock seam is public API or test-only. Lean test-only for v0.1.
