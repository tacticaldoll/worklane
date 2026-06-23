# Lifecycle Semantics

This guide summarizes the verified job lifecycle for users and operators. The
normative contract remains in `openspec/specs/`; this document is a readable map
to that contract, not a second source of truth.

## Core Lifecycle

`worklane` stores jobs as opaque `JobEnvelope` values. A `NewJob` becomes live
when a broker accepts it, keeps its lane, priority, payload bytes, max attempts,
and optional uniqueness key, and starts with `attempts = 0`.

The lifecycle loop is:

```text
enqueue -> reserve -> run handler -> ack | retry | defer | fail
```

- `enqueue` stores one job and returns the live job id. A delayed job is hidden
  until its delay elapses on the broker's injected clock.
- `reserve(lane)` returns at most one visible job on that lane, ordered by
  priority and initial visibility time, and hides it behind a reservation lease.
- `ack(receipt)` removes the live job when the receipt is current.
- `retry(receipt, delay)` increments attempts and makes the job visible after
  the retry delay.
- `defer(receipt, delay)` reschedules the job without incrementing attempts.
- `extend(receipt)` renews a current lease.
- `fail(receipt, error)` removes the live job and records a dead letter when the
  broker supports dead-letter inspection.
- `classify(job_id)` reports whether a job is live, dead-lettered, or completed
  or unknown.

See `openspec/specs/broker/spec.md` for the detailed requirements.

## Delivery Boundary

Delivery is at-least-once. A job may be run more than once after a worker crash,
after a lease expires, or after a forward clock movement makes a lease expire
while a handler is still running. The broker does not promise exactly-once
execution. Handlers must be idempotent.

## Uniqueness

When a job carries a `unique_key`, a broker keeps at most one live job for that
opaque key. A duplicate enqueue returns the existing live job id. The key is
released when the job leaves the live store by `ack` or `fail`.

Job ids are also live-store idempotency keys: enqueueing a `NewJob` whose id is
already live returns the existing id without overwriting the stored envelope.

## Dead Letters

Failing a job removes it from the live store and records the opaque envelope plus
the retained error. Brokers that implement the `DeadLetterStore` capability can
read, count, purge, and requeue those records. Requeue restores the original
envelope as live work when its unique key and job id can be reclaimed.

Dead-letter inspection is an optional capability. Passing the lifecycle suite
alone does not imply that a broker can list or requeue dead letters.

## Scheduling

Scheduled enqueue is provided by the optional `ScheduledStore` capability. A
schedule occurrence is claimed atomically when its occurrence value is strictly
greater than the last recorded occurrence for that schedule. The first occurrence
for a schedule always claims, including zero, negative values, and `i64::MIN`.

Occurrence values are Unix-second integers. Brokers store and compare them as
opaque signed integers and do not reinterpret them using backend-local time
zones. The scheduler does not backfill missed occurrences.

## Optional Capabilities

The core `Broker` trait is the portable lifecycle contract. Operations outside
that loop are exposed as optional capability traits:

- `BatchEnqueue` for all-or-nothing batch insertion.
- `DeadLetterStore` for dead-letter inspection, purge, and requeue.
- `QueueStats` for queue-depth statistics.
- `ScheduledStore` for atomic scheduled enqueue.
- `ResultStore` for durable handler outputs, configured beside a broker rather
  than through the core `Broker` trait.

Consumers request optional capabilities through accessors such as
`Broker::batch_enqueue`, `Broker::dead_letter_store`, `Broker::queue_stats`, and
`Broker::scheduled_store`. A missing accessor means the capability is absent,
not that the lifecycle broker is invalid.
