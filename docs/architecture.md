# Architecture

High-level architecture for `worklane`. The detailed, authoritative
job-lifecycle semantics live in `openspec/specs/` — this document is an overview
and links out.

## Goal

Make it easy for Rust web services to enqueue background jobs and run workers
with retries, ack/fail semantics, dead-lettering, and pluggable brokers.

## Core loop (the part we protect)

```text
API receives request
  -> client.enqueue(job)
  -> broker stores envelope
  -> worker reserves job
  -> dispatch by job kind
  -> handler runs
       success       -> ack
       failure       -> retry (until max attempts)
       final failure -> dead letter
```

## Crate layout

- `worklane-core` — `JobId`, `JobEnvelope`, `NewJob`, the `Broker` trait, the
  typed `Job` trait, and the error type. The broker stores **opaque envelopes**
  and does not know Rust handler types.
- `worklane-memory` — in-memory `Broker` implementation for dev and tests.
- `worklane-sqlite` — durable SQLite `Broker` implementation.
- `worklane-test` — reusable broker conformance suite any `Broker` can run.
- `worklane` — facade: `Client`, `Worker`, handler registry keyed by job kind.

(`worklane-redis`, `worklane-macros`, etc. are deferred — see `BACKLOG.md`.)

## Open semantic questions

Decided deliberately per change in `openspec/specs/`, not improvised in code:

- Reservation lease / visibility timeout when a worker crashes mid-job.
- Retry delay policy (fixed vs backoff).
- Worker concurrency model (single loop vs N concurrent handlers).
- Dead-letter storage shape.
- Error type design.

## Decisions

Significant architecture decisions are recorded as ADRs under `docs/adr/`.
