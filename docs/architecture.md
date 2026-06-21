# Architecture

High-level architecture for `worklane`. The detailed, authoritative
job-lifecycle semantics live in `openspec/specs/` — this document is an overview
and links out.

## Goal

Make it easy for Rust web services to enqueue background jobs and run workers
with retries, ack/fail semantics, dead-lettering, and pluggable brokers.

## The differentiator: a conformance-verified broker contract

Many job runners are "broker-agnostic." The distinction here is the *layer* and
the *verification*. Celery (via Kombu) abstracts at the **transport** layer — it
unifies the messaging API across RabbitMQ / Redis / SQS but job behavior (acks,
visibility timeouts, retries, dead-lettering) is layered on top and varies by
broker. `worklane` abstracts at the **job-lifecycle** layer: every backend
implements one minimal `Broker` contract and must pass **one shared conformance
suite** (`worklane-test`), so behavior is identical across backends — verified,
not hoped, and not a tail of per-broker caveats.

```text
        core Broker contract  (enqueue · reserve · ack · retry · fail
                  │             · lease · classify; + optional capabilities:
                  │             dead-letter · stats · scheduling)
   in-memory · SQLite · PostgreSQL · Redis · (third-party broker)
                  │
        all gated by the SAME conformance suite (worklane-test)
```

This is what makes the planned broker SPI safe to open: a third-party backend is
acceptable exactly when it passes the suite. The contract is kept deliberately
small so that uniformity is achievable. See the README's *What makes it
different* for the user-facing framing, and `BACKLOG.md` for the (parked) SPI /
capability-segregation program that would turn "we support N backends" into
"anyone can add one, safely."

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

- `worklane-core` — `JobId`, the `Lane` identifier (a validated newtype, not a
  bare string), `JobEnvelope`, `NewJob`, the `Broker` trait, the typed `Job`
  trait, and the error type. The broker stores **opaque envelopes** and does not
  know Rust handler types.
- `worklane-memory` — in-memory `Broker` implementation for dev and tests.
- `worklane-sqlite` — durable SQLite `Broker` implementation.
- `worklane-postgres` — durable PostgreSQL `Broker` implementation (pooled,
  `FOR UPDATE SKIP LOCKED` reserve).
- `worklane-redis` — durable Redis `Broker` implementation (atomic Lua scripts;
  the non-SQL backend).
- `worklane-scheduler` — recurring (cron) schedules; a `Scheduler` daemon that
  enqueues templated jobs via the atomic `ScheduledStore::enqueue_scheduled`
  (obtained through `Broker::scheduled_store`) and so
  coordinates across HA instances. Its own crate so non-schedulers do not
  compile `cron`/`chrono`.
- `worklane-pubsub` — pub/sub topic-routing layer that fans a payload out to
  multiple lanes over the `Client`.
- `worklane-otel` — opt-in OpenTelemetry trace-context propagation: inject at
  enqueue, extract at dispatch. Pulls in no OpenTelemetry code unless depended on.
- `worklane-metrics` — optional `metrics`-facade observer for worker outcomes,
  attempt gauges, processing duration, and queue-depth reporting.
- `worklane-cli` — operator CLI (`wl`) for inspecting and maintaining brokers
  (dead-letter list/requeue, lane stats).
- `worklane-test` — reusable broker and result-store conformance suites any
  implementation can run.
- `worklane` — facade: `Client`, `Worker`, handler registry keyed by job kind.

The durable result store (`ResultStore` in `worklane-core`) is implemented by
the SQLite, PostgreSQL, and Redis crates alongside their brokers. The optional
payload store (`PayloadStore` in `worklane-core`) supports the Claim Check
pattern: large job payload bytes live outside the broker envelope and the
envelope carries only a compact reference that the worker resolves before
dispatch.

(`worklane-macros`, a NATS/SQS backend, etc. are deferred — see `BACKLOG.md`.)

## Key semantics

Decided deliberately in `openspec/specs/` (the authoritative contract), not
improvised in code. The shipped baseline includes:

- **Reservation lease / visibility timeout** — `reserve` hides a job for a
  broker-owned lease; an unresolved lease expiry redelivers it (at-least-once),
  and a heartbeat can hold a long handler's lease. See the broker spec's
  *Reserve with visibility lease* and *Lease extension* requirements.
- **Retry delay policy** — `retry` takes a caller-supplied delay; `RetryPolicy`
  in `worklane-core` owns the backoff, because delay is the worker's policy. See
  the worker spec.
- **Worker concurrency model** — spawn-based bounded concurrency: each reserved
  job runs on its own `tokio::task` (tracked in a `JoinSet`), up to a configured
  limit, so a handler's work is scheduled across the runtime's threads rather
  than multiplexed onto the poll loop's task. See the worker spec.
- **Dead-letter storage shape** — an opaque `JobEnvelope` plus the retained
  error, inspectable and replayable. See the broker spec's *Fail to
  dead-letter*, *Dead-letter read*, and *Requeue from dead-letter* requirements.
- **Error type design** — a single `#[non_exhaustive]` `Error` enum in
  `worklane-core`; see the API stability policy in `AGENTS.md`.

## Documentation model

Current behavior is specified in `openspec/specs/`. Stable architecture and
operator guidance live in this directory and the README. Deferred work lives in
`BACKLOG.md`.
