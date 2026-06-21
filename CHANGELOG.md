# Changelog

All notable changes to `worklane` are documented here, following
[Keep a Changelog](https://keepachangelog.com/). The project uses semantic
versioning. While the project is pre-1.0, minor releases may include breaking
changes.

## [0.1.0]

Initial public baseline.

### Added

- Typed job API: `Job`, `Client`, `Worker`, typed payload serialization, typed
  handler outputs, and a facade crate for common application use.
- Core job lifecycle: enqueue, reserve, ack, retry, fail, dead-letter,
  requeue, scheduled visibility, lane partitioning, unique-key deduplication,
  and at-least-once delivery semantics.
- Shared `Broker` contract in `worklane-core` — the core job lifecycle plus
  optional `DeadLetterStore`, `QueueStats`, and `ScheduledStore` capabilities
  discovered through `Broker` accessors — with in-memory, SQLite, PostgreSQL,
  and Redis broker implementations.
- `worklane-test`, a reusable broker conformance suite used by first-party
  brokers and intended for third-party broker authors.
- Long-running worker runtime with bounded concurrency, cooperative shutdown,
  handler timeout, lease heartbeats, retry backoff, panic isolation, middleware,
  observers, and circuit-breaker support.
- Durable result-store support for SQLite, PostgreSQL, and Redis, plus
  lifecycle-gated typed result retrieval through the client.
- Claim Check payload offload for large payloads through pluggable
  `PayloadStore` implementations.
- Recurring schedule daemon in `worklane-scheduler`, including UTC and
  timezone-aware cron schedules.
- Topic-to-lane fan-out in `worklane-pubsub`.
- OpenTelemetry trace-context propagation in `worklane-otel`.
- Metrics-facade observer in `worklane-metrics`.
- Operator CLI `wl` for dead-letter inspection and requeue workflows.
- Release-readiness gates for package verification, warning-free public
  rustdoc, and Rust 1.85.0 MSRV validation.

### Compatibility

- This is a `0.x` release. Public APIs are designed for additive evolution, but
  the `Broker` contract may still change before 1.0 as durable backend and
  third-party broker needs are validated.
- Delivery is at-least-once. Handlers must be idempotent because a job may run
  more than once after lease expiry, process failure, or stale resolution.
- The declared MSRV is Rust 1.85.0.
