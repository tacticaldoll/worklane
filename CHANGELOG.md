# Changelog

All notable changes to `worklane` are documented here, following
[Keep a Changelog](https://keepachangelog.com/). The project uses semantic
versioning. While the project is pre-1.0, minor releases may include breaking
changes.

## [0.2.0]

A substantial pre-1.0 release. The `Broker` contract is reshaped into an
explicit minimal lifecycle core plus opt-in capabilities, the workflow API is
renamed to neutral fan-in/fan-out vocabulary, and the operator, broker-author,
and performance surfaces are extended. There are two breaking changes (the
broker contract and the workflow rename); see **Upgrade notes**.

### Added

- `wl classify <job-id>` CLI command: reports a job's lifecycle state
  (`Live` / `DeadLettered` / `CompletedOrUnknown`) as a human-readable line or
  `--format json`, completing the operator's by-id lifecycle question alongside
  `stats` and `dead-letters`.
- A documented broker-author extension surface in `worklane_core::spi` for the
  decisions every backend must make the same way: `MAX_DEAD_LETTER_SWEEP`,
  `classify_state`, `SCHEMA_VERSION` with `check_schema_version` /
  `SchemaVersionCheck`, `DEFAULT_LEASE`, and `RetentionPolicy::age_cutoff_nanos`
  / `keep_count`. Each backend re-exports the default lease as
  `worklane_<backend>::DEFAULT_LEASE`.
- A modular `worklane-test` conformance suite: one mandatory lifecycle battery
  plus per-capability batteries gated on capability presence, exported for
  third-party broker authors, with omitted capabilities reported visibly.

### Changed

- **BREAKING:** the `Broker` contract is now an explicit minimal lifecycle core
  (enqueue, reserve, ack, retry, defer, extend, fail, classify) plus opt-in
  capability traits discovered through `Option<&dyn Cap>` accessors. Batch
  enqueue moved off the required trait into a new `BatchEnqueue` capability
  (joining `DeadLetterStore` / `QueueStats` / `ScheduledStore`); a call to an
  unsupported capability fails predictably with `Error::UnsupportedCapability`.
- **BREAKING:** renamed the workflow-composition public API:
  - `Canvas` trait → `Workflow`
  - `ChordResults` → `FanInResults`
  - `ChordPolicy` → `FanInPolicy`
  - `Client::chord` → `Client::fan_in`
  - `Client::chord_with_policy` → `Client::fan_in_with_policy`

  Migration is a mechanical rename — no signature or behavior changes. Also
  renamed the doc-hidden watcher types `ChordWatcherJob`/`ChordWatcherPayload`
  → `FanInWatcherJob`/`FanInWatcherPayload`, the durable watcher job kind
  `worklane:chord_watcher` → `worklane:fan_in_watcher`, the internal unique-key
  prefixes `chain:`/`chord:`/`cw:` → `sequence:`/`fanin:`/`fiw:`, and the
  `workflow-canvas` capability spec to `workflow`.
- Performance (behavior-preserving): Redis lifecycle Lua scripts are built and
  SHA1-hashed once at construction and reused on the hot path; Postgres
  `enqueue_batch` uses a single multi-row `UNNEST` fast path when no job in the
  batch has a unique key (a measured throughput gain on that path).
- Internal (behavior-preserving): cross-backend shared decisions — the
  dead-letter sweep bound, status-code classification, schema-version policy,
  dead-letter retention math, and the default lease — are single-sourced in
  `worklane-core` so the backends cannot silently drift. The 30s default lease
  and all observable semantics are unchanged.

### Tooling

- Added `worklane-governance` (not published): a `modou` constitution that
  enforces the crate-graph invariants — `worklane-core` portability and durable
  backend substitutability — as a CI gate.

### Upgrade notes

- **Broker implementors and batch-enqueue callers:** batch enqueue is now the
  `BatchEnqueue` capability rather than a required `Broker` method. Implement
  the capability and reach it through its accessor, and guard calls for
  `Error::UnsupportedCapability`. Broker conformance is now organized
  per-capability in `worklane-test`.
- **Fan-in users:** drain workers before upgrading if any fan-in is in flight —
  the renamed watcher job kind and key prefixes do not carry an in-flight
  watcher across the upgrade.

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
