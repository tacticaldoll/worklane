# Worklane Backlog

Future features intentionally **excluded from the baseline** unless a real
consumer proves their shape. Active work is tracked as OpenSpec changes under
`openspec/changes/`; this file is the upstream idea list that feeds
`/opsx:propose`.

The core loop (enqueue → reserve → dispatch → ack / retry / fail / dead-letter)
is solid across the in-memory, SQLite, Postgres, and Redis brokers.

## Shipped

- ✓ **Verified release package gate** — the CI package job verifies packaged
  workspace tarballs with `cargo package --workspace` in an isolated target
  directory, so registry-style dependency resolution and stale package artifacts
  cannot silently bypass release readiness.
- ✓ **Warning-free docs.rs gate** — public Rust documentation now builds with
  `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps`, and CI fails
  broken, private, ambiguous, or otherwise warning-producing rustdoc links.
- ✓ **Verified MSRV gate** — CI now checks the workspace with Rust 1.85.0, and
  dependencies plus local syntax were tightened so the declared
  `rust-version = "1.85"` is an enforced release contract.
- ✓ **Public release support files** — the first-release changelog now lists the
  shipped feature set directly, and the repository includes security,
  contribution, conduct, issue, and pull request guidance for public adopters.
- ✓ **Published crate audience positioning** — each publishable crate now has
  tighter package metadata or crate-level docs that identify its audience, with
  `worklane-test` positioned as a broker-author conformance suite.
- ✓ **First release checklist** — `docs/release-checklist.md` now captures
  crates.io name checks, release gates, dry-runs, dependency-safe publish order,
  and post-publish verification for the workspace.
- ✓ **Known limitations and support matrix** — `docs/known-limitations.md` now
  documents broker support, release boundaries, and practical handling guidance
  for adopters.
- ✓ **Minimal benchmark entry point** — the `worklane` crate now ships a stable
  in-memory core-loop benchmark example plus `docs/benchmarks.md` to explain
  command, output, and scope.
- ✓ **Public API documentation and unsafe policy** — crate roots now forbid
  unsafe code, warn on missing public docs, and CI docs deny both rustdoc
  warnings and missing documentation.

## Deferred

To support `worklane` acting as an orchestration engine and an
**Event-Driven Upstream**, we strictly enforce the *Minimal contracts*
principle: the core knows nothing about events, topics, webhooks, or DAGs. These
patterns are built *on top* of core primitives.

### Backends

- **NATS / SQS backend** — additional `Broker` implementations validated against
  the shared conformance suite. Both are *name-based* backends (the lane is
  embedded in a native hierarchical subject/queue name), so they are the next
  consumers of the lane-encoding seam in `worklane-core` (`reject_chars` /
  `allow_only`). A concrete demand for one of these is also the trigger for the
  broker-SPI work below.
- **Redis Cluster support** — parked. The Redis broker
  is single-node only: its Lua scripts compute most key names internally from
  `ARGV` rather than declaring them in `KEYS[]`, so a cluster rejects the `EVAL`
  with `CROSSSLOT`. Pointing the broker at a cluster surfaces loudly on the first
  multi-key script, not as silent corruption. Cluster support is a redesign, not
  an incremental fix — it would need every key a script touches routed through
  `KEYS[]` (no in-script name construction) and each operation's keys co-located
  in one slot via hash tags (e.g. `ns:{lane}:job:{id}`), which in turn constrains
  the data model (cross-lane operations and the global `ns:seq` counter would
  need rethinking, e.g. a per-slot sequence). The sharpest blocker: `JobId` and
  `ReservationReceipt` are bare UUIDs and the lane lives only as a field inside
  the job hash, so the receipt/id-keyed operations (`ack`, `retry`, `defer`,
  `extend`, `fail`, `classify`, `requeue`) cannot recover the lane to compute the
  hash tag — the lane would have to be encoded into those identifiers, or a
  non-atomic pre-lookup added (which forfeits the single-`EVAL` atomicity the
  current design relies on). Parked per *least commitment*
  until a real clustered-deployment demand exists.

### Broker SPI & extensibility (parked — strategic, design ready)

The positioning differentiator (see the README's *What makes it different*) is a
*conformance-verified* job-lifecycle broker contract: today proven by four
first-party backends passing one shared suite. Turning "**we** support N
backends" into "**anyone** can add one, safely" is a deliberate, separate bet —
parked until there is a real external-broker consumer (a NATS/SQS demand, or a
committed third-party-broker product strategy). Designs are written so the
trigger can be pulled without rediscovery:

- **Broker capability segregation** — split the `Broker` trait into a minimal
  core loop plus opt-in capability traits (`BatchEnqueue`, `DeadLetters`,
  `JobInspector`; `ScheduledStore`/`ResultStore` already split). Prerequisite
  for a stable SPI; only justified once external brokers are in scope. Until
  then the unified `Broker` trait stays as-is. **Deliberately deferred past
  0.1.0**: it is a breaking change to both the `Broker` contract and the now
  public `worklane-test` conformance suite, so it is not worth doing days before
  the first publish for zero existing third-party brokers — pre-1.0 lets 0.2
  break cleanly. Target a 0.2 release.
- **SPI stability policy** — formally document `worklane_core::spi` + the
  capability traits as the broker-author extension point, with versioning
  guarantees. Follows capability segregation.
- **Adversarial / modular conformance** — restructure `worklane-test` per
  capability and add fault-injection / concurrency / clock-skew coverage, so it
  serves as the published acceptance test for third-party brokers.

### Storage representation

- **Columnar envelope schema** — parked (no consumer; design ready). Promoting
  the SQL brokers' remaining envelope fields (`kind`, `payload`, `max_attempts`,
  `trace_context`) to queryable columns plus a `JobEnvelope::from_stored`
  reconstruction constructor would let an operator query or report by `kind` or
  attempt count, but it erodes the opaque-envelope principle (adding a field
  becomes a three-touch change across the serde struct, each SQL backend, and
  `from_stored`) and has no proven consumer query. Park until a consumer such as
  a CLI `wl jobs --kind X` exists.

- **Cross-broker logic dedup** — non-breaking cleanup, deferrable to a 0.1.x.
  Several backend internals are copied rather than shared: the dead-letter sweep
  bound (`MAX_DEAD_LETTER_SWEEP = 128`, three copies), the `i64`/`Option<i64>` →
  `JobState` classify mapping (four copies), the dead-letter prune/retention
  computation (SQLite ↔ Postgres near-verbatim), and the `SCHEMA_VERSION = 1`
  baseline-rejection policy (three copies). `worklane_core::spi::reject_chars`
  and `redact_credentials` are the model: lift the shared *decision* into core
  and leave each backend only its dialect-specific statements.

### Ecosystem & Orchestration (out of core scope)

High-level patterns built at the client/application layer on the opaque
`Broker` primitives:

- **Webhooks / Event Egress** — standard user-space job handlers that make HTTP
  requests. The core does not include a native webhook dispatcher.
- **Outbox Pattern / Transactional Enqueue** — achieved by using
  `worklane-postgres` or `worklane-sqlite` within the application's native SQL
  transaction. No core change needed.
- **Exactly-once fan-in callback** — the fan-in callback is at-least-once today
  (its `fanin:{id}:callback` key releases when the callback completes, so a
  redelivered watcher generation can re-fire it; handlers must be idempotent).
  True exactly-once would need a new persistent, lifecycle-independent tombstone
  primitive (the `ResultStore` is TTL-bounded; `unique_key` is lifecycle-bound)
  plus an atomic "set marker ⊕ enqueue callback" step. Parked until a consumer
  needs exactly-once over at-least-once + idempotency.
- **Stronger cancellation adapters** — `JobContext::is_cancelled()` gives
  cooperative handlers a core signal for lease loss and timeout. Adapters for
  cancelling blocking libraries, database calls, or child processes remain
  user-space because cancellation mechanics vary by dependency.
- **Flow Control** — kept a user-space pattern: it combines four independent
  dimensions (what to limit, how, scope, on-exceed), so fixing one combination
  in core would limit users who need another. A `Broker::count_active` building
  block was considered and rejected — it would force a costly query method onto
  every backend and push the broker toward a general-purpose job-state database,
  violating *Minimal contracts*. Users implement flow control with external
  semaphores or token-buckets.

### Lane follow-ups

- **Multi-lane worker / fair scheduling across lanes** — one worker currently
  drains a single lane; multiple lanes mean multiple workers. True cross-lane
  fair scheduling is unlocked by the concurrent worker but deferred until a
  consumer needs it.

### Worker follow-ups

- **Structural handler decoupling** — the heartbeat already runs on its own task,
  but the handler-timeout is selected against the handler future on the worker's
  task, so it only fires when the handler yields; a handler that blocks its
  executor thread without yielding cannot be bounded by the timeout (and on a
  current-thread runtime also starves the heartbeat). Documented — handlers must be
  cooperatively async, CPU-bound work belongs in `spawn_blocking`. Decoupling the
  handler itself onto its own task would make the timeout independent too, but
  needs `Send + 'static` handler bounds — a breaking API change, deferred to a
  pre-1.0 evaluation.
- **Retry strategy trait** — a trait-based retry policy (constant, linear,
  decorrelated-jitter, deadline-aware). Deferred on the *late-introduction-cost*
  test, not for lack of a consumer: it is **additive-later and non-breaking** (add
  `trait RetryStrategy`, have `RetryPolicy` impl it, accept `impl RetryStrategy` in
  new overloads — old API untouched), so freezing a trait signature now, with no
  consumer to validate its shape, would only risk locking the wrong one. When it
  lands, the signature must carry `&Error` (back off differently per failure mode)
  and return a `RetryDecision` (retry-with-delay **or** give up), not merely a
  `Duration` — terminal-vs-retry is currently split out into the worker via
  `max_attempts`, and a real strategy needs to own that call.
- **Broker middleware framework** — the interception *seam already exists*: `Broker`
  is a public trait, so a user can wrap one today (`struct AuditBroker<B: Broker>(B)`)
  for audit/encryption/tracing. The baseline documents this and ships one reference
  decorator as an example. What stays deferred is an *auto-layering framework* that
  removes the ~18-method forwarding boilerplate — it is additive-later and
  non-breaking, and building it now would freeze a layering design with no consumer;
  baking interception into the `Broker` trait itself is explicitly rejected (it would
  break all four backends).
- **Broker lifecycle state machine** — *not backlog-tracked* and not a contract
  concern: the `Broker` trait methods are the external surface; an internal
  lifecycle state machine is invisible to users. The current enum + conformance
  suite suffices; revisit only if observable states (Paused/Deferred) ever grow.
- **Saga helpers** — a *scope-boundary* decision, not a timing one: compensating
  jobs for failed workflows belong above the core broker loop, built on
  fan-in/fan-out primitives in user space. Putting them in core would expand the
  contract surface and violate *Minimal contracts*, regardless of demand.

## Guiding principle

Protect the core loop. Most items above are out of scope until a real consumer
proves their shape. For library extension points specifically, the gate is the
**cost of late introduction**: an abstraction that can be added later *additively*
(a new trait, a decorator) is deferred rather than frozen prematurely; one whose
late introduction would be a *breaking* change (e.g. the typestate worker builder,
now promoted to active design) is committed pre-0.1 instead. Either way, none of
it silently touches the core contract.
