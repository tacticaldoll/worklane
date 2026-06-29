# Worklane Backlog

Future features intentionally **excluded from the baseline** unless a real
consumer proves their shape. Active work is tracked as OpenSpec changes under
`openspec/changes/`; this file is the upstream idea list that feeds
`/opsx:propose`.

The core loop (enqueue → reserve → dispatch → ack / retry / fail / dead-letter)
is solid across the in-memory, SQLite, Postgres, and Redis brokers.

## Strategy

`worklane` is a verified lifecycle queue. Its durable value is not a broad
transport abstraction, but a small job-lifecycle contract whose behavior is
checked across supported backends. Mature queue systems converge on the same
production pressures: precise lifecycle semantics, storage-native durability,
operator visibility, and predictable failure handling. This backlog records
work that strengthens those pressures without expanding the core contract before
a real consumer proves the shape.

## Shipped

- ✓ **Default-lease single source** (`default-lease-single-source`) — the default
  reservation lease (`Duration::from_secs(30)`), previously a `pub const`
  duplicated in all four backends plus the `worklane-test` harness and five
  per-backend contract-test `TEST_LEASE` copies, now lives once as
  `worklane_core::spi::DEFAULT_LEASE` (beside the other lifted shared defaults).
  Each backend re-exports it (`pub use`) so `worklane_<backend>::DEFAULT_LEASE`
  still resolves — non-breaking, value unchanged. Adversarial review moved it from
  the core root to `spi` (no user-facing consumer justified the root) and caught
  the missed `TEST_LEASE` copies. Archived `--skip-specs`.
- ✓ **Cross-broker decision dedup** (`cross-broker-decision-dedup`) — four shared
  cross-backend decisions that were copy-pasted per backend now live once in
  `worklane-core` (the `spi::reject_chars` model), so they cannot silently drift:
  the dead-letter sweep bound (`spi::MAX_DEAD_LETTER_SWEEP`; the Redis `RESERVE`
  script reads it from a new `ARGV[10]` instead of a Lua literal), the
  `Option<i64>` → `JobState` classify mapping (`spi::classify_state`), the
  `spi::SCHEMA_VERSION` const + `check_schema_version` match-vs-reject decision
  (each backend keeps its own dialect read/write **and** remediation message — the
  three differ and a Redis test pins its wording), and the dialect-independent
  retention prune math (`RetentionPolicy::age_cutoff_nanos` / `keep_count`, which
  also removed the third copy in the Redis reserve path). Behaviour-preserving: no
  `Broker`/API/schema/wire change; the API change is additive `spi` surface only.
  A new `poison_sweep_is_bounded_per_reserve` conformance scenario pins the sweep
  cap's observable bound (none existed) as the regression gate. `DEFAULT_LEASE`
  was deliberately left out as a user-facing default and shipped separately right
  after (see *Default-lease single source* above). Archived `--skip-specs` (no
  observable behaviour change).
- ✓ **Postgres `enqueue_batch` no-unique-key UNNEST fast path**
  (`postgres-enqueue-batch-unnest`) — when every job in a batch has no
  `unique_key`, `PostgresBroker::enqueue_batch` skips the per-row dedup/claim
  loop and stores the whole batch with one multi-row `INSERT … SELECT FROM
  UNNEST(…) WITH ORDINALITY … ORDER BY ord ON CONFLICT (id) DO NOTHING` (a new
  `insert_batch_unnest` helper; the fixed-shape statement is precomputed in
  `Queries`). `WITH ORDINALITY` pins `BIGSERIAL seq` assignment to input order so
  the batch reserves back strict-FIFO — a plain `UNNEST` gives no such guarantee.
  Batches with any unique-key job keep the existing advisory-lock-sorted per-row
  path unchanged. A new `batch_mixed_unique_and_plain` conformance scenario
  guards the `all(unique_key.is_none())` gate (a mixed batch must still dedup),
  across every broker. Behavior-preserving (no `Broker`/`BatchEnqueue`/API/schema
  change); the `worklane-test` Postgres batch battery is the regression gate, so
  no spec delta (archived `--skip-specs`). Measured ~5,400 → ~9,000–10,000 jobs/s
  on a no-unique-key batch (single-node `postgres:16`, N=5000, chunk=500),
  narrowing the gap to `apalis` `push_bulk` from ~3.4× to ~1.3×.
- ✓ **CLI job classification** (`cli-classify`) — `wl classify <job-id>` reports a
  job's lifecycle state (`Live` / `DeadLettered` / `CompletedOrUnknown`) via the
  existing `Broker::classify` point lookup, as a human-readable line or
  `--format json`. The id is parsed at the CLI layer so an invalid id exits
  non-zero before any broker connection opens. CLI-only — no `Broker` trait or
  `worklane-core` change. Completes the operator's by-id lifecycle question
  alongside the existing `stats` and `dead-letters` commands.
- ✓ **Verified broker extensibility** (`verified-broker-extensibility`, 0.2.0) —
  the `Broker` contract is now an explicit minimal lifecycle core (enqueue,
  reserve, ack, retry, defer, extend, fail, classify) plus opt-in capability
  traits discovered through `Option<&dyn Cap>` accessors. Batch enqueue moved
  off the required trait into a new `BatchEnqueue` capability (joining the
  already-split `DeadLetterStore`/`QueueStats`/`ScheduledStore`); absent
  capabilities fail predictably with `Error::UnsupportedCapability`.
  `worklane-test` is now a modular conformance suite — one mandatory lifecycle
  battery plus per-capability batteries gated on capability presence, exported
  for third-party brokers, with omitted capabilities reported visibly.
  `worklane_core::spi` is documented as the broker-author surface, and the
  `broker-extensibility` spec, lifecycle-semantics guide, custom-broker
  conformance guide, and broker conformance matrix make the contract legible.
  Breaking pre-1.0 API change; all four first-party brokers migrated. Result
  storage stays storage-adjacent (its own `ResultStore` harness), not promoted
  to a `Broker` accessor.
- ✓ **Redis hot-path script cache** — `RedisBroker` now builds and SHA1-hashes
  each lifecycle Lua script exactly once at construction (a `scripts::Scripts`
  struct populated in `connect_with_namespace`, the Redis analogue of the
  Postgres `Queries` precompute) and reuses the cached `redis::Script` on every
  call, instead of re-running `format!`+`Script::new` (string concat + SHA1) per
  call on the throughput-critical consume loop. All 13 call sites — the enqueue
  family, reserve/ack/retry/defer/fail/extend, requeue, purge_dead,
  pending_count, and the previously inline `classify` literal (now a `CLASSIFY`
  const) — share the cached values. Behavior-preserving (no script body, key
  layout, or `KEYS`/`ARGV` change); the `worklane-test` Redis conformance suite
  passes unchanged as the regression gate.
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

### Strategic lifecycle positioning

- **Lifecycle semantics guide** — document the exact observable behavior for
  enqueue, reserve, ack, retry, fail, lease expiry, stale resolution,
  dead-lettering, scheduling, uniqueness, and delayed visibility in one
  production-facing guide. The guide should point to the OpenSpec capabilities
  as the source of truth rather than creating a second contract.
- **Operator lifecycle inspection** — grow the CLI around lifecycle questions
  before building a dashboard. Shipped so far: lane health (`stats`),
  dead-letter inspection/requeue/purge (`dead-letters`), and job classification
  (`classify`, see *Shipped*). Remaining ideas — richer counts
  (running/delayed/failed) and storage diagnostics — would need new broker
  surface (`QueueStats` exposes only `pending_count` today), so they are gated on
  a real consumer rather than added speculatively. These commands should expose
  what already exists in the contract before they justify new broker surface.
- **Production patterns documentation** — collect application-level recipes for
  idempotent handlers, transactional enqueue, outbox integration, rate limiting,
  fan-out/fan-in, webhooks, and cancellation. Patterns that can be handlers,
  wrappers, metrics, or adapters stay outside the broker contract.
- **Conformance matrix** — publish a backend-by-backend lifecycle matrix showing
  which requirements each broker satisfies through `worklane-test`, including
  optional capabilities such as scheduled enqueue, queue stats, dead-letter
  inspection, and result stores.
- **Custom broker conformance guide** — document how a broker author wires a
  private or third-party broker into `worklane-test`, which lifecycle scenarios
  are mandatory, how optional capabilities are declared, and what passing the
  suite means for compatibility.

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

### Broker SPI & extensibility (shipped in 0.2.0; remainder parked)

The positioning differentiator (see the README's *What makes it different*) is a
*conformance-verified* job-lifecycle broker contract. The extension model —
turning "**we** support N backends" into "**anyone** can add one, safely" —
shipped in 0.2.0 via `verified-broker-extensibility` (see *Shipped* above):
capability segregation, the documented `worklane_core::spi` surface, and the
modular `worklane-test` conformance suite are done. What remains is depth, still
parked until there is a real external-broker consumer (a NATS/SQS demand, or a
committed third-party-broker product strategy):

- **Adversarial conformance depth** — extend the now-modular `worklane-test`
  with fault-injection, concurrency, and clock-skew coverage so it serves as a
  hardened published acceptance test for third-party brokers. The per-capability
  restructure shipped in 0.2.0; this is the additional adversarial coverage on
  top of it.
- **SPI versioning guarantees** — formalize stability/versioning guarantees for
  `worklane_core::spi` and the capability traits once an external broker pins to
  them. The surface and audience are documented (0.2.0); the formal
  version-compatibility promise is what remains.

### Storage representation

- **Columnar envelope schema** — parked (no consumer; design ready). Promoting
  the SQL brokers' remaining envelope fields (`kind`, `payload`, `max_attempts`,
  `trace_context`) to queryable columns plus a `JobEnvelope::from_stored`
  reconstruction constructor would let an operator query or report by `kind` or
  attempt count, but it erodes the opaque-envelope principle (adding a field
  becomes a three-touch change across the serde struct, each SQL backend, and
  `from_stored`) and has no proven consumer query. Park until a consumer such as
  a CLI `wl jobs --kind X` exists.

### Performance & hardening (perf/risk scan)

Findings from the scan that motivated the **Redis hot-path script cache** (now
shipped). Positioned here as separate future proposals — none is implemented by
that change.

- **P2 — Postgres `enqueue_batch` no-unique-key UNNEST fast path** — shipped (see
  *Shipped*: `postgres-enqueue-batch-unnest`).
- **P3 — idle-poll tax (documented, no code change needed)** — the poll-based
  design's idle cost is now recorded in `docs/known-limitations.md` ("Poll-Based
  Idle Load"). Re-measured: 16 consumers spinning `reserve` on an empty lane
  sustain ~2,500 empty reserves/s on Postgres and ~96,000/s on Redis (raw
  round-trip rates, single-node localhost — correcting the earlier ~4,000 /
  ~87,000 estimate). The doc frames these as per-call costs, not steady-state
  worker load: `Worker::run` already paces idle polling via `poll_interval`
  (default 1s) plus exponential idle backoff, so a default idle worker polls
  about once per second. `LISTEN`/`NOTIFY` is explicitly **not** added — it
  reintroduces the commit serialization worklane deliberately avoids. Revisit
  only if a push-delivery backend is ever in scope.
- **R1 — clock-skew conformance (forward direction already covered)** — the
  forward-step lease behavior (an in-flight lease expires → the job is
  redelivered → the superseded receipt is rejected as `StaleReservation`) is
  already verified cross-backend by the timed battery
  (`superseded_receipt_rejected_current_resolves`,
  `expired_receipt_rejected_without_mutation`): a `ManualClock.advance(lease)` is
  indistinguishable from an NTP forward jump at the broker layer, which only
  compares `now` against a stored absolute deadline. The backward-skew guarantee
  is a `WallClock` property (the `floor_nanos` clamp), already unit-tested in
  `worklane-core`; feeding a broker a non-monotonic clock would test the wrong
  layer, so `ManualClock` `set`/rewind is intentionally not added. Residual: at
  most a one-line module-doc note attributing the backward guarantee to
  `WallClock` rather than the broker — not worth a dedicated change.
- **R2 — SQLite `insert_job` dedup is already safe (not needed)** — the concern
  was that the plain `INSERT INTO unique_keys` after a check-then-insert could
  conflict and error under concurrent connections (the WAL pool, or a second
  process). Audit shows it cannot: `init_connection` sets
  `TransactionBehavior::Immediate`, so every broker write (`unchecked_transaction`)
  takes the write lock at `BEGIN`, serializing all writers cross-connection and
  cross-process via SQLite's file lock. A losing writer cannot run its dedup
  `SELECT` until the winner commits, so it always sees the committed holder and
  returns the existing id — the conflicting-`INSERT` path is unreachable. Verified
  by the file-backed concurrent conformance test (`concurrent_unique_enqueue_dedups`,
  30/30) and a `BEGIN IMMEDIATE` lock spike. The protection is write
  serialization at `BEGIN`, not deployment discipline; Postgres needs its
  `ON CONFLICT` claim loop only because READ COMMITTED lets its initial `SELECT`
  race, which `BEGIN IMMEDIATE` forecloses here. Revisit only if the broker ever
  moves off `BEGIN IMMEDIATE`.

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

### Governance / boundary enforcement

`crates/worklane-governance` makes the crate-graph invariants executable via a
`tianheng` constitution (worklane-core portability, backend substitutability —
see AGENTS.md). Scope is deliberately least-commitment; these are *candidate*
boundaries, not yet justified by an invariant this repo asserts, so they are
deferred rather than pre-built:

- **Facade-direction rules** — assert that the dependency arrow points toward
  `worklane-core` and that nothing depends on the `worklane` facade. Deferred for
  two reasons, not one. (1) The load-bearing part is already enforced:
  `worklane-core` forbids all workspace deps, and every broker plus
  `worklane-test` is restricted to `worklane-core`, so none of them can reach the
  facade. (2) The residual is an *incoming* (reverse-dependency) invariant, but
  `tianheng` 0.1.0 has only outgoing crate rules; it could be expressed only by
  hand-listing a `forbid_dependency_on(["worklane"])` per crate, which inverts the
  safe default — a new crate would be ungoverned — the very anti-pattern the
  membership-derived rules exist to avoid. And `worklane-pubsub` depends on the
  facade on purpose (a layer above the public API), so the naive "nothing depends
  on the facade" form is already false. Revisit only if `tianheng` gains a
  reverse-dependency rule and a concrete invariant names which crates may sit
  above the facade.
- **Intra-crate module layering** — `tianheng`'s `ModuleBoundary` can forbid
  `use` edges Cargo cannot see (e.g. envelope/model code importing broker
  internals inside `worklane-core`). Deferred until a concrete layering invariant
  exists to protect — adding one now would be inventing policy, not recording it.
- **Severity tiers / baseline** — `tianheng` supports advisory (`warn`)
  boundaries and a baseline file to ratchet down existing violations. Unused
  while every declared boundary is clean at `enforce`; reach for it only when
  introducing a boundary the tree does not yet satisfy.
- **Semantic / runtime dimensions** — `tianheng` adds observation dimensions
  `modou` lacked: semantic (渾儀 / `hunyi`: signature coupling, trait-impl
  locality, visibility) and runtime (漏刻 / `louke`: concrete-type origins
  crossing a seam). `worklane` declares only the static crate-graph dimension
  today; reach for these only once a concrete invariant of that kind exists to
  protect, not because the affordance is now available.

## Guiding principle

Protect the core loop. Most items above are out of scope until a real consumer
proves their shape. For library extension points specifically, the gate is the
**cost of late introduction**: an abstraction that can be added later *additively*
(a new trait, a decorator) is deferred rather than frozen prematurely; one whose
late introduction would be a *breaking* change (e.g. the typestate worker builder,
now promoted to active design) is committed pre-0.1 instead. Either way, none of
it silently touches the core contract.
