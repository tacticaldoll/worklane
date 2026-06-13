## Context

The broker contract (enqueue, lane-scoped reserve under a visibility lease,
receipt-validated ack/retry/fail, dead-letter, injectable time source) is
specified in `openspec/specs/broker` and executable as the `worklane-test`
conformance suite (`broker_contract_required!` + `broker_contract_timed!`). Its
only implementation so far, `InMemoryBroker`, stores jobs in a `Vec<JobState>`
behind a `Mutex` — the same in-process paradigm the contract was first written
against.

The `AGENTS.md` **Broker design gate** states the trait is not stable until a
durable backend passes the suite *without changing it*. This change adds that
backend — a SQLite broker — as the decoupling milestone (Near-term sequencing
step 3). It is governed by *Least commitment*, *Minimal contracts*, and the
Broker design gate. The explicit, verifiable success condition: the suite passes
both tiers and `worklane-core` is one line unchanged.

## Goals / Non-Goals

**Goals:**
- A `worklane-sqlite` crate implementing `Broker` on SQLite, proven by the
  shared conformance suite (both tiers green).
- Validate the `Broker` trait — and every public type that flows through it —
  against a row/SQL/transaction storage paradigm with **zero `worklane-core`
  code changes**. A forced trait/type change would be the signal to fix the
  contract, not the implementation.
- Close the one gap a serializing backend makes real — payload/envelope
  preservation across persistence — by completing the existing *Backend-agnostic
  payloads* requirement, with a shared conformance scenario both brokers pass.
  (The `Broker` trait stays unchanged; this strengthens the contract's prose,
  not its code surface.)
- Derive all time decisions from an injected `Clock` so the timed tier is
  deterministic (the spec's *Injectable time source* requirement).
- Keep the suite's per-scenario isolation trivial (fresh database per scenario).

**Non-Goals:**
- No `next_available_at` (precise wakeup) and no lease extension/renewal — each
  adds a `Broker` trait method and would trip the decoupling tripwire. This
  broker is recorded as their eventual validation site; not built here.
- No connection pool / multi-connection concurrency — a step-4 (concurrent
  worker) concern.
- No columnar schema and no `JobEnvelope::from_stored` constructor — deferred to
  the first columnar backend (Postgres), its real consumer.
- No restart-durable wall-clock epoch — see Risks.
- No change to the `Broker` trait or any `worklane-core` public type.
- No broker requirement removed and no new runtime behaviour introduced; the
  only spec change MODIFIES *Backend-agnostic payloads* to make explicit a
  preservation obligation no existing broker behaviour violates.
- No `reserve` ordering guarantee (e.g. strict FIFO per lane); recorded in
  BACKLOG, not specified (see Decision 8).

## Decisions

### 1. Backend crate: `rusqlite` with the `bundled` feature

SQLite is the simplest durable backend (embedded, no server). `rusqlite`
(synchronous, thin libsqlite3 wrapper) is chosen over `sqlx` (async-native but
heavier: connection pools, compile-time query macros, offline-data ceremony) —
the milestone wants the *simplest* durable broker, not an async-DB framework.
The `bundled` feature compiles SQLite from source, so there is no system
`libsqlite3` dependency (hermetic CI). `bundled` ships a modern SQLite, so
`UPDATE … RETURNING` (SQLite ≥ 3.35) is available.

*Alternative considered — `sqlx`:* its native async would remove the
`spawn_blocking` bridge below, but it pulls a large dependency tree and macro
machinery disproportionate to a first durable broker. Revisit if/when a Postgres
broker lands (sqlx supports both).

### 2. Async bridge: `spawn_blocking` over `Arc<Mutex<Connection>>`

`Broker` is `#[async_trait]`; `rusqlite` is synchronous. Each method clones an
`Arc<Mutex<rusqlite::Connection>>`, enters `tokio::task::spawn_blocking`, locks,
and runs its SQL. This keeps synchronous SQLite calls off the async runtime
threads (honest async), at negligible cost. A single connection behind a `Mutex`
serialises DB access — appropriate because SQLite serialises writes anyway, and
because an in-memory database (Decision 5) is private to its one connection, so
a pool would break isolation. `rusqlite::Connection` is `Send`, so the
`Arc<Mutex<…>>` moves into `spawn_blocking` cleanly.

*Alternative considered — call `rusqlite` synchronously inside the async fn
(holding a `Mutex` across the call):* simplest, and the suite would pass, but it
blocks an executor thread for each call. Since step 4 (concurrent workers) would
force the `spawn_blocking` idiom anyway, adopting it now avoids a known-wrong
pattern; it is execution hygiene, not a speculative abstraction.

### 3. Storage shape: serialized envelope blob + denormalized index columns

`JobEnvelope` already derives `Serialize`/`Deserialize` and is designated "the
durable, on-the-wire job format" (`AGENTS.md`). The SQLite broker stores that
serde form as the source of truth, with a small set of columns denormalized
purely as a query/resolution index:

```sql
CREATE TABLE jobs (
  seq          INTEGER PRIMARY KEY,   -- implicit rowid: stable FIFO order
  receipt      TEXT,                  -- current receipt; NULL = unleased
  lane         TEXT    NOT NULL,      -- reserve filters by lane
  available_at INTEGER NOT NULL,      -- nanos since clock epoch; visible when <= now
  leased_until INTEGER,               -- nanos; NULL = unleased; expired when <= now
  envelope     BLOB    NOT NULL       -- serde_json(JobEnvelope), source of truth
);
CREATE TABLE dead (
  seq      INTEGER PRIMARY KEY,
  envelope BLOB NOT NULL,
  error    TEXT NOT NULL
);
```

- `enqueue`: serialize the fresh envelope (`attempts = 0`), insert with
  `available_at = now`, `leased_until`/`receipt` NULL.
- `reserve`: atomic statement (Decision 4); returns the blob, deserialized.
- `retry`: deserialize blob → `env.attempts += 1` → reserialize; set
  `available_at = now + delay`, clear `leased_until`/`receipt`. Mutating the
  `attempts` **public field** on an owned, deserialized value is permitted under
  `#[non_exhaustive]` (which blocks only struct-literal construction).
- `ack`: delete the row whose receipt is current and valid.
- `fail`: move the row to `dead` with the error.

This requires **zero `worklane-core` changes** — the strongest form of the
decoupling tripwire (not just the trait, all of core stays untouched), achieved
via the serde path the envelope was designed for.

*Alternative considered — fully columnar schema* (`id, lane, kind, payload,
attempts, max_attempts, …` as columns): more SQL-idiomatic and what a Postgres
broker would want, **but** rebuilding a `JobEnvelope` from a row with
`attempts ≠ 0` is impossible from an external crate, because `JobEnvelope` is
`#[non_exhaustive]` and `JobEnvelope::new()` hardcodes `attempts = 0`. That gap
(a value type that cannot be rehydrated despite being "the on-the-wire format")
is a real finding from this probe; fixing it means an additive
`JobEnvelope::from_stored(...)` constructor. Per *Least commitment*, that
constructor is introduced with its first real consumer — the columnar Postgres
broker — not now. Recorded in BACKLOG.

### 4. `reserve` as one atomic `UPDATE … RETURNING`

```sql
UPDATE jobs SET receipt = ?receipt, leased_until = ?lease_until
WHERE seq = (
  SELECT seq FROM jobs
  WHERE lane = ?lane
    AND available_at <= ?now
    AND (leased_until IS NULL OR leased_until <= ?now)   -- expired lease = visible
  ORDER BY seq LIMIT 1                                    -- FIFO by enqueue order
)
RETURNING envelope;
```

One statement, atomic under the serialized connection: no separate "sweep
expired leases" pass is needed — expiry falls out of the `leased_until <= now`
predicate on read. Resolution validity (ack/retry/fail) is "a row with this
receipt exists **and** its `leased_until > now`"; otherwise the receipt is stale
(covers expired, superseded, and never-issued receipts uniformly), mirroring
`InMemoryBroker::find_current_receipt`.

*Broker design gate answer (how would a SQL/Redis backend do this?):* this is
exactly Postgres's `SELECT … FOR UPDATE SKIP LOCKED` over the same predicate; the
shape is portable, confirming the operation is not in-memory-specific.

### 5. Time as integer nanoseconds; `Clock` read fresh per operation

`available_at` and `leased_until` are stored as `INTEGER` nanoseconds
(`duration.as_nanos() as i64`, rebuilt via `Duration::from_nanos`). Realistic
monotonic-since-epoch values fit `i64` comfortably. Every operation reads
`clock.now()` fresh (no caching), so advancing a `ManualClock` between calls
drives lease expiry and scheduled visibility deterministically — satisfying the
*Injectable time source* requirement and the timed tier.

### 6. Per-scenario isolation: a fresh in-memory SQLite database

`reserve`/lease/visibility logic is identical whether the database is `:memory:`
or a file, so the conformance suite runs against
`rusqlite::Connection::open_in_memory()` — a private database per connection,
giving perfect per-scenario isolation with no temp files or cleanup. The suite
macros already re-evaluate `Harness::new()` per generated test, so each scenario
gets a fresh database and schema. A `SqliteBroker::open(path)` constructor
provides real on-disk durability for production use; disk-vs-memory is a
connection-string choice, not a separate code path. The harness's broker
construction is the durable analogue of the in-memory harness's `fresh_broker`.

### 7. Constructors mirror `InMemoryBroker`

`SqliteBroker::open(path)` (system clock, default lease), `::open_in_memory()`,
`::with_clock(conn_or_path, Arc<dyn Clock>)`, and `.with_lease(Duration)` builder
— matching the `InMemoryBroker` surface so the two harnesses are near-identical
and the substitutability is visible. Schema DDL (`CREATE TABLE IF NOT EXISTS`)
runs at construction; no migration framework (*Least commitment*; `PRAGMA
user_version` versioning recorded as a future item).

### 8. Close a real gap (payload preservation); defer the FIFO gap

The first durable broker forces two genuine gaps in the broker spec into the
open — gaps an in-memory `Vec` broker silently papered over. Each is resolved by
its kind: one *needs* deciding now (fold in), one does not (record).

**Folded in — payload/envelope preservation.** *Backend-agnostic payloads*
currently says only that the broker MUST NOT *inspect or deserialize* the
payload; it never says the broker must *preserve* it. An in-memory broker holds
the live value, so preservation is automatic and the gap is invisible. A
serializing backend is the first that could re-encode, reorder, or truncate the
bytes while still never "deserializing" them — so the obligation must be made
explicit. We therefore **MODIFY** *Backend-agnostic payloads* to require every
envelope field (including the opaque `payload` bytes) to return unchanged across
a storage round-trip, and add one **required-tier** scenario to the shared
`worklane-test` suite (enqueue a job with a distinctive `kind`, non-UTF-8
payload, and specific `max_attempts`; reserve; assert every field equal). Both
brokers run it: `InMemoryBroker` passes by identity, `SqliteBroker` via the
serde round-trip — keeping the suite the single backend-agnostic mirror.

**Deferred — `reserve` ordering.** `reserve` is specified as "at most one
currently-visible job" but is silent on *which* job when several are visible on a
lane. In-memory returns Vec-insertion order (de facto FIFO); SQLite must write an
explicit `ORDER BY` (rowid order is not guaranteed). This is a real gap, but
specifying strict FIFO now would pre-commit the contract against the backlogged
**priority-queue** feature, which deliberately reorders. Per *Least commitment*
it is recorded in BACKLOG as a noticed-but-deferred observation, not folded into
this delta; the implementation uses `ORDER BY seq` (FIFO) as an unspecified
implementation choice.

*Why any spec change in a "validate without changing the contract" milestone:*
the decoupling tripwire AGENTS.md names is the `Broker` **trait** (code), which
is untouched. Completing an existing *requirement* (with a test both brokers
satisfy) follows the precedent of `establish-broker-contract`, which added the
*Injectable time source* requirement while leaving the trait unchanged.

*Alternative considered — assert preservation only in the SQLite broker's own
test:* rejected. Preservation is a backend-agnostic property; asserting it for
one backend only would fragment the contract away from the shared suite, against
the conformance-kit philosophy.

## Risks / Trade-offs

- **Cross-restart lease correctness** → `SystemClock` is `Instant`-based
  (monotonic, process-local epoch); persisted absolute times are meaningless to
  a fresh process's epoch. Framing: the broker is correct w.r.t. whatever
  `Clock` it is given; restart-durable correctness needs a stable wall-clock
  epoch and is the *clock's* concern, not the broker's. The suite never restarts
  mid-lease, so this does not block the milestone. Recorded in BACKLOG as
  "production durable clock = wall-epoch"; not built (no consumer restarts the
  broker yet).
- **`bundled` adds a C build step** → SQLite compiles from source on first
  build / CI. Accepted: it removes the system-`libsqlite3` dependency and makes
  builds hermetic and reproducible.
- **Blob + index-column redundancy (`lane`, `available_at`)** → `lane` lives in
  both the blob and a column; they could drift. Mitigated: the column is written
  only from the same envelope being serialized in the same statement, and `lane`
  is immutable after enqueue. `attempts`/`available_at` mutations always
  round-trip through the blob, keeping it authoritative.
- **`retry` deserialize→bump→reserialize cost** → small and bounded; retries are
  not hot-path. Accepted in exchange for zero core change. A columnar schema
  would avoid it and is the recorded future refinement.
- **Suite/spec drift** → mitigated by keeping every scenario in the *shared*
  `worklane-test` suite (including the new fidelity scenario), run by both
  brokers; no broker asserts the contract privately, so no backend can drift
  from it independently.

## Migration Plan

Pre-release, no persisted state to migrate. Order: (1) scaffold
`crates/worklane-sqlite` and wire `[workspace.dependencies]`; (2) implement
`SqliteBroker` (schema DDL, the five `Broker` methods, constructors) against
`InMemoryBroker` as the behavioural reference; (3) add the harness + invoke both
suite macros; (4) green the DoD (build / test / clippy `-D warnings` / fmt);
(5) record the `AGENTS.md` durable-validation note and the BACKLOG deferrals.
Rollback = revert the commits; nothing else depends on the new crate.

## Open Questions

None blocking. The `from_stored`/columnar-schema refinement and the
restart-epoch clock are deliberately deferred (recorded in BACKLOG), to be
revisited with their first real consumers (a Postgres broker; a restart-durable
deployment).
