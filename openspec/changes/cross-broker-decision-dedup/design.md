## Context

`worklane`'s value is that four brokers (memory, SQLite, Postgres, Redis) honour
the *same* observable lifecycle semantics, proven by `worklane-test`. Several
shared *decisions* are nonetheless copy-pasted per backend:

- **Dead-letter sweep cap** `128` — `MAX_DEAD_LETTER_SWEEP` in
  `worklane-memory` (lib.rs:27), `worklane-postgres` (lib.rs:594),
  `worklane-sqlite` (lib.rs:322), and the Redis Lua literal `sweep_cap = 128`
  inside the `RESERVE` script (`scripts.rs:143`). Four copies.
- **Classify mapping** `1→Live, 2→DeadLettered, _→CompletedOrUnknown` — three
  hand-rolled integer matches (`worklane-sqlite` lib.rs:498, `worklane-postgres`
  lib.rs:777, `worklane-redis` lib.rs:550). The memory broker returns `JobState`
  directly and is not part of this.
- **Schema/baseline version policy** — `SCHEMA_VERSION: i64 = 1` plus the
  pre-1.0 "do not migrate; reject a mismatched version" rejection, duplicated in
  `worklane-sqlite/src/schema.rs` and `worklane-postgres/src/schema.rs`, with an
  analogous baseline check on the Redis side.
- **Dead-letter retention prune** — the per-backend translation of a
  `RetentionPolicy` into a prune is near-verbatim between the SQL backends.

`worklane-core` already proves the right pattern: `spi::reject_chars` and
`redact_credentials` are shared *decisions* lifted into core while each backend
keeps its dialect-specific statements. `RetentionPolicy` itself (the `max_age` /
`max_count` bounds, `is_unbounded`, and `UnboundedDlqWarning`) is *already* a core
type — so the retention item here is narrow: only the prune *computation* that is
still duplicated, never the policy.

## Goals / Non-Goals

**Goals:**

- One source of truth in `worklane-core` for each shared decision: the sweep cap,
  the classify code→state mapping, the schema-version rejection policy, and any
  retention prune math that is dialect-independent.
- Strictly behaviour-preserving: the `worklane-test` lifecycle and dead-letter
  conformance batteries pass unchanged across all four backends.
- Leave each backend only its dialect-specific statements (SQL text, Lua,
  `rusqlite`/`tokio-postgres`/`redis` calls).

**Non-Goals:**

- No `Broker` trait, core job-trait, `JobEnvelope`, on-disk schema, or wire-format
  change — so the *Broker design gate* is not engaged (the trait is untouched).
  (The API does change *additively* in `spi` — see D5 — which is not a contract
  break and not the design gate.)
- No change to retention *policy* surface (`RetentionPolicy`'s fields/builders are
  already core; D4 only adds a computation method).
- Not lifting dialect-specific SQL/Lua **or operator-visible messages** into core —
  that would over-centralise and violate *Minimal contracts* in the other
  direction (see D3).
- No new observable capability and no observable behaviour/error-text change, so no
  spec delta (archived `--skip-specs`; the `spi` growth is argued against the
  existing broker-extensibility requirement in D5).
- `DEFAULT_LEASE` (duplicated `pub` in all four backends) is **out of scope**: it
  is a user-facing constructor default whose lift needs a re-export/deprecation
  decision — recorded in BACKLOG as a separate API-compat change.

## Decisions

### D1 — Classify mapping: one core function over `Option<i64>`

Add a `worklane-core` function (in `spi`, beside `reject_chars`) mapping the
stored status code to a `JobState`: `1 → Live`, `2 → DeadLettered`, everything
else (including absent) `→ CompletedOrUnknown`. It takes `Option<i64>` so it
covers all three call sites uniformly — SQLite matches on `Option<i64>`, Postgres
has an outer `None` arm plus inner `1/2/_`, Redis matches a bare `i64`; all three
collapse to one call.

*Alternatives rejected:* a `i64`-only function (would leave SQLite/Postgres
hand-writing the `None` arm — partial dedup); a `From<i64> for JobState` impl
(`JobState` is a core lifecycle type; a lossy numeric `From` invites misuse where
the code is not a status code).

### D2 — Sweep cap: core const, injected into Redis via `ARGV`

Define `MAX_DEAD_LETTER_SWEEP` once in `worklane-core`. Memory, SQLite, and
Postgres reference it directly. **The Redis `RESERVE` script receives it as a new
`ARGV` parameter** (today it already takes `ARGV[1..9]`; the cap becomes
`ARGV[10]`, replacing the `local sweep_cap = 128` literal). The Rust caller passes
the core const when building the `EVAL` arguments.

*Alternatives rejected:* keeping the Lua literal and adding a coupling test that
asserts it equals the core const. That leaves **two** sources of truth and only
detects drift *after* someone edits one — the test is a tripwire, not a single
source. ARGV injection makes the core const the only value that exists. The cost
is a one-element `ARGV` arity bump on an internal script (not a wire/API change),
which the conformance suite covers.

### D3 — Schema-version policy: core const + decision; messages stay per-backend

Lift `SCHEMA_VERSION` and the match-vs-reject *decision* into a core helper —
given the version read from storage (`Option<i64>`), it tells the caller match vs.
mismatch (e.g. returns the mismatched value, or `Ok(())`/typed mismatch). Each
backend keeps its dialect-specific *read/write* of the stored version
(`PRAGMA user_version` for SQLite, the meta row for Postgres, the
`ns:schema_version` key for Redis) **and constructs its own remediation
message**.

**Correction from review:** the three rejection messages are deliberately
*different* and operator-visible (SQLite "drop and recreate the database",
Postgres "drop and recreate the schema", Redis "flush the namespace and
re-enqueue"), and `worklane-redis/tests/migration.rs` asserts the Redis wording
(`contains("flush") || contains("re-enqueue")`). Lifting a single "standard"
message would change operator-visible error text and break that test — so the core
surface carries only the *version* and the *decision*, never the message string.
This keeps the lift behaviour-preserving.

*Alternatives rejected:* lifting the whole schema-guard incl. the message (changes
operator-visible text, breaks the Redis migration test); a parameterized
message-builder in core (more surface than the decision needs; the message is
genuinely dialect-specific); leaving it (the const and the match/reject decision
silently diverge — e.g. bumping the version in one backend only).

### D4 — Retention prune: lift the dialect-independent computation

Review confirmed the duplication is real and present (not speculative): the same
two dialect-independent computations are copied verbatim in **three** places —
the age-cutoff instant `now.saturating_sub(nanos(max_age))` and the count-keep
bound `i64::try_from(max_count).unwrap_or(i64::MAX)` — in
`worklane-sqlite/src/dead_letters.rs`, `worklane-postgres/src/dead_letters.rs`,
and the Redis reserve path (`worklane-redis/src/lib.rs:398`, feeding `ageCutoff`
into `RESERVE`/`dead_letter_move`). Express both once as a method on the core
retention surface (returning the cutoff instant and the keep-count), and have each
backend feed the result into its own `DELETE`/sweep — including Redis, so the lift
removes the third copy rather than leaving it behind.

*Alternatives rejected:* a core `prune()` that emits SQL (dialect leak); a trait
method on `Broker` (contract growth for an internal concern — rejected, matches
the backlog's `count_active` rejection); deferring D4 to BACKLOG (rejected — the
duplication exists *now* and is the change's own thesis; the earlier "may be a
no-op" hedge was wrong, the third Redis copy makes it concrete).

### D5 — Placement in `spi`; additive public API, not a spec change

The classify helper, the sweep-cap const, and the `SCHEMA_VERSION` const +
decision land in `worklane_core::spi`, beside `reject_chars` / `nanos` / `stale`.
Cross-crate sharing **forces `pub`** (a `pub(crate)` const in `worklane-core` is
invisible to the backend crates), so these are public additions, not internal —
the proposal's earlier "API unchanged" was wrong; the API change is *additive* per
*API stability and evolution* (new items added, none removed/renamed).

This does **not** require a spec delta. The `broker-extensibility` spec's "Broker
author SPI" requirement already states SPI items "SHALL encode shared backend
decisions **such as** envelope encoding, receipt encoding, duration conversion,
… redaction, and backend name validation helpers" — an open list. A status-code
classify mapping, a dead-letter sweep bound, and a schema-version policy are the
same *kind* of shared backend decision; they extend the open list without changing
the requirement's normative text. Each serves multiple backends, so the
requirement's "Backend-local helper is not promoted" scenario is satisfied (these
are genuinely shared, not one-backend conveniences). Per that same requirement the
items are documented as **broker-author audience** and are **not** re-exported from
the `worklane` facade.

*Alternatives rejected:* `pub(crate)` (impossible — backends are separate crates);
a `MODIFIED` delta to "Broker author SPI" (the normative text is unchanged; the
"such as" list is illustrative, not exhaustive — a delta would imply a contract
change that is not happening); putting them outside `spi` in an undocumented core
module (would orphan them from the governed broker-author surface).

### D6 — Add the missing sweep-cap conformance test before touching Redis

Review found `worklane-test` asserts that dead-lettering *happens* but never that a
single `reserve` sweeps **at most** the cap before yielding empty — so D2's ARGV
change has no real regression gate today (a wrong `ARGV[10]`, an off-by-one, or a
`0` could leave every test green). Before changing the Redis script, add a
conformance scenario that enqueues more than the cap of expired/poison jobs on one
lane and asserts (a) a single `reserve` dead-letters a bounded number and yields
empty, and (b) a subsequent `reserve` continues the sweep (the "bounded progress"
property). This runs across all four backends and becomes the gate for D2.

*Alternatives rejected:* relying on the existing dead-letter scenarios (they do not
observe the bound — the exact silent-pass risk); a Redis-only unit test (the bound
is a cross-backend behavioural property and belongs in the shared suite).

## Risks / Trade-offs

- **Redis `ARGV` arity change (D2) could desync caller and script.** → The new D6
  sweep-bound scenario (plus the existing reserve/dead-letter battery) exercises
  the exact path; a missing/extra/`0` ARGV fails it. The script is internal, not a
  public contract. Sequencing: the Lua `tonumber(ARGV[10])` read and the 10th
  `.arg()` at the call site must land in the same edit.
- **Over-lifting dialect logic into core**, inverting the very principle this
  serves. → Lift *decisions* only (const, mapping, policy, pure math); keep all
  SQL/Lua statements AND operator-visible messages per-backend (D3).
  `spi::reject_chars` is the size template.
- **classify `Option` vs non-`Option` and `i32` vs `i64` (D1).** → The core fn
  takes `Option<i64>`; the SQL backends bind the status column as `i32` and must
  add a `.map(i64::from)` at the call site (do NOT change the column read type),
  Redis wraps its bare `i64` in `Some(..)`. `None`/unknown both map to
  `CompletedOrUnknown`, preserving each backend's outcome exactly.
- **Additive `spi` API is a permanent public commitment (D5).** → Accepted:
  cross-crate sharing requires `pub`; the items are documented broker-author SPI,
  not facade-re-exported, and additive per API stability. The benefit (one source
  of truth) outweighs carrying three small documented items.
- **A stray hand-synced copy survives the lift.** → The Redis migration test
  (`worklane-redis/tests/migration.rs`) defines its own `BASELINE_VERSION = 1`; it
  must be pointed at the core `SCHEMA_VERSION` so no fifth copy is left.

## Migration Plan

None — behaviour-preserving, no schema or wire change. Rollback is a plain
revert; nothing persisted changes. Verification is the existing `worklane-test`
suite (all four backends, including live Postgres/Redis) plus the full Definition
of Done.
