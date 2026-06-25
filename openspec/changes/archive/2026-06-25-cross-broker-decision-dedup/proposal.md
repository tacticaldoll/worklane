## Why

Four backends must honour the *same* observable lifecycle semantics, but several
shared decisions are copy-pasted per backend rather than defined once. The
dead-letter sweep cap (`128`) exists in four places, the classify
integer-to-`JobState` mapping in three, and the schema/baseline-version policy in
each SQL backend. Nothing forces these copies to agree: editing one and missing
another silently diverges cross-backend behaviour — exactly the failure mode the
conformance contract exists to prevent. Lifting each shared *decision* into
`worklane-core` (the model already set by `spi::reject_chars` and
`redact_credentials`) removes the drift surface while leaving each backend only
its dialect-specific statements.

## What Changes

- Lift the dead-letter sweep cap into a single `worklane-core` constant; the
  memory, SQLite, and Postgres brokers reference it, and the Redis Lua script
  receives it (no second source of truth — see design for the Lua decision).
- Lift the classify `i64` / `Option<i64>` → `JobState` mapping into one
  `worklane-core` function the SQLite, Postgres, and Redis brokers call, instead
  of three hand-rolled integer matches.
- Lift the dead-letter prune/retention computation (SQLite ↔ Postgres
  near-verbatim) into a shared core helper for the date/bound math, leaving each
  backend its own SQL.
- Lift the schema/baseline `SCHEMA_VERSION` **const and the match-vs-reject
  decision** (the pre-1.0 "we do not migrate; reject a mismatched version" policy)
  into core so the SQLite and Postgres schema guards — and the Redis baseline check
  — share one version and one decision. Each backend KEEPS its own dialect-specific
  remediation message (e.g. SQLite "drop and recreate the database" vs. Redis
  "flush the namespace and re-enqueue"), because that text is operator-visible and
  one Redis test pins its wording.
- Lift the dialect-independent dead-letter retention computation (the age-cutoff
  instant and the count-keep bound) — three verbatim copies across SQLite,
  Postgres, and the Redis reserve path — onto the core retention surface; each
  backend keeps its own `DELETE`/sweep.
- Behaviour-preserving for all observable lifecycle semantics, error text, and
  storage. The **only** API change is **additive**: new broker-author items in
  `worklane_core::spi` (a classify helper, the sweep-cap const, the schema-version
  const + decision helper) and one method on the core retention surface. No
  `Broker` trait, on-disk schema, or wire-format change; nothing is removed or
  renamed. The `worklane-test` conformance suite is the regression gate.

## Capabilities

### New Capabilities
<!-- None. This change introduces no new observable capability; it relocates
     shared internal decisions behind the existing contract. -->

### Modified Capabilities
<!-- None. No requirement changes. The new spi items fall under the EXISTING
     "Broker author SPI" requirement in openspec/specs/broker-extensibility, whose
     normative text already governs "shared backend decisions such as …" — an open
     list these extend without changing the requirement (argued in design.md).
     Observable lifecycle semantics, error text, and storage are unchanged across
     every backend, so no delta spec (precedent: redis-hotpath-script-cache,
     postgres-enqueue-batch-unnest, both archived --skip-specs — though neither
     grew spi, hence the explicit broker-extensibility argument here). The
     conformance suite proves behaviour is preserved. -->

## Impact

- **Code:** `worklane-core` gains a few shared broker-author items in
  `worklane_core::spi` (sweep-cap const, classify mapping, schema-version const +
  decision) and one retention method. `worklane-memory`, `worklane-sqlite`,
  `worklane-postgres`, and `worklane-redis` drop their private copies and call the
  core surface; `worklane-redis/src/scripts.rs` no longer hard-codes
  `sweep_cap = 128` (injected via the `RESERVE` script's `ARGV`).
- **API:** additive only — new `pub` items in `worklane_core::spi` (cross-crate
  sharing requires `pub`; they are broker-author surface, documented as such, and
  NOT re-exported from the `worklane` facade, per the broker-extensibility spec).
  Non-breaking; nothing removed or renamed.
- **Schema / wire format:** unchanged.
- **Specs:** none (no requirement change; see the broker-extensibility argument in
  design.md). Archived `--skip-specs`.
- **Tests:** existing `worklane-test` lifecycle/dead-letter batteries are the
  regression gate, PLUS a new conformance scenario that pins the dead-letter sweep
  *bound* (none exists today, so D2's ARGV change would otherwise be untested).
- **Out of scope (deferred to BACKLOG):** `DEFAULT_LEASE` (`30s`) is also
  duplicated `pub` in all four backends, but it is a user-facing constructor
  default whose lift requires a re-export/deprecation decision — a separate
  API-compatibility change, not folded in here.
