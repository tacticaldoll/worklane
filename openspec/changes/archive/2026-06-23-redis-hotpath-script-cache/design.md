## Context

`RedisBroker` issues every lifecycle operation as a Lua script via
`redis::Script`. The script accessors in `crates/worklane-redis/src/scripts.rs`
(`reserve()`, `ack()`, `retry()`, `defer()`, `fail()`, `extend()`, `requeue()`,
and the dead-letter helpers) each do `format!("{LUA_HELPERS}{BODY}")` —
re-concatenating the shared ~50-line `LUA_HELPERS` prologue with the body — and
each call site in `lib.rs` then wraps the result in `redis::Script::new(&body)`.
`Script::new` computes a SHA1 digest over the entire concatenated body in its
constructor. The wire path is already efficient (`invoke_async` sends
`EVALSHA`), so the only waste is the per-call string allocation + SHA1, repeated
on the hottest path in the system.

The sibling Postgres broker already faced this and solved it: `queries.rs`
builds the hot SQL statement strings once at connect into a `Queries` struct
that the broker reuses. This change applies the same precompute-once pattern to
Redis. `redis::Script` values are reusable across invocations and
`invoke_async` borrows `&self`, so a `Script` can be built once and shared.

Measured baseline (throwaway head-to-head harness, single-node Redis on
localhost): Redis reserve+ack drain ≈ 28,486 jobs/s, reserve-only ≈ 46,404
jobs/s — i.e. the rebuilt-and-rehashed script is constructed tens of thousands
of times per second under sustained load.

## Goals / Non-Goals

**Goals:**

- Build each `redis::Script` exactly once, at `RedisBroker` construction, and
  reuse the prebuilt value (and its cached digest) on every operation.
- Preserve behavior exactly: identical script bodies, Lua logic, key layout,
  `KEYS`/`ARGV` contracts, and error handling. The change must be invisible to
  every conformance scenario.
- Mirror the existing Postgres `Queries` precompute pattern so the two durable
  brokers stay structurally consistent.

**Non-Goals:**

- No change to Lua script content or semantics.
- No change to the `Broker` trait or any public API.
- Not addressing the other scan findings here (Postgres batch UNNEST fast path,
  idle-poll tax, conformance gaps, cross-broker dedup) — those are positioned in
  `BACKLOG.md` as separate future proposals, not implemented in this change.

## Decisions

**Decision: store prebuilt `redis::Script` values on `RedisBroker`, populated in
the constructor.** A `Scripts` struct (the Redis analogue of pg `Queries`) holds
one `redis::Script` per operation, built once from the same
`scripts::*` bodies. Each call site replaces `redis::Script::new(scripts::x())`
with a reference to the cached `self.scripts.x`.

- _Alternative — `OnceLock`/lazy static per script:_ rejected. The broker
  construction site is the established home for per-connection precompute (pg
  does exactly this), and instance fields keep the lifetime tied to the broker
  without global state or first-call latency jitter.
- _Alternative — leave as-is, rely on redis-rs internal caching:_ rejected.
  redis-rs caches the *hash* for the `EVALSHA`/`NOSCRIPT` fallback, but only
  after a `Script` exists — it does not avoid the per-call `format!` allocation
  or the SHA1 recomputation in `Script::new`, which is exactly the cost here.

**Decision: keep the `scripts::*` body functions as the single source of script
text.** The constructor calls them once to build the cached `Script`s, so the
script bodies remain defined in one place and the diff is confined to *where*
`Script::new` is called, not *what* it wraps.

## Risks / Trade-offs

- [Behavior drift from a subtle refactor error — e.g. caching the wrong body for
  an op] → The existing `worklane-test` Redis conformance suite exercises every
  lifecycle path; it must pass unchanged. That is the regression gate, recorded
  as a task.
- [Constructor cost / fallibility] → Building the `Script`s is cheap (the same
  work done once instead of per-call) and infallible (`Script::new` does not
  return a `Result`), so it cannot introduce a new connect-time failure mode.
- [Memory: holding N `Script`s for the broker's lifetime] → negligible; each is
  a small body string plus a 20-byte digest, and the count is fixed and small.

## Migration Plan

Pure in-process refactor — no schema, no data, no wire-format change. Deploys
with a normal release; rollback is reverting the commit. No migration steps and
no operator action.
