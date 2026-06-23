## Why

Every `RedisBroker` lifecycle operation rebuilds its Lua script from scratch
on each call: the script accessors (`scripts::reserve()` and siblings)
`format!`-concatenate the ~50-line `LUA_HELPERS` block with each body, then
`redis::Script::new(&body)` runs SHA1 over the whole concatenated body in its
constructor — before the call even reaches the wire. This is pure redundant
per-call CPU and allocation on the throughput-critical consume loop
(reserve / ack / retry / defer / fail / extend / requeue + dead-letter ops,
~13 call sites in `crates/worklane-redis/src/lib.rs`). The Redis consume hot
path was measured at 28,486 jobs/s (reserve+ack drain), so this overhead is
paid thousands of times per second under load.

Postgres already solved the analogous problem: `worklane-postgres` precomputes
its hot statements once at connect in the `Queries` struct
(`queries.rs`). This change is the Redis analogue of that pattern.

## What Changes

- Precompute each `redis::Script` once at `RedisBroker` construction (storing
  the prebuilt, already-hashed `Script` values on the broker) and reuse them on
  every operation, instead of rebuilding the script string and re-hashing SHA1
  per call.
- No change to script bodies, Lua logic, key layout, or any observable broker
  behavior — this is an internal performance refactor.
- Record the broader perf/risk scan findings in `BACKLOG.md` as positioned
  future ideas (P2, P3, R1, R2, R3 — see tasks), and correct two stale
  duplication counts the backlog already records.

## Capabilities

### New Capabilities

- _(none)_ — no new lifecycle behavior is introduced.

### Modified Capabilities

- _(none)_ — this is a behavior-preserving internal refactor. No
  `openspec/specs/` requirement changes: script bodies and observable
  contract are unchanged, and the existing `broker` conformance suite for
  Redis is the regression guard.

## Impact

- **Code**: `crates/worklane-redis/src/lib.rs` (call sites) and
  `crates/worklane-redis/src/scripts.rs` (script construction); the
  `RedisBroker` struct gains prebuilt-`Script` fields, populated in its
  constructor.
- **APIs**: none. No public signature changes; `Broker` contract unchanged.
- **Dependencies**: none. Uses the existing `redis` crate `Script` API.
- **Verification**: the existing `worklane-test` Redis conformance suite must
  still pass unchanged; a before/after micro-measurement documents the win.
- **Docs**: `BACKLOG.md` (✓-shipped entry for this change, plus positioned
  future ideas and corrected counts).
