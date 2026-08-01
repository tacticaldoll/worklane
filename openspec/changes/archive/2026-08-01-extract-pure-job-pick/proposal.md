## Why

`InMemoryBroker::reserve`'s best-visible-job scan (priority desc, then earliest
`available_at`) is a pure decision over plain job data, but it is currently written
inline inside the `Mutex`-holding, delivery-bound-sweeping loop, with no way to
unit-test the selection rule in isolation. Unlike the fan-in watcher and the circuit
breaker, this decision is stateless per call (no history carried between
reservations) and has no second real implementation to justify a published crate —
the SQL and Redis backends express the equivalent ordering in SQL/Lua, not Rust, so
there is no cross-backend Rust duplication a shared crate would remove. This does not
clear the bar for a new family member; it is a local testability improvement.

## What Changes

- Extract the nested best-index scan (lines computing `best_idx`) into a private,
  free function `fn pick_best(jobs: &[StoredJob], lane: &Lane, now: Duration) ->
  Option<usize>` in the same module.
- Add direct unit tests for `pick_best` covering: lane filtering, lease-visibility
  filtering, `available_at` visibility filtering, priority ordering, and the
  available-at tie-break.
- No behavior change: `reserve` calls the extracted function instead of running the
  scan inline; the delivery-bound sweep, dead-lettering, and lease/receipt
  assignment all stay exactly where they are.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

None. This is a private, in-module refactor with no observable behavior change —
`InMemoryBroker`'s public `Broker` trait implementation is untouched.

## Impact

- Affected code: `crates/worklane-memory/src/lib.rs` only.
- No dependency, API, or behavior changes.
- No public capability spec change.
