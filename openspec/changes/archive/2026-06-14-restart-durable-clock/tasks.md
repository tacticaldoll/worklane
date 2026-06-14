## 1. Core: add `WallClock`

- [x] 1.1 Add a `WallClock` `Clock` impl in `worklane-core`'s clock module (now = `SystemTime` since `UNIX_EPOCH`, clamped to zero if before), with a doc note that it is restart-stable but not monotonic.
- [x] 1.2 Export `WallClock` from `worklane-core` (and re-export wherever `SystemClock` is surfaced).

## 2. SqliteBroker: default to `WallClock`

- [x] 2.1 Default `SqliteBroker::open` and `open_in_memory` to `WallClock` instead of `SystemClock`; keep `with_clock` as the override.
- [x] 2.2 Confirm the timed conformance harness still injects `ManualClock` via `with_clock` and the full conformance suite still passes.

## 3. Tests

- [x] 3.1 Restart-durability test (file-backed `SqliteBroker`): enqueue a job, drop the broker, reopen the same file with a fresh `WallClock`, and assert the job is still reservable.
- [x] 3.2 Persisted-retry-delay test: retry a job with a future delay, reopen the broker before the delay elapses, assert it stays hidden until the delay then becomes reservable (use an injected clock so the delay is deterministic across the two instances).

## 4. Definition of Done

- [x] 4.1 `cargo build`, `cargo test`, `cargo clippy --all-targets -- -D warnings`, and `cargo fmt --all --check` all pass.
- [x] 4.2 Verify the change with `openspec validate restart-durable-clock --strict`.
