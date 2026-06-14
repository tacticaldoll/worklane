## 1. Core contract (`worklane-core`)

- [ ] 1.1 Add `Broker::extend(&self, receipt: ReservationReceipt) -> Result<()>` to the trait with a doc comment matching the broker spec (re-applies the broker lease; stale/expired/superseded rejected without mutation; does not change attempts).
- [ ] 1.2 Add an additive `lease: Duration` field to `Reservation` (keep `#[non_exhaustive]`); update `Reservation::new` callers/signature to carry the lease.
- [ ] 1.3 Write an ADR under `docs/adr/` recording the first post-durable-validation `Broker` trait change (the `extend` method) and its rationale.

## 2. Broker conformance suite (`worklane-test`)

- [ ] 2.1 Add a timed scenario: extend holds a reserved job past its original lease, then it remains resolvable with the same receipt.
- [ ] 2.2 Add a timed scenario: extend after lease expiry is rejected as stale, the job stays available, and attempts/schedule are unchanged.
- [ ] 2.3 Add a timed scenario: a superseded receipt cannot extend; the current reservation's lease is unchanged.
- [ ] 2.4 Add a scenario asserting a reservation conveys the broker's configured lease duration.
- [ ] 2.5 Wire the new scenarios into the `broker_contract_timed!` / `broker_contract_required!` macros as appropriate.

## 3. Broker implementations

- [ ] 3.1 `worklane-memory`: implement `extend` (re-apply lease via the existing `find_current_receipt` guard); populate `Reservation.lease` in `reserve`.
- [ ] 3.2 `worklane-sqlite`: implement `extend` as a guarded `UPDATE jobs SET leased_until = now + lease WHERE receipt = ? AND leased_until > now` (0 rows ⇒ stale); populate `Reservation.lease` in `reserve`.
- [ ] 3.3 Run the conformance suite against both brokers; confirm both pass with no further `Broker` trait change.

## 4. Worker: extract the per-job execution seam (structure)

- [ ] 4.1 Create `worklane/src/worker/` module; move `Worker` orchestration (`run`, `run_until_idle`, `process_next`, concurrency, shutdown, poll) into `worker/mod.rs`.
- [ ] 4.2 Extract the per-job lifecycle (`process`, `handle_failure`, `resolve`) into `worker/execution.rs` as a unit that takes a `Reservation` and returns a resolution `Result<()>`, behavior-preserving (no spec change).
- [ ] 4.3 Confirm existing worker tests still pass unchanged after the extraction.

## 5. Worker: bounded long-handler support (behavior)

- [ ] 5.1 Add `Worker::with_handler_timeout(Duration)` builder; store an `Option<Duration>` (default `None`).
- [ ] 5.2 In the per-job unit, when a timeout is configured, race the handler future against a `sleep(lease/2)` heartbeat tick and a `sleep(handler_timeout)` deadline.
- [ ] 5.3 On heartbeat tick: call `broker.extend(receipt)`; on stale rejection, stop extending and let the handler finish (eventual resolution stale-rejected and logged); do not crash or stall.
- [ ] 5.4 On timeout: stop maintaining the lease and resolve via the failure path (retry while attempts remain, else dead-letter with a timeout error).
- [ ] 5.5 Ensure the default path (no timeout) neither heartbeats nor times out — unchanged behavior.

## 6. Worker tests

- [ ] 6.1 Test: a slow-but-finishing handler under a timeout is heartbeated, runs exactly once, and is acked (not redelivered).
- [ ] 6.2 Test: a handler that exceeds its timeout is retried while attempts remain, then dead-lettered with a timeout error; the worker keeps processing.
- [ ] 6.3 Test: with no timeout configured, behavior matches today (lease expiry may redeliver; no extend calls).
- [ ] 6.4 Test: a heartbeat rejected as stale is tolerated (no crash/stall; resolution logged).

## 7. Definition of Done

- [ ] 7.1 `cargo build`, `cargo test`, `cargo clippy --all-targets -- -D warnings`, and `cargo fmt --all --check` all pass.
- [ ] 7.2 Update the README/example if the worker builder surface is shown there; note `with_handler_timeout` where relevant.
- [ ] 7.3 Verify the change with `openspec validate bounded-long-handler-support --strict`.
