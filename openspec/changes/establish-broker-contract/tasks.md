## 1. Lift the time seam into core

- [x] 1.1 Move the `Clock` trait and `SystemClock` from `worklane-memory` into `worklane-core` (keep `now()` only; no async sleep), and re-export from core's `lib.rs`
- [x] 1.2 Update `worklane-memory` to import `Clock`/`SystemClock` from `worklane-core`; keep `InMemoryBroker::with_clock(Arc<dyn Clock>)`
- [x] 1.3 `cargo build` to confirm the workspace compiles after the move

## 2. The worklane-test crate

- [x] 2.1 Add a new `worklane-test` crate to the workspace (publishable; depends on `worklane-core` only)
- [x] 2.2 Add `ManualClock` (test-only, implements core's `Clock`) to `worklane-test`
- [x] 2.3 Define `BrokerContractHarness` (`type Broker: Broker`, `fresh_broker`, `dead_letters -> Option<...>`) and a timed harness variant adding `advance_time`
- [x] 2.4 Implement the shared async contract test functions, one per broker-spec scenario, observing brokers only through the `Broker` trait + harness adapter
- [x] 2.5 Provide `broker_contract_required!` and `broker_contract_timed!` macros that expand to one `#[tokio::test]` per scenario over a harness; capability-gated assertions emit a visible skip notice when an `Option` capability is absent

## 3. First-version scenarios (8)

- [x] 3.1 Required: enqueue then reserve on the same lane returns the job
- [x] 3.2 Required: reserve does not return a job from a different lane (isolation)
- [x] 3.3 Required: reserving twice on a lane does not hand out the same job twice
- [x] 3.4 Required: ack with the current receipt removes the job (no longer reservable)
- [x] 3.5 Required: retry with `delay = 0` increments attempts and the job is immediately reservable
- [x] 3.6 Required: fail with the current receipt removes the live job; if the harness exposes dead-letter inspection, assert the dead-letter content (else visibly skip)
- [x] 3.7 Timed: retry with `delay > 0` hides the job before the delay and exposes it after
- [x] 3.8 Timed: expired receipt is rejected for ack/retry/fail with no mutation; superseded receipt is rejected while the current receipt resolves

## 4. Adopt the suite in worklane-memory and re-home tests

- [x] 4.1 Add `worklane-test` as a dev-dependency of `worklane-memory`; build an `InMemoryBroker` harness (required + timed via `ManualClock`)
- [x] 4.2 Invoke `broker_contract_required!` and `broker_contract_timed!` from `worklane-memory`'s tests
- [x] 4.3 Remove the pure-broker scenarios now covered by the suite from `crates/worklane/tests` (lease/receipt/lane-isolation cases); keep Client/Worker integration tests in the facade

## 5. Definition of Done

- [x] 5.1 `cargo build` passes
- [x] 5.2 `cargo test` passes (suite runs against `InMemoryBroker`; facade integration tests intact)
- [x] 5.3 `cargo clippy --all-targets -- -D warnings` is clean
- [x] 5.4 `cargo fmt --all --check` passes
- [x] 5.5 `Broker` trait is unchanged; `worklane-test` carries no runtime (non-dev) dependents
