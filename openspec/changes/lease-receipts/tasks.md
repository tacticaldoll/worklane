## 1. Core contract types

- [ ] 1.1 Add `Reservation` and `ReservationReceipt` types to `worklane-core` and re-export them from `worklane-core` and `worklane`
- [ ] 1.2 Add `Error::StaleReservation` with wording that covers expired and superseded receipts
- [ ] 1.3 Update the `Broker` trait so `reserve` returns `Option<Reservation>` and `ack`, `retry`, and `fail` accept `ReservationReceipt`

## 2. In-memory broker behavior

- [ ] 2.1 Store the active receipt and lease expiry for each reserved live job
- [ ] 2.2 Issue a fresh opaque receipt on each successful `reserve`
- [ ] 2.3 Reject `ack`, `retry`, and `fail` with `Error::StaleReservation` when the receipt is unknown, expired, or no longer active
- [ ] 2.4 Preserve current valid-resolution behavior: ack removes, retry increments attempts and schedules visibility, fail dead-letters

## 3. Worker integration

- [ ] 3.1 Thread the reservation receipt through `process_next`, dispatch, success ack, retry, fail, unknown-kind failure, and serialization failure paths
- [ ] 3.2 Treat `Error::StaleReservation` from resolution as a logged non-fatal outcome and continue the worker loop
- [ ] 3.3 Keep handler context and logs based on the reserved envelope's job id, attempts, max attempts, and kind

## 4. Tests and examples

- [ ] 4.1 Update existing integration tests and the basic example for the receipt-based `Broker` API
- [ ] 4.2 Add tests showing valid receipt ack/retry/fail preserve current behavior
- [ ] 4.3 Add ManualClock tests showing expired receipt ack/retry/fail are rejected without mutating the job
- [ ] 4.4 Add a ManualClock test showing a superseded receipt is rejected after re-reservation and the new receipt works
- [ ] 4.5 Add or update a worker test showing stale resolution is logged/handled as non-fatal and subsequent jobs can still be processed
- [ ] 4.6 Confirm the Definition of Done: `cargo build`, `cargo test`, `cargo clippy --all-targets -- -D warnings`, and `cargo fmt --all --check` all pass
