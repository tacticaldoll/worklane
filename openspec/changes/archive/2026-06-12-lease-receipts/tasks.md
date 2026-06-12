## 1. Core contract types

- [x] 1.1 Add `Reservation` and `ReservationReceipt` types to `worklane-core` and re-export them from `worklane-core` and `worklane`
- [x] 1.2 Add `Error::StaleReservation` with wording that covers expired and superseded receipts
- [x] 1.3 Update the `Broker` trait so `reserve` returns `Option<Reservation>` and `ack`, `retry`, and `fail` accept `ReservationReceipt`

## 2. In-memory broker behavior

- [x] 2.1 Store the active receipt and lease expiry for each reserved live job
- [x] 2.2 Issue a fresh opaque receipt on each successful `reserve`
- [x] 2.3 Reject `ack`, `retry`, and `fail` with `Error::StaleReservation` when the receipt is unknown, expired, or no longer active
- [x] 2.4 Preserve current valid-resolution behavior: ack removes, retry increments attempts and schedules visibility, fail dead-letters

## 3. Worker integration

- [x] 3.1 Thread the reservation receipt through `process_next`, dispatch, success ack, retry, fail, unknown-kind failure, and serialization failure paths
- [x] 3.2 Treat `Error::StaleReservation` from resolution as a logged non-fatal outcome and continue the worker loop
- [x] 3.3 Keep handler context and logs based on the reserved envelope's job id, attempts, max attempts, and kind

## 4. Tests and examples

- [x] 4.1 Update existing integration tests and the basic example for the receipt-based `Broker` API
- [x] 4.2 Add tests showing valid receipt ack/retry/fail preserve current behavior
- [x] 4.3 Add ManualClock tests showing expired receipt ack/retry/fail are rejected without mutating the job
- [x] 4.4 Add a ManualClock test showing a superseded receipt is rejected after re-reservation and the new receipt works
- [x] 4.5 Add or update a worker test showing stale resolution is logged/handled as non-fatal and subsequent jobs can still be processed
- [x] 4.6 Confirm the Definition of Done: `cargo build`, `cargo test`, `cargo clippy --all-targets -- -D warnings`, and `cargo fmt --all --check` all pass
