## Why

`Broker::ack`, `retry`, and `fail` currently resolve jobs by `JobId` alone. A
resolver whose visibility lease has expired, or whose job has been re-reserved
by another worker, can still mutate the job, which weakens the visibility-lease
contract and would be unsafe for concurrent workers or durable brokers.

This change protects the core loop before adding those scale-out features:
resolution must prove it belongs to the current reservation.

## What Changes

- **BREAKING**: replace JobId-only resolution with reservation-bound receipts.
- `reserve(lane)` returns a reservation containing the `JobEnvelope` and an
  opaque receipt bound to that reservation instance.
- `ack`, `retry`, and `fail` require a valid current receipt instead of only a
  `JobId`.
- Brokers reject expired or superseded receipts predictably instead of silently
  applying stale resolution.
- The worker threads the receipt from reserve through ack/retry/fail and treats
  stale-resolution errors as non-fatal loop events.
- Client/enqueue behavior is unchanged.
- Out of scope: concurrent workers, durable brokers, multiple lanes, and new
  polling behavior.

## Capabilities

### New Capabilities

<!-- None. This change modifies existing lifecycle capabilities. -->

### Modified Capabilities

- `broker`: reserve returns a reservation receipt, and ack/retry/fail require a
  valid current receipt.
- `worker`: the worker resolves jobs with the receipt returned by reserve and
  continues safely if resolution is rejected as stale.

## Impact

- `worklane-core`: add reservation/receipt types, update the `Broker` trait, and
  add a stale-reservation error variant.
- `worklane-memory`: store active reservation receipts and reject expired or
  superseded receipts.
- `worklane`: update worker internals to carry receipts through resolution.
- Tests: add deterministic ManualClock coverage for valid receipts, expired
  receipt rejection, superseded receipt rejection, and current receipt success.
