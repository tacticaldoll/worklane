## Context

The current broker contract reserves jobs with a visibility lease but resolves
them by `JobId` alone. That means an old resolver can still call `ack`, `retry`,
or `fail` after its lease expires, even if the job has become visible again or
has already been re-reserved by another worker.

The archived `core-loop` design accepted this as a v0.1 trade-off because the
worker is single and sequential. The risk belongs in the core contract, though:
durable brokers and concurrent workers need stale resolvers to be rejected by
the broker, not merely avoided by the current worker shape.

## Goals / Non-Goals

**Goals:**

- Bind every reservation to an opaque receipt issued by the broker.
- Require that receipt for `ack`, `retry`, and `fail`.
- Reject stale resolution when a receipt is expired or superseded.
- Keep the broker backend-agnostic and payload-opaque.
- Keep the worker sequential while threading receipts through its existing
  resolve paths.

**Non-Goals:**

- Add concurrent workers, worker task pools, or polling behavior.
- Add durable brokers or backend-specific receipt semantics.
- Add lane partitioning.
- Change client enqueue behavior.

## Decisions

### D1. Use opaque reservation receipts

`reserve(lane)` returns `Reservation { envelope, receipt }`. The receipt is an
opaque token issued by the broker for that reservation instance and required for
resolution.

- *Alternative — JobId-only resolution:* keeps the current API but permits stale
  mutation after lease expiry. Rejected.
- *Alternative — public lease epoch:* simpler for an in-memory broker, but leaks
  one implementation strategy into the public contract. Rejected for the trait
  surface.
- *Consequence:* broker implementations can encode a UUID, backend receipt
  handle, fencing token, or job lookup key inside the receipt without changing
  worker behavior.

### D2. Resolve by receipt only

The broker trait changes from `ack(job_id)`, `retry(job_id, delay)`, and
`fail(job_id, error)` to receipt-based resolution. The worker keeps the reserved
envelope for logging and handler context, but resolution authority comes from
the receipt.

- *Alternative — pass both `JobId` and receipt:* helpful for diagnostics, but it
  risks implying that the caller-selected `JobId` participates in authorization.
  Rejected for the public trait.
- *Consequence:* a broker can return `Error::StaleReservation` when the receipt
  no longer matches a live current reservation.

### D3. Expiry and supersession both make a receipt stale

A receipt is valid only while it is the active receipt for the job and its lease
has not elapsed. If the lease expires, the old receipt is stale. If the job is
reserved again, the broker issues a new receipt and the old receipt remains
stale even if the caller later tries to resolve it.

- *Alternative — allow old receipt until a new reservation happens:* permits
  late mutation after the lease elapsed. Rejected.
- *Consequence:* after lease expiry the job may be delivered again, preserving
  at-least-once behavior while preventing stale mutation.

### D4. Worker treats stale resolution as non-fatal loop progress

If a handler finishes after the reservation is no longer current, resolution
will fail with `Error::StaleReservation`. The worker logs that result and
continues processing instead of panicking or retrying resolution. The job has
already become available again or has been reserved by another worker.

- *Alternative — return the error from `process_next`:* accurate but makes stale
  resolution look like worker failure. Rejected.
- *Consequence:* a future concurrent worker can safely continue even when one
  task loses its lease before resolution.

### D5. In-memory broker stores active receipt state per job

The in-memory broker stores the active receipt and lease expiry alongside each
live job. `reserve` reclaims expired leases before selecting visible jobs,
issues a new receipt, and records it as active. `ack`, `retry`, and `fail`
locate the matching active receipt, verify the lease is still current, then
apply the state transition.

- *Alternative — validate by time only:* catches expiry but not supersession.
  Rejected.
- *Consequence:* ManualClock tests can deterministically exercise valid,
  expired, and superseded resolution.

## Risks / Trade-offs

- **Breaking broker trait change** -> Acceptable at `0.0.x`; document in the
  proposal and keep client/enqueue unchanged.
- **Receipt type may look forgeable in tests** -> Broker validation still
  requires an active stored receipt; callers cannot resolve with arbitrary
  tokens.
- **Receipt-only lookup may be inefficient for some brokers** -> The opaque
  token can encode backend-specific lookup data, and in-memory performance is
  not a v0.1 concern.
- **Stale resolution could hide slow handlers** -> Log stale-resolution events
  with job id and kind from the reserved envelope so operators can see lease
  timing problems later.

## Migration Plan

1. Add `Reservation` and `ReservationReceipt` to `worklane-core`.
2. Change the `Broker` trait to return `Option<Reservation>` from `reserve` and
   accept `ReservationReceipt` for `ack`, `retry`, and `fail`.
3. Update the in-memory broker to issue, store, and validate receipts.
4. Update worker internals to keep the receipt with the envelope and handle
   `Error::StaleReservation` as a logged, non-fatal resolution outcome.
5. Update integration tests and examples for the new API.

Rollback is a normal git revert before sync/archive; no data migration exists
because there are no durable brokers yet.

## Open Questions

- Exact public constructor/accessor shape for `ReservationReceipt`; lean toward
  a small opaque newtype with broker-facing construction and standard debug/
  clone/equality traits.
- Exact stale error name; lean `Error::StaleReservation` because it covers both
  expired and superseded receipts.
