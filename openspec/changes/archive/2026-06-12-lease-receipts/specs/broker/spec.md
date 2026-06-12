## MODIFIED Requirements

### Requirement: Reserve with visibility lease

`reserve(lane)` SHALL return at most one currently-visible job whose scheduled
time has arrived as a `Reservation` containing the `JobEnvelope` and an opaque
reservation receipt, and SHALL make that job invisible for a lease duration.
While leased, the job MUST NOT be returned by another `reserve`. If the lease
expires without a valid `ack`, `retry`, or `fail`, the job SHALL become visible
again (at-least-once delivery), and the expired receipt SHALL be rejected for
resolution.

#### Scenario: Reserve hides the job

- **WHEN** a visible job is reserved
- **THEN** an immediately following `reserve` on the same lane SHALL NOT return that job

#### Scenario: Empty lane

- **WHEN** `reserve` is called on a lane with no visible jobs
- **THEN** it SHALL return no job

#### Scenario: Lease expiry requeues

- **WHEN** a reserved job's lease expires before it is acked, retried, or failed
- **THEN** the job SHALL become visible again
- **AND** a subsequent `reserve` SHALL return it with a new receipt

#### Scenario: Expired receipt rejected

- **WHEN** a reserved job's lease expires before resolution
- **THEN** resolving the job with the expired receipt SHALL fail with a stale-reservation error
- **AND** the job SHALL remain available for a current reservation

#### Scenario: Superseded receipt rejected

- **WHEN** a reserved job's lease expires and the job is reserved again
- **THEN** resolving the job with the first receipt SHALL fail with a stale-reservation error
- **AND** resolving the job with the current receipt SHALL be allowed

### Requirement: Acknowledge

`ack(receipt)` SHALL permanently remove the job from the broker only when the
receipt is the current valid receipt for the job's active reservation.

#### Scenario: Ack removes the job

- **WHEN** a reserved job is acked with its current receipt
- **THEN** it SHALL never be returned by `reserve` again
- **AND** it SHALL NOT appear in the dead-letter store

#### Scenario: Stale ack rejected

- **WHEN** `ack` is called with an expired or superseded receipt
- **THEN** the broker SHALL reject the ack with a stale-reservation error
- **AND** it SHALL NOT remove the job due to that stale receipt

### Requirement: Retry

`retry(receipt, delay)` SHALL increment the job's `attempts`, schedule it to
become visible after `delay`, and end its current lease only when the receipt is
the current valid receipt for the job's active reservation.

#### Scenario: Retry increments attempts and delays visibility

- **WHEN** a reserved job is retried with its current receipt and a delay
- **THEN** its `attempts` SHALL increase by one
- **AND** it SHALL NOT be reservable until the delay has elapsed
- **AND** after the delay it SHALL be reservable again with a new receipt

#### Scenario: Stale retry rejected

- **WHEN** `retry` is called with an expired or superseded receipt
- **THEN** the broker SHALL reject the retry with a stale-reservation error
- **AND** it SHALL NOT increment attempts or change the job's schedule due to that stale receipt

### Requirement: Fail to dead-letter

`fail(receipt, error)` SHALL remove the job from the live store and place it in
the dead-letter store, retaining the error message, only when the receipt is the
current valid receipt for the job's active reservation.

#### Scenario: Fail dead-letters the job

- **WHEN** a reserved job is failed with its current receipt and an error
- **THEN** it SHALL appear in the dead-letter store with that error
- **AND** it SHALL NOT be returned by `reserve` again

#### Scenario: Stale fail rejected

- **WHEN** `fail` is called with an expired or superseded receipt
- **THEN** the broker SHALL reject the fail with a stale-reservation error
- **AND** it SHALL NOT dead-letter the job due to that stale receipt
