# Broker Specification

## Purpose

Defines the backend-agnostic `Broker` contract: how jobs are enqueued, reserved
under a visibility lease, and resolved (ack / retry / fail), plus dead-lettering.
The broker operates only on opaque envelopes.

## Requirements

### Requirement: Enqueue

The broker SHALL accept a `NewJob` and store it as a visible `JobEnvelope` with a
freshly assigned `JobId`, the lane carried by the `NewJob`, and `attempts = 0`,
returning the `JobId`.

#### Scenario: Enqueue makes a job reservable

- **WHEN** a job is enqueued to a lane
- **THEN** a `reserve` on that lane SHALL be able to return it

#### Scenario: Stored envelope retains its lane

- **WHEN** a job is enqueued to lane `"critical"`
- **THEN** the stored envelope SHALL carry lane `"critical"`
- **AND** that lane SHALL be preserved through reservation and dead-lettering

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

### Requirement: Lane-scoped reserve

`reserve(lane)` SHALL only return jobs that were enqueued to that lane. A job on
one lane MUST NOT be returned by a `reserve` on a different lane. Lanes are
arbitrary strings with no registration; a lane that no worker reserves SHALL
retain its jobs indefinitely, which is a deliberate operator responsibility.

#### Scenario: Reserve returns only same-lane jobs

- **WHEN** a job is enqueued to lane `"critical"` and `reserve("critical")` is called
- **THEN** the reservation SHALL return that job

#### Scenario: Other lanes cannot steal

- **WHEN** a job is enqueued to lane `"critical"` and `reserve("default")` is called
- **THEN** `reserve("default")` SHALL return no job
- **AND** the job SHALL remain reservable on lane `"critical"`

#### Scenario: Lanes are isolated

- **WHEN** one job is enqueued to lane `"a"` and another to lane `"b"`
- **THEN** `reserve("a")` SHALL return only the lane `"a"` job
- **AND** `reserve("b")` SHALL return only the lane `"b"` job

#### Scenario: Unworked lane retains jobs

- **WHEN** a job is enqueued to a lane that no worker ever reserves
- **THEN** the job SHALL remain enqueued indefinitely and SHALL NOT be returned
  by a `reserve` on any other lane

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

### Requirement: Backend-agnostic payloads

The broker SHALL operate only on opaque envelopes and MUST NOT depend on Rust
handler types or inspect payload contents.

#### Scenario: Opaque handling

- **WHEN** any broker operation processes a job
- **THEN** it SHALL use only envelope fields (`id`, `lane`, `kind`, `payload`
  bytes, `attempts`, `max_attempts`)
- **AND** it MUST NOT deserialize the payload

### Requirement: Injectable time source

A broker SHALL derive all time-based decisions (job visibility, lease expiry, and
retry scheduling) from an injectable clock rather than reading wall-clock time
directly, so that its lease and visibility semantics are deterministic and
portable across deployments and verifiable by the shared contract suite.

#### Scenario: Visibility advances by injected time

- **WHEN** a broker is constructed with a clock and that clock is advanced past a
  reserved job's lease without an intervening ack, retry, or fail
- **THEN** the job SHALL become reservable again
- **AND** this transition SHALL depend on the injected clock, not on wall-clock time

#### Scenario: Scheduled visibility tracks injected time

- **WHEN** a job is retried with a delay and the injected clock is advanced by that delay
- **THEN** the job SHALL become reservable again, independent of wall-clock time
