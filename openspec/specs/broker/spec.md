# Broker Specification

## Purpose

Defines the backend-agnostic `Broker` contract: how jobs are enqueued, reserved
under a visibility lease, and resolved (ack / retry / fail), plus dead-lettering.
The broker operates only on opaque envelopes.

## Requirements

### Requirement: Enqueue

The broker SHALL accept a `NewJob` and store it as a visible `JobEnvelope` with a
freshly assigned `JobId` and `attempts = 0`, returning the `JobId`.

#### Scenario: Enqueue makes a job reservable

- **WHEN** a job is enqueued to a lane
- **THEN** a `reserve` on that lane SHALL be able to return it

### Requirement: Reserve with visibility lease

`reserve(lane)` SHALL return at most one currently-visible job whose scheduled
time has arrived, and SHALL make that job invisible for a lease duration. While
leased, the job MUST NOT be returned by another `reserve`. If the lease expires
without `ack`, `retry`, or `fail`, the job SHALL become visible again
(at-least-once delivery).

#### Scenario: Reserve hides the job

- **WHEN** a visible job is reserved
- **THEN** an immediately following `reserve` on the same lane SHALL NOT return that job

#### Scenario: Empty lane

- **WHEN** `reserve` is called on a lane with no visible jobs
- **THEN** it SHALL return no job

#### Scenario: Lease expiry requeues

- **WHEN** a reserved job's lease expires before it is acked, retried, or failed
- **THEN** the job SHALL become visible again
- **AND** a subsequent `reserve` SHALL return it

### Requirement: Acknowledge

`ack(job_id)` SHALL permanently remove the job from the broker.

#### Scenario: Ack removes the job

- **WHEN** a reserved job is acked
- **THEN** it SHALL never be returned by `reserve` again
- **AND** it SHALL NOT appear in the dead-letter store

### Requirement: Retry

`retry(job_id, delay)` SHALL increment the job's `attempts`, schedule it to
become visible after `delay`, and end its current lease.

#### Scenario: Retry increments attempts and delays visibility

- **WHEN** a reserved job is retried with a delay
- **THEN** its `attempts` SHALL increase by one
- **AND** it SHALL NOT be reservable until the delay has elapsed
- **AND** after the delay it SHALL be reservable again

### Requirement: Fail to dead-letter

`fail(job_id, error)` SHALL remove the job from the live store and place it in the
dead-letter store, retaining the error message.

#### Scenario: Fail dead-letters the job

- **WHEN** a reserved job is failed with an error
- **THEN** it SHALL appear in the dead-letter store with that error
- **AND** it SHALL NOT be returned by `reserve` again

### Requirement: Backend-agnostic payloads

The broker SHALL operate only on opaque envelopes and MUST NOT depend on Rust
handler types or inspect payload contents.

#### Scenario: Opaque handling

- **WHEN** any broker operation processes a job
- **THEN** it SHALL use only envelope fields (`id`, `kind`, `payload` bytes,
  `attempts`, `max_attempts`)
- **AND** it MUST NOT deserialize the payload
