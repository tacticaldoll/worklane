# Worker Specification

## Purpose

Defines how a worker registers handlers by job kind and runs the reserve →
dispatch → run → resolve loop, including retry-until-max, dead-lettering, and
unknown-kind handling.

## Requirements

### Requirement: Handler registration by kind

A worker SHALL register handlers keyed by job `KIND`. Registering two handlers
for the same kind SHALL be rejected.

#### Scenario: Register a handler

- **WHEN** a handler for kind `"send_email"` is registered
- **THEN** jobs of kind `"send_email"` SHALL be dispatched to it

#### Scenario: Duplicate kind

- **WHEN** two handlers are registered for the same kind
- **THEN** the worker SHALL reject the duplicate registration with an error

### Requirement: Sequential processing loop

The worker SHALL process one job at a time: reserve a job and its receipt,
dispatch it, run the handler to completion, and resolve it with that receipt
(ack / retry / fail) before reserving the next job.

#### Scenario: One job at a time

- **WHEN** the worker is running
- **THEN** it SHALL NOT reserve a new job until the current job has been acked,
  retried, failed, or rejected as a stale reservation

### Requirement: Success acknowledges

The worker SHALL ack a job whose handler returns successfully by passing the
reservation receipt returned by `reserve`.

#### Scenario: Successful handler

- **WHEN** a handler returns success
- **THEN** the worker SHALL ack the job with the current receipt
- **AND** the job SHALL NOT be retried or dead-lettered

#### Scenario: Ack rejected after lease expiry

- **WHEN** a handler returns success after its reservation receipt is no longer current
- **THEN** the worker SHALL log the stale-resolution result
- **AND** the worker SHALL continue processing subsequent jobs

### Requirement: Failure retries until max attempts

The worker SHALL retry a failed job, with a delay from the retry policy, while it
has remaining attempts, and SHALL fail it to the dead-letter store with the
handler error once no attempts remain. Retry and fail resolution SHALL use the
reservation receipt returned by `reserve`.

#### Scenario: Retry below max attempts

- **WHEN** a handler errors and `attempts + 1 < max_attempts`
- **THEN** the worker SHALL retry the job with the current receipt and the policy-computed delay

#### Scenario: Dead-letter at max attempts

- **WHEN** a handler errors and `attempts + 1 >= max_attempts`
- **THEN** the worker SHALL fail the job to the dead-letter store with the current receipt and the handler error

#### Scenario: Retry or fail rejected after lease expiry

- **WHEN** a handler errors after its reservation receipt is no longer current
- **THEN** the worker SHALL log the stale-resolution result
- **AND** the worker SHALL continue processing subsequent jobs

### Requirement: Unknown kind handling

When a reserved job has a kind with no registered handler, the worker SHALL fail
it predictably using the reservation receipt and MUST NOT panic or stall the
loop.

#### Scenario: Unknown kind

- **WHEN** a reserved job's kind has no registered handler
- **THEN** the worker SHALL fail the job to the dead-letter store with the current
  receipt and an unknown-kind error
- **AND** the worker SHALL continue processing subsequent jobs

#### Scenario: Unknown-kind fail rejected after lease expiry

- **WHEN** an unknown-kind job's reservation receipt is no longer current before failure resolution
- **THEN** the worker SHALL log the stale-resolution result
- **AND** the worker SHALL continue processing subsequent jobs

### Requirement: Exponential retry backoff

The retry policy SHALL compute the delay as `min(base * factor^attempts, cap)`.

#### Scenario: Backoff growth

- **WHEN** `attempts` increases
- **THEN** the computed retry delay SHALL increase exponentially until it is
  capped at `cap`
