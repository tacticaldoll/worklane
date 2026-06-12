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

The worker SHALL process one job at a time: reserve a job, dispatch it, run the
handler to completion, and resolve it (ack / retry / fail) before reserving the
next job.

#### Scenario: One job at a time

- **WHEN** the worker is running
- **THEN** it SHALL NOT reserve a new job until the current job has been acked,
  retried, or failed

### Requirement: Success acknowledges

The worker SHALL ack a job whose handler returns successfully.

#### Scenario: Successful handler

- **WHEN** a handler returns success
- **THEN** the worker SHALL ack the job
- **AND** the job SHALL NOT be retried or dead-lettered

### Requirement: Failure retries until max attempts

The worker SHALL retry a failed job, with a delay from the retry policy, while it
has remaining attempts, and SHALL fail it to the dead-letter store with the
handler error once no attempts remain.

#### Scenario: Retry below max attempts

- **WHEN** a handler errors and `attempts + 1 < max_attempts`
- **THEN** the worker SHALL retry the job with the policy-computed delay

#### Scenario: Dead-letter at max attempts

- **WHEN** a handler errors and `attempts + 1 >= max_attempts`
- **THEN** the worker SHALL fail the job to the dead-letter store with the handler error

### Requirement: Unknown kind handling

When a reserved job has a kind with no registered handler, the worker SHALL fail
it predictably and MUST NOT panic or stall the loop.

#### Scenario: Unknown kind

- **WHEN** a reserved job's kind has no registered handler
- **THEN** the worker SHALL fail the job to the dead-letter store with an
  unknown-kind error
- **AND** the worker SHALL continue processing subsequent jobs

### Requirement: Exponential retry backoff

The retry policy SHALL compute the delay as `min(base * factor^attempts, cap)`.

#### Scenario: Backoff growth

- **WHEN** `attempts` increases
- **THEN** the computed retry delay SHALL increase exponentially until it is
  capped at `cap`
