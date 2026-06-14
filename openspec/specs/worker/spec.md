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

### Requirement: Bounded concurrent processing

The worker SHALL process up to a configured maximum number of jobs — its
**concurrency** — at once. Each in-flight job SHALL be reserved with its
receipt, dispatched, run to completion, and resolved (ack / retry / fail) with
that receipt. The worker SHALL NOT exceed its configured concurrency in flight.
Concurrency SHALL default to 1, which is strictly sequential: no new job is
reserved until the current job has been acked, retried, failed, or rejected as a
stale reservation.

#### Scenario: Default concurrency is sequential

- **WHEN** concurrency is 1 (the default) and the worker is running
- **THEN** it SHALL NOT reserve a new job until the current job has been acked,
  retried, failed, or rejected as a stale reservation

#### Scenario: Concurrency bounds jobs in flight

- **WHEN** concurrency is N and more than N jobs are available on the lane
- **THEN** the worker SHALL run at most N handlers at the same time
- **AND** each job SHALL be resolved with the receipt from its own reservation

#### Scenario: Handler exceeding its lease may be redelivered

- **WHEN** a handler runs longer than its reservation lease while the worker has
  free capacity
- **THEN** the job MAY be reserved again and run a second time (at-least-once)
- **AND** the original reservation's later resolution SHALL be rejected as a
  stale reservation and logged, without crashing or stalling the worker

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

### Requirement: Long-running poll loop

The worker SHALL provide a `run` operation that processes jobs until a shutdown
signal: it SHALL process every currently available job on its lane, and when no
job is available it SHALL wait a configurable poll interval before checking
again. This lets a worker pick up jobs that become available later (for example
a pending retry whose delay has elapsed), which `run_until_idle` does not.
Processing honours the worker's configured concurrency: up to N jobs run at once
(N defaults to 1, which is strictly one job at a time).

#### Scenario: Processes available jobs then waits

- **WHEN** `run` is executing and jobs are available on the lane
- **THEN** it SHALL process them, up to its configured concurrency at a time,
  until none remain
- **AND** it SHALL then wait the poll interval before checking the lane again

#### Scenario: Picks up work that appears while idle

- **WHEN** the worker is idle in `run` and a job then becomes available on its lane
- **THEN** the worker SHALL process that job on a subsequent poll

### Requirement: Cooperative shutdown

`run` SHALL accept a shutdown signal and stop cleanly. The signal SHALL be
honoured only between jobs: all in-flight jobs (up to the configured
concurrency) SHALL run to completion and be resolved (ack, retry, or fail)
before `run` returns. A worker that is instead hard-cancelled (its `run` future
dropped) MAY leave in-flight jobs unresolved, in which case they are redelivered
later under at-least-once delivery.

#### Scenario: Shutdown while idle returns

- **WHEN** the worker is idle in `run` and the shutdown signal fires
- **THEN** `run` SHALL return without reserving further jobs

#### Scenario: Shutdown drains all in-flight jobs first

- **WHEN** the shutdown signal fires while one or more handlers are running
- **THEN** every in-flight job SHALL run to completion and be resolved with its
  receipt
- **AND** `run` SHALL return only after all in-flight jobs have resolved

### Requirement: Bounded long-handler support

A worker SHALL support an optional **handler timeout** bounding how long a single
handler may run, and when one is configured it SHALL hold the reservation across
a slow handler and bound a stuck one. The handler timeout is the maximum
wall-clock time a single handler may run. When a handler timeout is configured,
while a handler runs within its timeout the worker SHALL periodically **extend**
the job's reservation lease (a heartbeat) so the job is not redelivered merely
for outliving its original lease. If a handler does not complete within its
timeout, the worker SHALL stop maintaining the lease and resolve the job through
the existing failure path — retry while attempts remain, otherwise dead-letter
with a timeout error — so a stuck handler stays bounded and is eventually
dead-lettered rather than held indefinitely.

When no handler timeout is configured (the default), the worker SHALL neither
heartbeat nor time out a handler; lease expiry and possible redelivery behave as
before.

#### Scenario: Heartbeat holds a slow handler's lease

- **WHEN** a handler timeout is configured and a handler runs longer than the
  reservation lease but completes within its timeout
- **THEN** the worker SHALL extend the lease while the handler runs so the job is
  not redelivered
- **AND** on completion the worker SHALL ack the job with its current receipt
- **AND** the handler SHALL run exactly once

#### Scenario: Timed-out handler is failed

- **WHEN** a handler does not complete within its configured timeout
- **THEN** the worker SHALL resolve the job through the failure path: retry with
  the policy delay while `attempts + 1 < max_attempts`, otherwise dead-letter
  with a timeout error
- **AND** the worker SHALL continue processing subsequent jobs

#### Scenario: Default has no timeout and no heartbeat

- **WHEN** no handler timeout is configured and a handler runs
- **THEN** the worker SHALL NOT extend the lease and SHALL NOT time out the handler

#### Scenario: Lost lease during a heartbeat is tolerated

- **WHEN** a heartbeat `extend` is rejected as a stale reservation (the lease was
  already lost and the job redelivered)
- **THEN** the worker SHALL stop extending that job and SHALL NOT crash or stall
- **AND** the handler's eventual resolution SHALL be rejected as stale and logged

### Requirement: Handler panic isolation

A worker SHALL contain a panic that unwinds out of a handler and treat it as a
handler failure rather than letting it propagate. The worker SHALL resolve the
panicking job through the existing failure path — retry with the policy delay
while `attempts + 1 < max_attempts`, otherwise dead-letter with a panic error —
and SHALL continue processing other jobs. A panic in one in-flight handler MUST
NOT crash the worker, stall its loop, or abandon other in-flight jobs. This
relies on the unwinding panic strategy; a build configured to abort on panic is
out of scope.

#### Scenario: Panicking handler is dead-lettered

- **WHEN** a handler panics on its final attempt (`attempts + 1 >= max_attempts`)
- **THEN** the worker SHALL dead-letter the job with a panic error
- **AND** the worker SHALL continue processing subsequent jobs

#### Scenario: Panicking handler is retried below max attempts

- **WHEN** a handler panics and `attempts + 1 < max_attempts`
- **THEN** the worker SHALL retry the job with the policy-computed delay
- **AND** a later successful attempt SHALL ack the job normally

#### Scenario: A panic does not abandon sibling jobs

- **WHEN** one handler panics while other handlers are in flight under the
  worker's concurrency
- **THEN** the worker SHALL NOT crash or stall
- **AND** every sibling in-flight job SHALL still run to completion and be
  resolved
