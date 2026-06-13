## ADDED Requirements

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

## MODIFIED Requirements

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

## REMOVED Requirements

### Requirement: Sequential processing loop

**Reason**: Generalized into the **Bounded concurrent processing** requirement —
the worker now supports up to N concurrent handlers rather than strictly one at
a time.

**Migration**: None for callers. Concurrency defaults to 1, which preserves the
original one-job-at-a-time guarantee exactly; raise it with
`Worker::with_concurrency(n)`.
