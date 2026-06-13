## ADDED Requirements

### Requirement: Long-running poll loop

The worker SHALL provide a `run` operation that processes jobs until a shutdown
signal: it SHALL process every currently available job on its lane, and when no
job is available it SHALL wait a configurable poll interval before checking
again. This lets a worker pick up jobs that become available later (for example
a pending retry whose delay has elapsed), which `run_until_idle` does not.
Processing remains strictly one job at a time.

#### Scenario: Processes available jobs then waits

- **WHEN** `run` is executing and jobs are available on the lane
- **THEN** it SHALL process them one at a time until none remain
- **AND** it SHALL then wait the poll interval before checking the lane again

#### Scenario: Picks up work that appears while idle

- **WHEN** the worker is idle in `run` and a job then becomes available on its lane
- **THEN** the worker SHALL process that job on a subsequent poll

### Requirement: Cooperative shutdown

`run` SHALL accept a shutdown signal and stop cleanly. The signal SHALL be
honoured only between jobs: an in-flight job SHALL run to completion and be
resolved (ack, retry, or fail) before `run` returns. A worker that is instead
hard-cancelled (its `run` future dropped) MAY leave an in-flight job unresolved,
in which case the job is redelivered later under at-least-once delivery.

#### Scenario: Shutdown while idle returns

- **WHEN** the worker is idle in `run` and the shutdown signal fires
- **THEN** `run` SHALL return without processing further jobs

#### Scenario: Shutdown during a job finishes that job first

- **WHEN** the shutdown signal fires while a job's handler is running
- **THEN** that job SHALL run to completion and be resolved with its receipt
- **AND** `run` SHALL return only after that resolution
