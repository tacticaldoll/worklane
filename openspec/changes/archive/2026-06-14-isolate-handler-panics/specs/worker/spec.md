## ADDED Requirements

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
