## ADDED Requirements

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
