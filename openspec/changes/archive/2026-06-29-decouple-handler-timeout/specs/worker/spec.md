## MODIFIED Requirements

### Requirement: Bounded long-handler support

A worker SHALL support an optional **handler timeout** bounding how long a single
handler may run, and an independent opt-in **lease keepalive** that maintains the
reservation across a slow handler. The handler timeout is the maximum wall-clock
time a single handler may run. Lease keepalive is the worker periodically
**extending** the job's reservation lease (a heartbeat) while the handler runs,
so the job is not redelivered merely for outliving its original lease.

The worker SHALL run the heartbeat whenever a reservation is held and **either** a
handler timeout **or** lease keepalive is configured. The heartbeat SHALL renew
the lease on an interval that leaves margin for the `extend` round-trip to
complete before the current lease expires and SHALL NOT heartbeat more often
than every 50ms. It SHALL NOT wait until the lease is nearly expired to begin
renewing.

A configured handler timeout SHALL fire after its duration even when the handler
does not yield control at an `.await`, **provided a runtime worker thread is free
to poll the timeout** — i.e. on a multi-thread runtime where fewer handlers are
concurrently blocking their threads than the runtime has worker threads. Under
that condition the timeout, the heartbeat, and the reserve loop each make
progress independently of any single handler. When a handler timeout is
configured and a handler does not complete within it, the worker SHALL stop
maintaining the lease, free the handler's concurrency slot, and resolve the job
through the existing failure path — retry while attempts remain, otherwise
dead-letter with a timeout error — so a stuck handler stays bounded and is
eventually dead-lettered rather than held indefinitely. This SHALL hold even for
a handler that never yields, subject to the worker-thread-availability condition
above.

When lease keepalive is enabled without a handler timeout, the worker SHALL keep
extending the lease for as long as the handler runs and SHALL NOT impose a
deadline; bounding such a handler is then the caller's responsibility. When
neither a handler timeout nor lease keepalive is configured (the default), the
worker SHALL neither heartbeat nor time out a handler; lease expiry and possible
redelivery behave as before.

Firing the timeout stops the handler at its next yield point; it does **not**
preempt a handler that never yields. A handler that blocks the async executor
without yielding — a tight CPU loop, blocking I/O, or a long synchronous call —
therefore keeps running until it yields or returns, even after the timeout has
fired and the job has been resolved and its slot freed; and on a current-thread
runtime such a handler blocks the single executor thread, so the timeout, the
heartbeat, and the reserve loop cannot make progress until it yields. Likewise,
if non-yielding handlers occupy every worker thread of a multi-thread runtime, no
thread remains to poll any timeout until one frees. Callers MUST run blocking or
CPU-bound work off the async task (for example via `tokio::task::spawn_blocking`)
and `.await` its result; only that removes the dependence on yielding entirely.

#### Scenario: Heartbeat holds a slow handler's lease

- **WHEN** a handler timeout is configured and a handler runs longer than the
  reservation lease but completes within its timeout
- **THEN** the worker SHALL extend the lease while the handler runs so the job is
  not redelivered
- **AND** on completion the worker SHALL ack the job with its current receipt
- **AND** the handler SHALL run exactly once

#### Scenario: Keepalive holds a slow handler's lease without a timeout

- **WHEN** lease keepalive is enabled, no handler timeout is configured, and a
  handler runs longer than the reservation lease
- **THEN** the worker SHALL extend the lease while the handler runs so the job is
  not redelivered
- **AND** on completion the worker SHALL ack the job with its current receipt
- **AND** the handler SHALL run exactly once

#### Scenario: Timed-out handler is failed

- **WHEN** a handler does not complete within its configured timeout
- **THEN** the worker SHALL stop waiting for that handler
- **AND** the worker SHALL resolve the job through the failure path: retry with
  the policy delay while `attempts + 1 < max_attempts`, otherwise dead-letter
  with a timeout error
- **AND** the worker SHALL continue processing subsequent jobs

#### Scenario: Timeout fires for a non-yielding handler

- **WHEN** a handler timeout is configured, the worker runs on a multi-thread
  runtime with a worker thread free to poll the timeout, and a handler exceeds
  its timeout without yielding at an `.await` (e.g. a blocking call)
- **THEN** the worker SHALL still fire the timeout, stop maintaining the lease,
  free the handler's concurrency slot, and resolve the job through the failure
  path (retry or dead-letter with a timeout error)
- **AND** the worker SHALL continue reserving and processing other jobs while the
  orphaned handler runs to its next yield point

#### Scenario: Non-yielding handlers that saturate all worker threads still stall

- **WHEN** non-yielding handlers occupy every worker thread of the runtime
- **THEN** no thread remains to poll any handler's timeout until one frees, so
  timeouts do not fire in the meantime
- **AND** the documented bound for blocking or CPU-bound work is `spawn_blocking`,
  which runs it off the async worker threads

#### Scenario: Default has no timeout and no heartbeat

- **WHEN** neither a handler timeout nor lease keepalive is configured and a
  handler runs
- **THEN** the worker SHALL NOT extend the lease and SHALL NOT time out the handler

#### Scenario: Lost lease during a heartbeat is tolerated

- **WHEN** a heartbeat `extend` is rejected as a stale reservation (the lease was
  already lost and the job redelivered)
- **THEN** the worker SHALL stop extending that job and SHALL NOT crash or stall
- **AND** the handler's eventual resolution SHALL be rejected as stale and logged

#### Scenario: Heartbeat transport failure signals cancellation

- **WHEN** a heartbeat `extend` fails with a non-stale broker error
- **THEN** the worker SHALL stop extending that job
- **AND** the worker SHALL signal cooperative cancellation to the handler
- **AND** the worker SHALL log the heartbeat failure

#### Scenario: A non-yielding handler is not preempted

- **WHEN** a handler timeout is configured and a handler blocks the async
  executor without yielding at an `.await`
- **THEN** firing the timeout SHALL stop the handler at its next yield point but
  SHALL NOT preempt a handler that never yields; the orphaned task MAY keep
  running until it yields or returns
- **AND** on a current-thread runtime such a handler blocks the single executor,
  so the timeout and heartbeat cannot run until it yields
- **AND** the documented requirement is that callers run blocking or CPU-bound
  work off the async task (e.g. `spawn_blocking`)
