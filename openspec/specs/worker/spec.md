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

### Requirement: Handler context carries the lane

The per-run context handed to a dispatched handler SHALL carry the lane the job
was reserved from, so a handler can observe its lane without inspecting the
broker.

#### Scenario: Handler observes its lane

- **WHEN** a job enqueued to lane `"critical"` is reserved and dispatched to its
  handler
- **THEN** the context passed to the handler SHALL carry the lane `"critical"`

### Requirement: Bounded concurrent processing

The worker SHALL process up to a configured maximum number of jobs — its
**concurrency** — at once. Each in-flight job SHALL be reserved with its
receipt, dispatched, run to completion or to the configured timeout boundary,
and resolved (ack / retry / fail) with that receipt. The worker SHALL NOT exceed
its configured concurrency in flight.
Concurrency SHALL default to 1, which is strictly sequential: no new job is
reserved until the current job has been acked, retried, failed, or rejected as a
stale reservation.

Handler panics during normal polling SHALL be isolated and routed through the
normal failure path. If a handler timeout is configured, the worker SHALL
abandon the timed-out handler future and route the job through the failure path.
The handler contract SHALL make any remaining isolation limits explicit without
weakening at-least-once delivery.

The worker's observer contract SHALL expose per-attempt start and stop hooks in
addition to successful-resolution outcome hooks. Start and stop SHALL be paired
for every reserved in-flight attempt, including attempts that lose their lease,
are deferred, time out, or are cancelled before resolution takes effect.

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
reservation receipt returned by `reserve`. If the worker is configured with a
result store, the worker SHALL store the handler's output in the result store
using the job's ID before acknowledging the job.

#### Scenario: Successful handler

- **WHEN** a handler returns success
- **THEN** the worker SHALL store the result if a result store is configured
- **AND** the worker SHALL ack the job with the current receipt
- **AND** the job SHALL NOT be retried or dead-lettered

#### Scenario: Ack rejected after lease expiry

- **WHEN** a handler returns success after its reservation receipt is no longer current
- **THEN** the worker SHALL log the stale-resolution result
- **AND** the worker SHALL continue processing subsequent jobs

#### Scenario: Result store failure prevents ack

- **WHEN** a handler returns success but storing the result fails
- **THEN** the worker SHALL NOT ack the job
- **AND** the worker SHALL resolve the job through the failure path (retry or
  dead-letter)

### Requirement: Failure retries until max attempts

The worker SHALL retry a failed job, with a delay from the retry policy, while it
has remaining attempts, and SHALL fail it to the dead-letter store with the
handler error once no attempts remain. Retry and fail resolution SHALL use the
reservation receipt returned by `reserve`.

#### Scenario: Retry below max attempts

- **WHEN** a handler errors and `attempts + 1 < max_attempts`
- **THEN** the worker SHALL retry the job with the current receipt and the
  policy-computed delay

#### Scenario: Dead-letter at max attempts

- **WHEN** a handler errors and `attempts + 1 >= max_attempts`
- **THEN** the worker SHALL fail the job to the dead-letter store with the
  current receipt and the handler error

#### Scenario: Retry or fail rejected after lease expiry

- **WHEN** a handler errors after its reservation receipt is no longer current
- **THEN** the worker SHALL log the stale-resolution result
- **AND** the worker SHALL continue processing subsequent jobs

### Requirement: Payload decode failures are non-retryable

The worker SHALL immediately dead-letter a reserved job whose stored payload
cannot be decoded into the handler's input type, without consuming a retry
attempt — the bytes will never decode, so the job is unrecoverable. This is
distinct from a handler's *output* failing to encode after the handler has
already succeeded, which can be transient and which the worker SHALL route
through the normal failure path.

#### Scenario: Undecodable payload is dead-lettered immediately

- **WHEN** a reserved job's payload cannot be decoded into the handler's input
  type
- **THEN** the worker SHALL fail the job to the dead-letter store with the current
  receipt and a serialization error
- **AND** the worker SHALL NOT retry it, regardless of remaining attempts

#### Scenario: Output-encode failure takes the normal failure path

- **WHEN** a handler returns success but its output value cannot be encoded
- **THEN** the worker SHALL NOT ack the job
- **AND** the worker SHALL resolve it through the failure path (retry while
  attempts remain, otherwise dead-letter)

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

- **WHEN** an unknown-kind job's reservation receipt is no longer current before
  failure resolution
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
job is available it SHALL wait an **idle backoff** before checking again. The
idle backoff SHALL start at a configured base duration, grow (bounded by a
configured cap) across consecutive empty polls, and reset to the base as soon as
a job is found. This bounds how frequently an idle worker calls the broker on an
empty lane — material for a remote broker — while keeping latency low once work
appears. The backoff base and cap SHALL be configurable; defaults SHALL preserve
the prior poll-interval behavior. This lets a worker pick up jobs that become
available later (for example a pending retry whose delay has elapsed), which
`run_until_idle` does not. A shutdown signal SHALL interrupt the idle wait so
`run` returns promptly.

The worker SHALL also provide a `run_until_idle` operation that processes
currently-available jobs and returns once the lane is idle (no currently-visible
job and nothing in flight), without waiting for jobs that may become available
later. **Both** `run` and `run_until_idle` SHALL process jobs up to the worker's
configured concurrency — up to N at once (N defaults to 1, strictly one job at a
time) — so the concurrency setting is not silently ignored by either.

#### Scenario: Processes available jobs then waits

- **WHEN** `run` is executing and jobs are available on the lane
- **THEN** it SHALL process them, up to its configured concurrency at a time,
  until none remain
- **AND** it SHALL then wait the idle backoff (starting at the base) before
  checking the lane again

#### Scenario: Picks up work that appears while idle

- **WHEN** the worker is idle in `run` and a job then becomes available on its lane
- **THEN** the worker SHALL process that job on a subsequent poll

#### Scenario: run_until_idle honours concurrency and returns when idle

- **WHEN** `run_until_idle` is invoked with concurrency N and at least N jobs are
  available on the lane
- **THEN** up to N jobs SHALL run at once (not strictly one at a time)
- **AND** the call SHALL return once the lane has no currently-visible job and
  nothing is in flight

#### Scenario: Idle backoff grows then resets

- **WHEN** the worker polls an empty lane on consecutive cycles
- **THEN** the wait between polls SHALL grow from the base up to the configured
  cap and SHALL NOT exceed the cap
- **AND** once a job is found and processed, the wait SHALL reset to the base

#### Scenario: Shutdown interrupts the idle wait

- **WHEN** the worker is waiting out an idle backoff and the shutdown signal fires
- **THEN** `run` SHALL stop waiting and return without reserving further jobs

### Requirement: Resilient broker-error handling

`run` SHALL support a configurable handling of **non-stale broker errors** (errors
from `reserve` and other broker calls that are not stale-reservation rejections).
By default `run` SHALL **fail fast**: it SHALL stop reserving new jobs, let
in-flight jobs drain to resolution under cooperative shutdown, and return the
error. When **resilient mode** is enabled, `run` SHALL instead log the error and
continue the loop after an idle backoff, without returning, so a transient broker
fault does not tear down the daemon. Stale-reservation results SHALL continue to
be logged and tolerated in both modes (unchanged), and cooperative shutdown and
in-flight drain SHALL behave identically in both modes.

#### Scenario: Default fail-fast surfaces the error

- **WHEN** resilient mode is disabled (the default) and a broker call returns a
  non-stale error during `run`
- **THEN** `run` SHALL stop reserving new jobs, drain any in-flight jobs to
  resolution, and return the error

#### Scenario: Resilient mode logs and continues

- **WHEN** resilient mode is enabled and a broker call returns a non-stale error
  during `run`
- **THEN** the worker SHALL log the error and continue the loop after an idle
  backoff, without returning
- **AND** when the broker recovers, the worker SHALL resume processing jobs

#### Scenario: Shutdown is honoured in resilient mode

- **WHEN** resilient mode is enabled and the shutdown signal fires
- **THEN** `run` SHALL drain in-flight jobs and return cleanly, exactly as in the
  default mode

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

### Requirement: UTF-8-Safe Error Bounds

Worker and core error paths SHALL bound human-facing error strings without
panicking on arbitrary UTF-8 input. Truncation MUST occur on valid character
boundaries.

#### Scenario: Long Unicode error is bounded

- **WHEN** a handler or payload decode failure produces a long Unicode error
  message
- **THEN** the stored or displayed bounded message SHALL remain valid UTF-8
- **AND** the worker SHALL NOT panic while truncating it

#### Scenario: Short error is unchanged

- **WHEN** an error message is already within the configured bound
- **THEN** the stored or displayed message SHALL preserve the original text
