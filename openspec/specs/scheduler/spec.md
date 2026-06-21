# Scheduler Specification

## Purpose

Defines how recurring (cron) schedules are declared and how a scheduler daemon
enqueues each schedule's templated job when it becomes due, on the injected
clock, with cooperative shutdown, missed-occurrence skipping, and an opt-in
per-fire deduplication key. The scheduler only enqueues through the broker; it
adds nothing to the `Broker` contract.
## Requirements
### Requirement: Schedule definition

A scheduler SHALL accept **schedule definitions**, each pairing a cron expression
with a job template: the job kind, its serialized payload, and the target lane.
A definition with an unparseable cron expression SHALL be rejected when it is
added, not silently ignored at run time. Each definition SHALL have a stable
identifier (caller-supplied or derived) used for logging and per-fire dedup.

Schedule identifiers SHALL be unique within a scheduler. The identifier is the
cluster-wide occurrence key (it keys both the `enqueue_scheduled` watermark and,
when enabled, the per-fire `unique_key`), so two definitions sharing an
identifier would have one silently swallow the other's fires. A definition whose
identifier is already registered SHALL be rejected when it is added, not accepted
and left to fail silently at run time.

#### Scenario: Add a valid schedule

- **WHEN** a schedule is added with a valid cron expression and a job template
- **THEN** the scheduler SHALL accept it and include it when computing due times

#### Scenario: Reject an invalid cron expression

- **WHEN** a schedule is added with an unparseable cron expression
- **THEN** the scheduler SHALL reject it with an error
- **AND** it MUST NOT be registered or fire at run time

#### Scenario: Reject a duplicate schedule id

- **WHEN** a schedule is added with an identifier already registered on the
  scheduler
- **THEN** the scheduler SHALL reject it with a registration error
- **AND** it MUST NOT be registered (the first definition for that id is
  unaffected)

### Requirement: Recurring enqueue on schedule

The scheduler SHALL provide a `run` operation that, until a shutdown signal,
enqueues each schedule's templated job through the broker every time that
schedule's cron expression becomes due, evaluated on the injected clock. `run`
SHALL compute the earliest next due time across all schedules, wait until then,
enqueue every schedule that is due at that instant, and repeat. A schedule with
no registered handler on the worker side is still enqueued (the worker, not the
scheduler, owns unknown-kind handling).

#### Scenario: Fires a due schedule

- **WHEN** `run` is active and a schedule's cron expression becomes due on the
  clock
- **THEN** the scheduler SHALL enqueue that schedule's templated job to its
  target lane

#### Scenario: Fires repeatedly

- **WHEN** a schedule's cron expression is due on multiple successive occurrences
- **THEN** the scheduler SHALL enqueue the templated job once per occurrence

#### Scenario: Multiple schedules due at once

- **WHEN** more than one schedule is due at the same instant
- **THEN** the scheduler SHALL enqueue each due schedule's job

### Requirement: Cooperative shutdown

`Scheduler::run` SHALL accept a shutdown signal and stop cleanly, interrupting
any wait for the next due time so `run` returns promptly without enqueuing
further jobs.

#### Scenario: Shutdown while waiting returns

- **WHEN** the scheduler is waiting for the next due time and the shutdown signal
  fires
- **THEN** `run` SHALL stop waiting and return without enqueuing further jobs

### Requirement: Missed occurrences are not backfilled

The scheduler SHALL NOT backfill missed occurrences. When the scheduler starts
(or resumes after being blocked) and one or more occurrences of a schedule fell
in the past, the scheduler MUST fire based on the clock's current time and MUST
NOT backfill an enqueue for each missed occurrence. The next fire is the next
due time at or after now.

This no-backfill guarantee SHALL hold even when firing itself is slow. After a
schedule fires, the scheduler MUST advance that schedule's cursor past the
clock's time *as observed after the fire completes*, so an occurrence that
elapses during a slow fire is skipped rather than enqueued on a subsequent loop
iteration. The advance MUST remain a single step (it MUST NOT iterate one cron
occurrence per skipped occurrence), so a large clock jump does not cause
per-occurrence work.

#### Scenario: Past occurrences are skipped

- **WHEN** the scheduler starts and a schedule had occurrences while it was not
  running
- **THEN** the scheduler SHALL NOT enqueue one job per missed occurrence
- **AND** it SHALL enqueue at the next occurrence due at or after the current time

#### Scenario: A slow fire does not backfill an elapsed occurrence

- **WHEN** a schedule fires and the time taken to fire causes the schedule's
  next occurrence to become due before the fire completes
- **THEN** the scheduler SHALL advance past that elapsed occurrence and SHALL NOT
  enqueue it on the next loop iteration
- **AND** the next fire SHALL be the next occurrence due at or after the time
  observed once the fire completed

### Requirement: Optional per-fire deduplication

A schedule SHALL support being configured to enqueue with a `unique_key` derived from its
identifier and the fire timestamp. When enabled, two enqueues for the same
schedule and the same fire instant SHALL deduplicate to a single live job via the
broker's unique-key handling. When not enabled, each
fire enqueues an independent job as usual.

#### Scenario: Dedup key makes a fire idempotent

- **WHEN** per-fire dedup is enabled and the same schedule fires for the same
  instant is attempted twice
- **THEN** the broker's unique-key handling SHALL keep at most one live job for
  that schedule-and-instant

#### Scenario: Without dedup each fire is independent

- **WHEN** per-fire dedup is not enabled
- **THEN** each occurrence SHALL enqueue an independent job with no unique key

### Requirement: Scheduler packaging

The scheduler SHALL be provided by a dedicated `worklane-scheduler` crate and
SHALL NOT be exported from the base `worklane` facade crate. Consumers that do
not use scheduling SHALL NOT compile the `cron` or `chrono` dependencies as a
result of depending on `worklane`. The `worklane-scheduler` crate SHALL build on
the public `worklane-core` / broker API. Coordination for HA scheduling is
provided by `enqueue_scheduled` on the optional `ScheduledStore` capability
(obtained through `Broker::scheduled_store`), which atomically claims each
occurrence so only one instance fires it; any further coordination mechanism
added to `worklane-core` or the capability traits MUST be answerable by all
durable broker implementations.

#### Scenario: Scheduler imported from its own crate
- **WHEN** a consumer wants recurring schedules
- **THEN** it SHALL add a dependency on `worklane-scheduler` and import
  `worklane_scheduler::Scheduler`
- **AND** `worklane::Scheduler` SHALL NOT exist

#### Scenario: Base crate is free of scheduling dependencies
- **WHEN** a consumer depends only on `worklane` (and a broker) for plain typed
  jobs
- **THEN** neither `cron` nor `chrono` SHALL be pulled in by the `worklane` crate
- **AND** the scheduler's behavior, when used via `worklane-scheduler`, SHALL be
  identical to before the extraction

### Requirement: Distributed HA Coordination

The scheduler SHALL support High Availability (HA) deployments where multiple
instances of the scheduler run concurrently. To prevent multiple scheduler
instances from enqueuing the same job at the same scheduled occurrence, the
scheduler SHALL coordinate across instances. This coordination MUST ensure that
each occurrence of a schedule is fired at most once across the entire cluster.

#### Scenario: Multiple schedulers do not duplicate jobs
- **WHEN** multiple active scheduler instances are evaluating the same schedules
- **THEN** the broker SHALL receive and successfully enqueue exactly one job per
  occurrence
- **AND** other instances SHALL gracefully skip or be rejected from enqueuing the
  same occurrence

#### Scenario: Crash mid-enqueue does not lose occurrence
- **WHEN** a scheduler instance crashes while claiming an occurrence
- **THEN** the claim and enqueue SHALL be atomic, such that either the job is
  successfully enqueued, or the claim is not recorded and another instance can
  claim it
- **AND** the occurrence SHALL NOT be permanently lost

### Requirement: Resilient scheduler fire-error handling

`Scheduler::run` SHALL support a configurable handling of **broker errors raised
while firing a due schedule** (errors returned by `enqueue_scheduled` or the
underlying enqueue). By default `run` SHALL **fail fast**: it SHALL stop and
return the error, ending the daemon. When **resilient mode** is enabled, `run`
SHALL instead log the error and continue the loop — advancing past the failed
fire to the next due time — so a transient broker fault does not permanently
stop all scheduling. Cooperative shutdown SHALL behave identically in both
modes, and a lost (false) claim from `enqueue_scheduled` is not an error in
either mode (it is the normal HA outcome of another instance winning).

#### Scenario: Default fail-fast surfaces the fire error

- **WHEN** resilient mode is disabled (the default) and `enqueue_scheduled`
  returns a non-stale broker error while firing a due schedule
- **THEN** `run` SHALL stop and return that error

#### Scenario: Resilient mode logs and continues

- **WHEN** resilient mode is enabled and `enqueue_scheduled` returns a broker
  error while firing a due schedule
- **THEN** the scheduler SHALL log the error and continue the loop without
  returning
- **AND** when the broker recovers, subsequent due schedules SHALL fire normally

#### Scenario: Lost claim is not treated as an error

- **WHEN** `enqueue_scheduled` returns `false` because another instance already
  claimed the occurrence
- **THEN** the scheduler SHALL treat the fire as a normal skip in both modes and
  SHALL NOT log it as an error or stop the loop
