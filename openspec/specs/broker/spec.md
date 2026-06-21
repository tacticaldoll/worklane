# Broker Specification

## Purpose

Defines the backend-agnostic `Broker` contract. The core `Broker` trait is the
job lifecycle: how jobs are enqueued, reserved under a visibility lease, resolved
(ack / retry / fail), dead-lettered when attempts are exhausted, and classified.
Dead-letter inspection / requeue, queue-depth statistics, and scheduled enqueue
are **optional capabilities** a broker exposes through `Broker` accessor methods
(`dead_letter_store`, `queue_stats`, `scheduled_store`), defined by the
`DeadLetterStore`, `QueueStats`, and `ScheduledStore` traits respectively. The
broker operates only on opaque envelopes.
## Requirements
### Requirement: Durable Receipt Lookup

Durable SQL brokers SHALL make receipt-based resolution efficient enough for the
hot path. Their baseline schema MUST include an index or equivalent lookup path
for currently leased jobs by reservation receipt.

#### Scenario: SQL schema indexes receipts

- **WHEN** a durable SQL broker initializes its baseline schema
- **THEN** the live job store SHALL include an index or equivalent lookup path
  for non-null reservation receipts

### Requirement: Atomic batch enqueue

The broker SHALL provide an atomic `enqueue_batch` method that accepts multiple
jobs. It MUST ensure all-or-nothing insertion.

#### Scenario: All jobs visible
- **WHEN** a batch of multiple jobs is successfully enqueued
- **THEN** all jobs in the batch MUST be immediately available for reservation

#### Scenario: Preservation of order
- **WHEN** a batch of jobs is enqueued
- **THEN** the returned `JobId` list MUST exactly match the input array's index order

### Requirement: Batch unique key deduplication

The broker SHALL handle `unique_key` deduplication within a batch without
failing the batch insertion.

Concurrent batches MUST NOT deadlock or spuriously fail because their unique
keys overlap. When two batches are enqueued concurrently and share one or more
unique keys — in any relative order — each batch SHALL complete: every shared
key deduplicates to a single live job, and neither batch surfaces a deadlock or
lock-ordering error to the caller.

#### Scenario: Collision with an existing live job
- **WHEN** a job in a batch has a unique key identical to an existing live job
- **THEN** the broker MUST NOT abort the batch and SHALL return the existing
  `JobId` for that specific job

#### Scenario: Intra-batch collision deduplication
- **WHEN** two jobs within the same batch share the same unique key
- **THEN** the first job SHALL be inserted normally, and the second job SHALL
  receive the `JobId` of the first job

#### Scenario: Concurrent overlapping batches do not deadlock
- **WHEN** two batches are enqueued concurrently and their unique keys overlap
  in opposite order (one batch lists `[A, B]`, the other `[B, A]`)
- **THEN** both batches SHALL complete without a deadlock or lock-ordering error
- **AND** each shared key SHALL deduplicate to a single live job

### Requirement: Atomic scheduled enqueue

A `ScheduledStore` (obtained through `Broker::scheduled_store`) SHALL provide an
atomic `enqueue_scheduled` method that accepts a
schedule identifier, an occurrence timestamp, and a `NewJob`. It MUST atomically
claim the occurrence if it is strictly greater than the last recorded occurrence
for that schedule. If the claim succeeds, the store SHALL record the occurrence
and apply enqueue semantics to the supplied job, including live `JobId`
idempotency and `unique_key` deduplication. The method SHALL return true when
the occurrence claim succeeds, even if the supplied job deduplicates to an
existing live job. If the claim fails because the occurrence is less than or
equal to the last recorded occurrence, the job MUST NOT be enqueued and the
method SHALL return false.

A schedule with no recorded occurrence SHALL accept the first claim of any
occurrence value, including occurrences less than or equal to `0`, and including
`i64::MIN`. The "strictly greater than the last recorded occurrence" rule
applies only once an occurrence has been recorded. Implementations MUST NOT use
`0` or any value other than the absence of a record as the "no prior occurrence"
sentinel.

#### Scenario: First claim succeeds and enqueues job

- **WHEN** a scheduled occurrence is claimed for the first time
- **THEN** the occurrence SHALL be recorded
- **AND** the job SHALL be made live according to normal enqueue semantics
- **AND** the broker SHALL return true

#### Scenario: First claim succeeds and deduplicates unique key

- **WHEN** a scheduled occurrence is claimed for the first time
- **AND** the `NewJob` carries a unique key already held by a live job
- **THEN** the occurrence SHALL be recorded
- **AND** the broker SHALL NOT create a second job
- **AND** the broker SHALL return true

#### Scenario: First claim succeeds and deduplicates live JobId

- **WHEN** a scheduled occurrence is claimed for the first time
- **AND** the `NewJob` carries a `JobId` already held by a live job
- **THEN** the occurrence SHALL be recorded
- **AND** the broker SHALL NOT create or overwrite a second live job
- **AND** the broker SHALL return true

#### Scenario: First claim of a non-positive occurrence succeeds

- **WHEN** a schedule has no recorded occurrence
- **AND** the occurrence claimed is less than or equal to `0`
- **THEN** the occurrence SHALL be recorded
- **AND** the job SHALL be made live according to normal enqueue semantics
- **AND** the broker SHALL return true

#### Scenario: Duplicate claim fails without enqueuing

- **WHEN** a scheduled occurrence is claimed but is equal to the last recorded
  occurrence
- **THEN** the job SHALL NOT be enqueued
- **AND** the broker SHALL return false

#### Scenario: Older claim fails without enqueuing

- **WHEN** a scheduled occurrence is claimed but is strictly less than the last
  recorded occurrence
- **THEN** the job SHALL NOT be enqueued
- **AND** the broker SHALL return false

### Requirement: Scheduled Occurrence Units

Scheduled occurrence values accepted by `enqueue_scheduled` SHALL represent Unix
seconds. Brokers MUST store and compare occurrence values as opaque signed
integers and MUST NOT reinterpret them using backend-local time zones.

#### Scenario: Unix second occurrence is recorded exactly

- **WHEN** `enqueue_scheduled` claims occurrence `123`
- **THEN** the broker SHALL compare future occurrences against the integer value
  `123`

#### Scenario: Backend time zone does not change ordering

- **WHEN** two scheduled occurrences are claimed on a backend configured with a
  non-UTC local time zone
- **THEN** their ordering SHALL be based only on the supplied Unix-second
  integers

### Requirement: Enqueue

The broker SHALL accept a `NewJob` and store it as a `JobEnvelope` with a
freshly assigned `JobId`, the lane carried by the `NewJob`, the assigned
`priority`, and `attempts = 0`, returning the `JobId`. The job SHALL become
visible for reservation after the `NewJob`'s
`delay`, measured from enqueue on the broker's injected clock; a zero delay (the
default) makes it immediately visible.

#### Scenario: Enqueue makes a job reservable

- **WHEN** a job is enqueued to a lane with no delay
- **THEN** a `reserve` on that lane SHALL be able to return it

#### Scenario: Stored envelope retains its lane

- **WHEN** a job is enqueued to lane `"critical"`
- **THEN** the stored envelope SHALL carry lane `"critical"`
- **AND** that lane SHALL be preserved through reservation and dead-lettering

#### Scenario: Delayed enqueue is hidden until its delay elapses

- **WHEN** a job is enqueued with a positive delay
- **THEN** it SHALL NOT be reservable before the delay elapses
- **AND** after the delay elapses it SHALL be reservable

### Requirement: Unique enqueue

When a `NewJob` carries a unique key, the broker SHALL ensure at most one live
job exists per key: enqueuing with a key already held by a live job SHALL NOT
create a second job and SHALL return the existing job's `JobId`. A key SHALL be
released when its job leaves the live store (via `ack` or `fail`), after which an
`enqueue` with that key SHALL create a new job. A `NewJob` with no unique key
SHALL never be deduplicated.

A `unique_key` is **opaque**: the broker SHALL accept any key bytes — including
`:` and the glob metacharacters `* ? [ ]` — and SHALL NOT reject, truncate, or
otherwise transform a key based on its characters. Two keys SHALL be treated as
the same key if and only if their bytes are equal. (This is distinct from a
`lane` or a schedule identifier, which a backend MAY constrain because they are
embedded in collision-significant or pattern-significant key positions.)

#### Scenario: Enqueue with a held key is deduplicated

- **WHEN** a job is enqueued with unique key `"k"` and another is enqueued with
  the same key while the first is still live
- **THEN** the second `enqueue` SHALL return the first job's `JobId`
- **AND** only one job SHALL be live for that key

#### Scenario: Key released after ack

- **WHEN** a job enqueued with key `"k"` is reserved and acked, then a job is
  enqueued with key `"k"` again
- **THEN** the second `enqueue` SHALL create a new job with a different `JobId`

#### Scenario: Key released after fail

- **WHEN** a job enqueued with key `"k"` is reserved and failed, then a job is
  enqueued with key `"k"` again
- **THEN** the second `enqueue` SHALL create a new job with a different `JobId`

#### Scenario: Distinct keys are not deduplicated

- **WHEN** one job is enqueued with key `"a"` and another with key `"b"`
- **THEN** both SHALL be created as distinct live jobs

#### Scenario: No key means no deduplication

- **WHEN** two jobs are enqueued with no unique key
- **THEN** both SHALL be created as distinct live jobs

#### Scenario: Unique key is opaque

- **WHEN** two jobs are enqueued with the same unique key containing `:` and glob
  metacharacters (for example `"chord:abc-*?[]:42"`) while the first is live
- **THEN** the second `enqueue` SHALL dedup to the first job's `JobId`
- **AND** a job enqueued with a different such key SHALL create a distinct job
- **AND** no backend SHALL reject the key for its characters

### Requirement: Reserve with visibility lease

`reserve(lane)` SHALL return at most one currently-visible job whose scheduled
time has arrived as a `Reservation` containing the `JobEnvelope` and an opaque
reservation receipt, and SHALL make that job invisible for a lease duration.
The job returned MUST be the one with the highest `priority` among all
currently-visible jobs; for jobs with the same priority, it MUST return the job
that became visible earliest; for jobs with the same priority and identical
visibility time, it MUST return the job that was enqueued earliest (strict
FIFO).
While leased, the job MUST NOT be returned by another `reserve`, including by a
concurrent `reserve` on the same lane. If the lease expires without a valid
`ack`, `retry`, or `fail`, the job SHALL become visible again (at-least-once
delivery), and the expired receipt SHALL be rejected for resolution.

Lease expiry is measured against the broker's injected clock. Because that clock
follows real time forward, a forward clock movement (e.g. an NTP jump) greater
than a reserved job's remaining lease SHALL be permitted to expire that lease
even while the original worker is still running the handler; the job MAY then be
reserved and executed again by another worker. Duplicate execution is therefore a
permitted consequence of at-least-once delivery, and handlers MUST be idempotent.
The broker SHALL NOT attempt to prevent such duplicate execution.

The ordering guarantee above applies to a job's **initial** delivery. Once a job
has been delivered and its lease has expired, its position relative to other jobs
on **redelivery** is implementation-defined: a backend MAY keep the job's original
enqueue position (so a repeatedly-redelivered job retains its place, as the
in-memory and SQL brokers do) or treat the lease expiry as its new visibility time
(so it yields to never-leased jobs, as the Redis broker does). Callers MUST NOT
rely on redelivery order; only the initial-delivery ordering above is guaranteed.

#### Scenario: Reserve highest priority job

- **WHEN** multiple visible jobs exist on a lane with different priorities
- **THEN** `reserve` SHALL return the job with the highest priority value

#### Scenario: Reserve oldest job within same priority

- **WHEN** multiple visible jobs exist on a lane with the same priority but
  different visibility times
- **THEN** `reserve` SHALL return the job that became visible earliest

#### Scenario: Reserve is FIFO for identical priority and visibility time

- **WHEN** multiple visible jobs exist on a lane with the same priority and
  identical visibility times
- **THEN** `reserve` MUST return them in the order they were enqueued (strict FIFO)

#### Scenario: Reserve hides the job

- **WHEN** a visible job is reserved
- **THEN** an immediately following `reserve` on the same lane SHALL NOT return that job

#### Scenario: Empty lane

- **WHEN** `reserve` is called on a lane with no visible jobs
- **THEN** it SHALL return no job

#### Scenario: Forward clock jump can expire a live lease

- **WHEN** a job is reserved with a lease and the broker's clock then advances
  forward by more than the remaining lease
- **THEN** the job SHALL become reservable again before the original worker
  resolves it
- **AND** a subsequent resolution with the original (now expired) receipt SHALL
  be rejected

### Requirement: Reserved lease window is observable

A `Reservation` returned by `reserve` SHALL convey the lease duration the broker
applied, so a caller can schedule lease maintenance (for example a heartbeat
that calls `extend`) without reading the broker's clock. The conveyed duration
SHALL equal the lease the broker uses to hide the reserved job.

#### Scenario: Reservation conveys the broker's lease

- **WHEN** a broker configured with a known lease duration reserves a job
- **THEN** the returned reservation SHALL convey that lease duration

### Requirement: Lane-scoped reserve

`reserve(lane)` SHALL only return jobs that were enqueued to that lane. A job on
one lane MUST NOT be returned by a `reserve` on a different lane. Lanes are
validated `Lane` values (see the `lane-identifier` capability) with no
registration; a lane that no worker reserves SHALL retain its jobs indefinitely,
which is a deliberate operator responsibility.

#### Scenario: Reserve returns only same-lane jobs

- **WHEN** a job is enqueued to lane `"critical"` and `reserve("critical")` is called
- **THEN** the reservation SHALL return that job

#### Scenario: Other lanes cannot steal

- **WHEN** a job is enqueued to lane `"critical"` and `reserve("default")` is called
- **THEN** `reserve("default")` SHALL return no job
- **AND** the job SHALL remain reservable on lane `"critical"`

#### Scenario: Lanes are isolated

- **WHEN** one job is enqueued to lane `"a"` and another to lane `"b"`
- **THEN** `reserve("a")` SHALL return only the lane `"a"` job
- **AND** `reserve("b")` SHALL return only the lane `"b"` job

#### Scenario: Unworked lane retains jobs

- **WHEN** a job is enqueued to a lane that no worker ever reserves
- **THEN** the job SHALL remain enqueued indefinitely and SHALL NOT be returned
  by a `reserve` on any other lane

### Requirement: Broker-specific lane rejection

A broker SHALL be permitted to reject a lane whose name it cannot safely encode
for its storage, even when that lane satisfies the portable `Lane` invariant (for
example a backend that embeds the lane in a structured key). When a broker cannot
encode a given lane, the operation SHALL return an error and SHALL store or change
nothing. Such rejection SHALL be consistent — the same lane is always rejected by
that broker — and SHALL NOT affect jobs on other lanes. A broker that can encode
every valid `Lane` rejects none.

#### Scenario: An unencodable lane is rejected without side effects

- **WHEN** a broker that cannot safely encode a given lane is asked to enqueue a
  job to it
- **THEN** the call SHALL return an error
- **AND** no job SHALL be stored for that lane or any other

#### Scenario: Rejection is lane-local

- **WHEN** a broker rejects an unencodable lane
- **THEN** jobs already enqueued on other lanes SHALL remain reservable and
  unaffected

#### Scenario: An unencodable lane in a batch rolls back the entire batch

- **WHEN** a broker that cannot safely encode a given lane is asked to enqueue a
  batch that includes a job on that lane
- **THEN** the batch enqueue SHALL return an error
- **AND** no jobs from the batch SHALL be stored or made visible on any lane

### Requirement: Acknowledge

`ack(receipt)` SHALL permanently remove the job from the broker only when the
receipt is the current valid receipt for the job's active reservation. When two
`ack` calls race with the same receipt, the broker SHALL remove the job at most
once: at most one such call SHALL succeed and any other SHALL be rejected with a
stale-reservation error.

When the acked job holds a uniqueness key, `ack` SHALL release that key
atomically with the job's removal: there SHALL be no observable or crash-induced
state in which the job is gone but its uniqueness key remains held. A subsequent
`enqueue` with that key SHALL therefore always be able to create a new job.

#### Scenario: Ack removes the job

- **WHEN** a reserved job is acked with its current receipt
- **THEN** it SHALL never be returned by `reserve` again
- **AND** it SHALL NOT appear in the dead-letter store

#### Scenario: Stale ack rejected

- **WHEN** `ack` is called with an expired or superseded receipt
- **THEN** the broker SHALL reject the ack with a stale-reservation error
- **AND** it SHALL NOT remove the job due to that stale receipt

#### Scenario: Concurrent acks resolve at most once

- **WHEN** a reserved job is acked twice concurrently with the same receipt
- **THEN** exactly one of the calls SHALL succeed
- **AND** the other SHALL be rejected with a stale-reservation error

#### Scenario: Ack releases the uniqueness key atomically

- **WHEN** a job enqueued with key `"k"` is reserved and acked, then a job is
  enqueued with key `"k"` again
- **THEN** the second `enqueue` SHALL create a new live job (the key was released
  together with the job's removal, leaving no orphaned key)

### Requirement: Retry

`retry(receipt, delay)` SHALL increment the job's `attempts`, schedule it to
become visible after `delay`, and end its current lease only when the receipt is
the current valid receipt for the job's active reservation. When two `retry`
calls race with the same receipt, the reservation SHALL be resolved at most once:
at most one such call SHALL succeed (incrementing `attempts` exactly once) and
any other SHALL be rejected with a stale-reservation error. This at-most-once
resolution SHALL hold even when the racing calls reach the store over separate
connections or processes, not only within one in-process serializer.

The `attempts` increment SHALL saturate at its maximum representable value rather
than wrap, so a job can never have its attempt count silently reset. An
arbitrarily large `delay` SHALL be accepted without overflow, clamped to the
maximum representable visibility time.

#### Scenario: Retry increments attempts and delays visibility

- **WHEN** a reserved job is retried with its current receipt and a delay
- **THEN** its `attempts` SHALL increase by one
- **AND** it SHALL NOT be reservable until the delay has elapsed
- **AND** after the delay it SHALL be reservable again with a new receipt

#### Scenario: Stale retry rejected

- **WHEN** `retry` is called with an expired or superseded receipt
- **THEN** the broker SHALL reject the retry with a stale-reservation error
- **AND** it SHALL NOT increment attempts or change the job's schedule due to
  that stale receipt

#### Scenario: Concurrent retries resolve at most once

- **WHEN** a reserved job is retried twice concurrently with the same receipt
- **THEN** exactly one of the calls SHALL succeed
- **AND** the other SHALL be rejected with a stale-reservation error
- **AND** the job's `attempts` SHALL have increased by exactly one

#### Scenario: Extreme delay does not overflow

- **WHEN** a reserved job is retried with its current receipt and a delay near the
  maximum representable `Duration`
- **THEN** the broker SHALL NOT panic
- **AND** the job's visibility time SHALL be clamped to the maximum representable
  value rather than wrapping

### Requirement: Fail to dead-letter

`fail(receipt, error)` SHALL remove the job from the live store and place it in
the dead-letter store, retaining the error message, only when the receipt is the
current valid receipt for the job's active reservation. When two `fail` calls
race with the same receipt, the reservation SHALL be resolved at most once: at
most one such call SHALL succeed (writing exactly one dead-letter record) and any
other SHALL be rejected with a stale-reservation error.

#### Scenario: Fail dead-letters the job

- **WHEN** a reserved job is failed with its current receipt and an error
- **THEN** it SHALL appear in the dead-letter store with that error
- **AND** it SHALL NOT be returned by `reserve` again

#### Scenario: Stale fail rejected

- **WHEN** `fail` is called with an expired or superseded receipt
- **THEN** the broker SHALL reject the fail with a stale-reservation error
- **AND** it SHALL NOT dead-letter the job due to that stale receipt

#### Scenario: Concurrent fails resolve at most once

- **WHEN** a reserved job is failed twice concurrently with the same receipt
- **THEN** exactly one of the calls SHALL succeed
- **AND** the other SHALL be rejected with a stale-reservation error
- **AND** the dead-letter store SHALL hold exactly one record for that job

### Requirement: Dead-letter read

A `DeadLetterStore` (obtained through `Broker::dead_letter_store`) SHALL expose a
bounded, lane-scoped read of dead-letter records:
`read_dead_letters(lane, limit)` SHALL return up to `limit` records for that
lane. Each record SHALL carry the preserved opaque `JobEnvelope` (all fields
unchanged) and the error message retained at `fail` time. The read SHALL NOT
remove records from the dead-letter store nor alter any field. Records for other
lanes SHALL NOT be returned. The order of returned records is unspecified.

A read concurrent with a `requeue` of a record on the same lane SHALL NOT fail:
a record removed by the requeue after the read began MAY be absent from the
result, but the read SHALL still succeed and return the remaining records.

#### Scenario: Read returns a failed job

- **WHEN** a job is reserved and failed with an error, then the dead-letter store
  is read for its lane
- **THEN** the read SHALL return a record carrying that job's envelope and the
  retained error
- **AND** a subsequent read SHALL still return it (the read is non-destructive)

#### Scenario: Bounded read honours the limit

- **WHEN** more jobs are dead-lettered on a lane than a read's `limit`
- **THEN** the read SHALL return at most `limit` records

#### Scenario: Read is lane-scoped

- **WHEN** jobs are dead-lettered on lane `"a"` and the dead-letter store is read
  for lane `"b"`
- **THEN** the read for lane `"b"` SHALL return no records

#### Scenario: Read preserves the opaque envelope

- **WHEN** a job with arbitrary (including non-UTF-8) payload bytes is
  dead-lettered and then read
- **THEN** the record's `payload` bytes, `kind`, and `max_attempts` SHALL equal
  the enqueued values exactly

#### Scenario: Empty dead-letter store

- **WHEN** the dead-letter store for a lane has no records and is read
- **THEN** the read SHALL return no records

#### Scenario: Read tolerates a concurrent requeue

- **WHEN** a dead-lettered record is requeued concurrently with a read of that
  lane's dead-letter store
- **THEN** the read SHALL succeed (it SHALL NOT surface a decode or storage error
  for the requeued-in-flight record)
- **AND** the requeued record MAY be absent from the returned records

### Requirement: Dead-letter count

A `DeadLetterStore` SHALL expose a bounded, lane-scoped count of dead-letter records:
`count_dead_letters(lane)` SHALL return the number of dead-letter records for
that lane as a `u64`. The count SHALL include only records for the given lane
(records for other lanes SHALL NOT be counted) and SHALL NOT remove or alter any
dead-letter record. The count is independent of any `read_dead_letters` `limit`:
it reflects every record present for the lane, not at most some bound.

#### Scenario: Count reflects the number of dead-lettered jobs

- **WHEN** `N` jobs are reserved and failed on a lane, then that lane's
  dead-letter store is counted
- **THEN** the count SHALL equal `N`

#### Scenario: Count is lane-scoped

- **WHEN** jobs are dead-lettered on lane `"a"` and the dead-letter store is
  counted for lane `"b"`
- **THEN** the count for lane `"b"` SHALL be `0`

#### Scenario: Empty dead-letter store counts zero

- **WHEN** the dead-letter store for a lane has no records and is counted
- **THEN** the count SHALL be `0`

#### Scenario: Count is non-destructive

- **WHEN** a lane's dead-letter store is counted and then read with
  `read_dead_letters`
- **THEN** the read SHALL still return every record, and a subsequent count
  SHALL return the same value (the count neither removes nor mutates records)

#### Scenario: Count stays consistent after requeue

- **WHEN** a lane holds `N` dead-letter records and one of them is requeued
- **THEN** a subsequent count for that lane SHALL equal `N - 1`

### Requirement: Job State Classification

The broker SHALL expose a bounded, by-id check of a job's state: `classify(id)`
SHALL return one of three states for a given job: `Live`, `DeadLettered`, or
`CompletedOrUnknown`. This method provides a single atomic snapshot, preventing
TOCTOU races between liveness and dead-letter classification.

The check SHALL be non-destructive and answerable without scanning (a point
lookup by id), and is lane-agnostic.

#### Scenario: Pending or leased job is reported live
- **WHEN** a job is enqueued, then checked by its id; and again after it is
  reserved (leased)
- **THEN** `classify` SHALL return `JobState::Live` in both cases

#### Scenario: Dead-lettered job is reported dead-lettered
- **WHEN** a job is reserved and failed, then checked by its id
- **THEN** `classify` SHALL return `JobState::DeadLettered`

#### Scenario: Acked job is not live and not dead-lettered
- **WHEN** a job is reserved and acked, then checked by its id
- **THEN** `classify` SHALL return `JobState::CompletedOrUnknown`

#### Scenario: Unknown id is reported completed or unknown
- **WHEN** an id that was never enqueued is checked
- **THEN** `classify` SHALL return `JobState::CompletedOrUnknown`

#### Scenario: Requeued job is live again
- **WHEN** a dead-lettered job is requeued, then checked by its id
- **THEN** `classify` SHALL return `JobState::Live`

#### Scenario: Check is non-destructive
- **WHEN** a dead-lettered job's id is checked and then the lane is read with
  `read_dead_letters`
- **THEN** the read SHALL still return the record, and a re-check SHALL still
  return `JobState::DeadLettered`

### Requirement: Requeue from dead-letter

A `DeadLetterStore` SHALL move a dead-lettered job identified by `JobId` back to its
original lane as a visible job, preserving every envelope field, and removing it
from the dead-letter store, only when such a record exists and no live job
already holds the same `JobId`. The requeued job's `attempts` SHALL be preserved
unchanged (the broker imposes no retry policy). A requeue for a `JobId` with no
dead-letter record SHALL fail without side effects.

If the dead-lettered job carried a `unique_key`, requeue SHALL re-acquire that
key for the revived job. Because the key was released when the job was
dead-lettered, another live job MAY hold it by the time of requeue; in that case
requeue SHALL fail with `Error::UniqueKeyHeld` and without side effects. If
another live job already holds the dead-lettered job's `JobId`, requeue SHALL
fail with `Error::LiveJobIdConflict` and without side effects.

#### Scenario: Requeue makes a job reservable again

- **WHEN** a dead-lettered job is requeued
- **THEN** a `reserve` on its original lane SHALL be able to return it
- **AND** it SHALL no longer appear in the dead-letter store

#### Scenario: Requeue preserves the opaque envelope

- **WHEN** a dead-lettered job is requeued and later reserved
- **THEN** the reserved envelope's `payload` bytes, `kind`, and `max_attempts`
  SHALL equal the original values

#### Scenario: Requeue of an unknown job is rejected

- **WHEN** requeue is called with a `JobId` that has no dead-letter record
- **THEN** the broker SHALL reject it
- **AND** it SHALL NOT change any stored job or dead-letter record

#### Scenario: Requeue re-acquires a free unique key

- **WHEN** a dead-lettered job that carried a `unique_key` is requeued and no live
  job currently holds that key
- **THEN** the requeued job SHALL re-acquire the key
- **AND** a subsequent enqueue with that key SHALL deduplicate to the requeued job

#### Scenario: Requeue is rejected when the unique key is held

- **WHEN** a dead-lettered job that carried a `unique_key` is requeued but another
  live job now holds that key
- **THEN** requeue SHALL fail with `Error::UniqueKeyHeld`
- **AND** it SHALL NOT change the dead-lettered job or the live key holder

#### Scenario: Concurrent unique-key race is conflict-safe

- **WHEN** a requeue races with another enqueue for the same unique key
- **THEN** at most one live job SHALL hold that unique key
- **AND** the losing operation SHALL return the broker's unique-key conflict or
  deduplication outcome without corrupting either store

#### Scenario: Requeue is rejected when the JobId is live

- **WHEN** a dead-lettered job is requeued but another live job already holds the
  same `JobId`
- **THEN** requeue SHALL fail with `Error::LiveJobIdConflict`
- **AND** it SHALL NOT change the dead-lettered job or the live job

### Requirement: Purge dead-letters

A `DeadLetterStore` SHALL permanently remove all dead-letter records for a given lane and
return how many were removed. The purge SHALL be lane-scoped (records for other
lanes are untouched) and irreversible (removed records are not requeued). It
bounds the otherwise-unbounded growth of the dead-letter store. Purging a lane
with no dead-letter records SHALL remove nothing and return zero.

#### Scenario: Purge removes a lane's dead-letters

- **WHEN** a lane holds dead-letter records and `purge_dead_letters` is called for it
- **THEN** it SHALL return the number removed
- **AND** a subsequent count and read of that lane SHALL be empty

#### Scenario: Purge is lane-scoped

- **WHEN** two lanes hold dead-letter records and one is purged
- **THEN** the other lane's dead-letter records SHALL be unchanged

#### Scenario: Purge of an empty lane removes nothing

- **WHEN** `purge_dead_letters` is called for a lane with no dead-letter records
- **THEN** it SHALL return zero and change nothing

### Requirement: Lease extension

`extend(receipt)` SHALL re-apply the broker's visibility lease to the job
currently held under `receipt`, keeping it invisible to other `reserve` calls
for a fresh lease measured from the current time, only when the receipt is the
current valid receipt for the job's active reservation. A receipt that is
unknown, superseded, or whose lease has already expired SHALL be rejected with a
stale-reservation error, and the broker MUST NOT change the job's lease,
schedule, or visibility due to that stale receipt. `extend` SHALL NOT change the
job's `attempts`. The lease duration is owned by the broker (as for `reserve`);
`extend` takes no caller-supplied duration.

#### Scenario: Extend holds the job past its original lease

- **WHEN** a reserved job is extended with its current receipt before its lease
  expires, and the clock then advances past the original lease but within the
  re-applied lease
- **THEN** a `reserve` on that lane SHALL NOT return the job
- **AND** the job SHALL still be resolvable (ack / retry / fail) with that receipt

#### Scenario: Extend after lease expiry rejected

- **WHEN** a reserved job's lease expires before it is extended
- **THEN** extending with the expired receipt SHALL fail with a stale-reservation error
- **AND** the job SHALL remain available for a current reservation
- **AND** its `attempts` and schedule SHALL be unchanged by the rejected extend

#### Scenario: Superseded receipt cannot extend

- **WHEN** a reserved job's lease expires, the job is reserved again, and the
  first receipt is used to extend
- **THEN** the extend SHALL fail with a stale-reservation error
- **AND** the current reservation SHALL remain valid and its lease unchanged

### Requirement: Backend-agnostic payloads

The broker SHALL operate only on opaque envelopes and MUST NOT depend on Rust
handler types or inspect payload contents. It SHALL also **preserve** every
envelope field — `id`, `lane`, `kind`, the opaque `payload` bytes, `attempts`,
and `max_attempts` — unchanged across storage and retrieval, returning them
identical from a subsequent `reserve` and in any dead-letter record. An
in-memory broker satisfies this by retaining the value; a durable broker
satisfies it by faithfully reconstructing the envelope from its storage. The
broker MUST NOT alter, re-encode, reorder, or truncate the `payload` bytes.

#### Scenario: Opaque handling

- **WHEN** any broker operation processes a job
- **THEN** it SHALL use only envelope fields (`id`, `lane`, `kind`, `payload`
  bytes, `attempts`, `max_attempts`)
- **AND** it MUST NOT deserialize the payload

#### Scenario: Payload bytes survive a storage round-trip verbatim

- **WHEN** a job whose payload is arbitrary (including non-UTF-8) bytes is
  enqueued and later reserved
- **THEN** the reserved envelope's `payload` SHALL equal the enqueued bytes
  exactly, with no alteration, re-encoding, reordering, or truncation
- **AND** its `kind` and `max_attempts` SHALL also equal the enqueued values
- **AND** its `attempts` SHALL equal the number of prior retries (zero on first
  reservation)

### Requirement: Injectable time source

A broker SHALL derive all time-based decisions (job visibility, lease expiry, and
retry scheduling) from an injectable clock rather than reading wall-clock time
directly, so that its lease and visibility semantics are deterministic and
portable across deployments and verifiable by the shared contract suite.

#### Scenario: Visibility advances by injected time

- **WHEN** a broker is constructed with a clock and that clock is advanced past a
  reserved job's lease without an intervening ack, retry, or fail
- **THEN** the job SHALL become reservable again
- **AND** this transition SHALL depend on the injected clock, not on wall-clock time

#### Scenario: Scheduled visibility tracks injected time

- **WHEN** a job is retried with a delay and the injected clock is advanced by
  that delay
- **THEN** the job SHALL become reservable again, independent of wall-clock time

### Requirement: Restart-durable time for persisted jobs

A broker that persists jobs across process restarts SHALL derive its visibility
and lease times from a clock whose epoch is stable across restarts, so that a
persisted job's `available_at` and lease deadline remain meaningful after the
process restarts and reopens the same storage. A broker whose jobs do not
survive a restart (for example an in-memory broker) has no such obligation and
MAY use a process-local monotonic clock.

Such a restart-stable clock SHALL be **monotonic non-decreasing** for the
lifetime of the broker instance: a backward wall-clock adjustment (e.g. an NTP
correction) SHALL NOT make the clock return a value below one it has already
returned. This prevents a backward step from reordering the `available_at` /
lease keys derived from it or re-hiding in-flight work. (A forward step still
advances the clock — required to follow real time for durability — and MAY
expire an in-flight lease early, which is the duplicate delivery the
at-least-once contract already permits.)

#### Scenario: A backward clock step does not move time backward

- **WHEN** the underlying wall clock steps backward after the broker has already
  observed a later time
- **THEN** the broker's clock SHALL NOT return a value earlier than the latest it
  has already returned

#### Scenario: Persisted jobs survive a restart

- **WHEN** a job is enqueued to a persistent broker and then the broker is
  restarted (the same storage reopened by a new broker instance with a fresh
  clock of the same kind)
- **THEN** the job SHALL still be reservable after the restart

#### Scenario: A persisted retry delay is honoured across a restart

- **WHEN** a job is retried with a future visibility delay and the broker is then
  restarted before the delay elapses
- **THEN** after the restart the job SHALL remain hidden until the delay elapses
  and become reservable thereafter, consistent with its pre-restart schedule

### Requirement: Schema-stable persistence across upgrades

A broker that persists jobs across process restarts SHALL never silently read
storage written under a schema version it does not natively support.

worklane is pre-1.0 and has no stable on-disk format yet: there is **no in-place
migration**. When a persistent broker opens storage stamped with any schema
version other than the one it natively supports — older or newer — it SHALL
reject the open with a clear error rather than read the storage under its current
assumptions. Crossing a schema boundary is an operational reset — drop and
recreate (SQLite, Postgres) or drain and re-enqueue (Redis) — not an in-place
migration. In-place migration becomes a requirement at 1.0, when the on-disk
format is frozen.

A broker whose jobs do not survive a restart (for example an in-memory broker)
has no such obligation.

#### Scenario: A fresh store opens at the baseline

- **WHEN** a persistent broker opens empty or uninitialized storage
- **THEN** it SHALL create its baseline schema and open successfully
- **AND** a job enqueued afterwards SHALL be reservable

#### Scenario: Storage from a different schema version is rejected

- **WHEN** a persistent broker opens storage stamped with a schema version it
  does not natively support (whether older or newer)
- **THEN** the open SHALL fail with a clear error rather than read the storage
  under the current version's assumptions

### Requirement: fail enforces the configured retention policy

When a broker is configured with a `RetentionPolicy`, the `fail` operation SHALL,
after writing the new dead-letter record, prune the failing job's lane to satisfy
the policy's `max_count` and `max_age` bounds in the same operation. When no
policy is configured, `fail` SHALL behave exactly as without this capability and
SHALL NOT remove any existing dead-letter record.

#### Scenario: fail prunes under a configured policy

- **WHEN** a broker configured with `max_count = 1` dead-letters two jobs on a
  lane
- **THEN** after the second `fail` the lane SHALL hold exactly one dead-letter
  record (the second)

#### Scenario: fail without a policy removes nothing

- **WHEN** a broker with no retention policy dead-letters a job on a lane that
  already holds dead-letter records
- **THEN** all prior dead-letter records SHALL remain
- **AND** the new record SHALL be added

### Requirement: Bounded redelivery

A broker SHALL support being configured with an optional maximum delivery count,
`max_deliveries`, and SHALL track, per job, a **delivery count**: the number of
times the job has been delivered (reserved and returned to a caller). The
delivery count is distinct from `attempts` — `attempts` counts handler failures
(advanced only by `retry`/`fail`), whereas the delivery count advances on every
reservation, even when the caller never resolves the job (for example a worker
process that crashes before acking, retrying, or failing). The delivery count is
broker-internal: it is not part of the `JobEnvelope` and need not be exposed to
handlers.

When `max_deliveries` is configured, on a reservation that would make a job's
delivery count exceed `max_deliveries`, the broker SHALL NOT return the job;
instead it SHALL move the job to the dead-letter store — releasing any
`unique_key` exactly as `fail` does, and retaining an error indicating the
delivery bound was exceeded — and SHALL continue selecting the next eligible job
on the lane. Otherwise the broker SHALL record the incremented delivery count and
return the job. A job is therefore delivered at most `max_deliveries` times, then
dead-lettered on the next reservation. The increment, the bound check, and the
dead-letter-and-skip SHALL be atomic with the reservation.

When `max_deliveries` is not configured (the default), the broker SHALL NOT
dead-letter a job on its delivery count: redelivery is unbounded and behavior is
exactly as if this requirement were absent.

#### Scenario: A repeatedly-redelivered job is dead-lettered after the bound

- **WHEN** a broker is configured with `max_deliveries = N` and a job's lease
  expires `N` times without the job being acked, retried, or failed (as a
  crashed worker process would leave it)
- **THEN** the job SHALL have been delivered `N` times
- **AND** the next reservation SHALL move the job to the dead-letter store with a
  delivery-bound error rather than returning it
- **AND** that reservation SHALL return the next eligible job on the lane, or
  `None` if there is none

#### Scenario: A unique key is released when the delivery bound dead-letters a job

- **WHEN** a job carrying a `unique_key` is dead-lettered for exceeding
  `max_deliveries`
- **THEN** the key SHALL be released (as on `fail`), so a new enqueue with that
  key creates a new job
- **AND** the dead-letter record SHALL retain the key for a later `requeue`

#### Scenario: Redelivery is unbounded by default

- **WHEN** no `max_deliveries` is configured and a job's lease repeatedly expires
  without resolution
- **THEN** the broker SHALL keep redelivering the job and SHALL NOT dead-letter
  it on a delivery count

### Requirement: Core broker contract is the job lifecycle

The `Broker` contract SHALL consist of the job-lifecycle operations only:
enqueue, batch enqueue, reserve, acknowledge, retry, defer, extend, fail, and
job-state classification. Dead-letter inspection/maintenance, queue-depth
statistics, and scheduled enqueue SHALL NOT be part of the core contract; they
are optional capabilities (see "Optional broker capabilities are discovered
through accessors"). A type that implements the lifecycle operations SHALL be a
valid `Broker` even if it provides no optional capability.

#### Scenario: A lifecycle-only broker is valid

- **WHEN** a backend implements only the job-lifecycle operations and no optional
  capability
- **THEN** it SHALL satisfy the `Broker` contract
- **AND** the worker core loop (reserve, dispatch, ack/retry/fail/dead-letter)
  SHALL operate against it unchanged

#### Scenario: Classification stays in the core contract

- **WHEN** a caller holds any `Broker`
- **THEN** job-state classification SHALL be available directly on the broker
  without negotiating a capability

### Requirement: Optional broker capabilities are discovered through accessors

A `Broker` SHALL expose its optional capabilities through accessor methods that
return an optional capability handle, defaulting to absent. The capabilities and
their accessors SHALL be:

- dead-letter inspection/maintenance (read, count, purge, requeue) via a
  `dead_letter_store` accessor returning an optional borrow of a `DeadLetterStore`;
- queue-depth statistics (pending count) via a `queue_stats` accessor returning
  an optional borrow of a `QueueStats`;
- scheduled enqueue via a `scheduled_store` accessor returning an optional owned
  handle to a `ScheduledStore`.

A broker that does not support a capability SHALL return absent from that
accessor and SHALL remain a valid broker. A broker that supports a capability
SHALL return a handle whose behavior conforms to the corresponding dead-letter,
queue-stats, or scheduled-enqueue requirements in this specification.

#### Scenario: A supported capability is advertised

- **WHEN** a broker supports dead-letter inspection
- **THEN** its `dead_letter_store` accessor SHALL return a present handle
- **AND** operations on that handle SHALL behave as the dead-letter read, count,
  requeue, and purge requirements specify

#### Scenario: An unsupported capability is absent

- **WHEN** a broker does not support a given optional capability
- **THEN** the corresponding accessor SHALL return absent
- **AND** the broker SHALL still satisfy the core `Broker` contract

#### Scenario: Scheduled enqueue is acquired through the broker

- **WHEN** a recurring scheduler is constructed from a broker that supports
  scheduled enqueue
- **THEN** it SHALL obtain the scheduled-store handle from the broker's
  `scheduled_store` accessor
- **AND** atomic scheduled enqueue SHALL behave as the scheduled-enqueue
  requirements specify

#### Scenario: First-party brokers expose their capabilities

- **WHEN** any first-party broker (in-memory, SQLite, Postgres, Redis) is asked
  for dead-letter inspection, queue statistics, and scheduled enqueue
- **THEN** each accessor SHALL return a present handle

