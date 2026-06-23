## MODIFIED Requirements

### Requirement: Core broker contract is the job lifecycle

The `Broker` contract SHALL consist of the job-lifecycle operations only:
enqueue, reserve, acknowledge, retry, defer, extend, fail, and job-state
classification. Batch enqueue, dead-letter inspection/maintenance, queue-depth
statistics, and scheduled enqueue SHALL NOT be part of the core contract; they
are optional capabilities (see "Optional broker capabilities are discovered
through accessors"). A type that implements the lifecycle operations SHALL be a
valid `Broker` even if it provides no optional capability.

Each core operation SHALL preserve the existing lifecycle semantics for
visibility, reservation receipts, stale resolution, attempts, dead-lettering,
uniqueness, lanes, priority, and opaque envelopes. Operations that are not
required to run the lifecycle loop SHALL NOT be required by the core contract.

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

#### Scenario: Core lifecycle implementation is sufficient

- **WHEN** a broker implements the core lifecycle contract and no optional
  capability traits
- **THEN** a client SHALL be able to enqueue a job
- **AND** a worker SHALL be able to reserve, run, ack, retry, defer, extend, or
  fail that job according to the existing lifecycle semantics
- **AND** the broker SHALL be eligible to run the mandatory lifecycle
  conformance suite

#### Scenario: Optional inspection is absent

- **WHEN** a broker implements the core lifecycle contract but not dead-letter
  inspection
- **THEN** it SHALL still be a valid lifecycle broker
- **AND** code requiring dead-letter inspection SHALL detect that the capability
  is absent instead of assuming the core broker provides it

#### Scenario: Lifecycle semantics are unchanged

- **WHEN** a first-party broker is migrated to the split contract
- **THEN** its enqueue, reserve, ack, retry, defer, extend, fail, and classify
  behavior SHALL remain compatible with the existing broker requirements
- **AND** its conformance tests for those lifecycle scenarios SHALL still pass

### Requirement: Optional broker capabilities are discovered through accessors

A `Broker` SHALL expose its optional capabilities through accessor methods that
return an optional capability handle, defaulting to absent. The capabilities and
their accessors SHALL be:

- atomic batch enqueue via a `batch_enqueue` accessor returning an optional
  borrow of a `BatchEnqueue`;
- dead-letter inspection/maintenance (read, count, purge, requeue) via a
  `dead_letter_store` accessor returning an optional borrow of a `DeadLetterStore`;
- queue-depth statistics (pending count) via a `queue_stats` accessor returning
  an optional borrow of a `QueueStats`;
- scheduled enqueue via a `scheduled_store` accessor returning an optional owned
  handle to a `ScheduledStore`.

A broker that does not support a capability SHALL return absent from that
accessor and SHALL remain a valid broker. A broker that supports a capability
SHALL return a handle whose behavior conforms to the corresponding batch-enqueue,
dead-letter, queue-stats, or scheduled-enqueue requirements in this
specification. A consumer that needs an optional capability SHALL request it
through the accessor and SHALL fail predictably with an explicit
unsupported-capability result when the accessor returns absent.

#### Scenario: A supported capability is advertised

- **WHEN** a broker supports dead-letter inspection
- **THEN** its `dead_letter_store` accessor SHALL return a present handle
- **AND** operations on that handle SHALL behave as the dead-letter read, count,
  requeue, and purge requirements specify

#### Scenario: An unsupported capability is absent

- **WHEN** a broker does not support a given optional capability
- **THEN** the corresponding accessor SHALL return absent
- **AND** the broker SHALL still satisfy the core `Broker` contract

#### Scenario: A consumer requests an absent capability

- **WHEN** a consumer requests an optional capability from a broker that does not
  implement it
- **THEN** the consumer SHALL receive an explicit absence or unsupported
  capability result
- **AND** the consumer SHALL NOT infer support from the core lifecycle contract

#### Scenario: Optional capability does not change lifecycle behavior

- **WHEN** a broker adds or removes support for an optional capability
- **THEN** the core lifecycle semantics SHALL remain unchanged
- **AND** mandatory lifecycle conformance SHALL NOT depend on that optional
  capability

#### Scenario: Scheduled enqueue is acquired through the broker

- **WHEN** a recurring scheduler is constructed from a broker that supports
  scheduled enqueue
- **THEN** it SHALL obtain the scheduled-store handle from the broker's
  `scheduled_store` accessor
- **AND** atomic scheduled enqueue SHALL behave as the scheduled-enqueue
  requirements specify

#### Scenario: First-party brokers expose their capabilities

- **WHEN** any first-party broker (in-memory, SQLite, Postgres, Redis) is asked
  for batch enqueue, dead-letter inspection, queue statistics, and scheduled
  enqueue
- **THEN** each accessor SHALL return a present handle

### Requirement: Atomic batch enqueue

Atomic batch enqueue SHALL be an optional broker capability rather than part of
the core lifecycle contract. A broker that provides it (its `batch_enqueue`
accessor returns present) SHALL provide an atomic `enqueue_batch` method that
accepts multiple jobs and MUST ensure all-or-nothing insertion. A broker MAY omit the
capability, in which case its `batch_enqueue` accessor SHALL return absent and a
batch operation SHALL fail predictably with an unsupported-capability result
rather than silently degrading.

#### Scenario: All jobs visible
- **WHEN** a batch of multiple jobs is successfully enqueued
- **THEN** all jobs in the batch MUST be immediately available for reservation

#### Scenario: Preservation of order
- **WHEN** a batch of jobs is enqueued
- **THEN** the returned `JobId` list MUST exactly match the input array's index order

#### Scenario: Batch enqueue on a broker without the capability
- **WHEN** a consumer requests batch enqueue from a broker whose `batch_enqueue`
  accessor returns absent
- **THEN** the operation SHALL return an explicit unsupported-capability result
- **AND** no jobs SHALL be persisted

### Requirement: Batch unique key deduplication

A broker that provides the batch-enqueue capability SHALL handle `unique_key`
deduplication within a batch without failing the batch insertion.

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

## ADDED Requirements

### Requirement: Portable broker contract changes

Any new broker core operation or required capability SHALL be justified against
both SQL-style and Redis-style implementations before implementation begins. The
change design SHALL record the portability argument and rejected alternatives.

#### Scenario: Proposed core operation is portable

- **WHEN** a change proposes a new required broker operation
- **THEN** its design SHALL explain how a SQL broker can implement it
- **AND** its design SHALL explain how a Redis broker can implement it
- **AND** implementation SHALL NOT begin until the portability argument is
  recorded

#### Scenario: Proposed operation is implementation-specific

- **WHEN** a proposed operation depends on live references, full in-memory scans,
  or synchronous visibility assumptions
- **THEN** it SHALL NOT be added to the required broker core
- **AND** it SHALL be rejected, kept backend-local, or exposed through a
  narrower optional capability with its own portability argument
