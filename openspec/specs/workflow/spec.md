# Workflow

## Purpose

User-space orchestration library over the core broker, supporting idempotent
continuation, parallel fan-out, and fan-in aggregations via persistent result
stores and live-window unique keys.

## Requirements

### Requirement: Idempotent Continuation (Sequence)

The system SHALL provide an extension on the facade `Client` to safely enqueue a
continuation job from within an executing handler. The system SHALL expose this
as `build_continuation::<J>(ctx, payload)`, which returns a `JobBuilder` the
caller MAY mutate before calling `.enqueue()`. The continuation MUST use the
`unique_key` primitive derived from the parent job's ID, such as
`sequence:{parent_id}:{next_kind}`, to provide live-window deduplication, and by
default inherits the parent's lane. The system MUST acknowledge that this
provides at-least-once, best-effort guarantees, meaning the continuation job
MUST be idempotent.

#### Scenario: Successful continuation

- **WHEN** Job A completes and calls
  `build_continuation::<JobB>(ctx, payload)?.enqueue()`
- **THEN** Job B is enqueued with a unique key derived from Job A's ID

#### Scenario: Redelivery window deduplication

- **WHEN** Job A completes, enqueues Job B, but crashes before acknowledging,
  and is redelivered while Job B is still live
- **THEN** the second `build_continuation::<JobB>(...)?.enqueue()` for Job B is
  successfully deduplicated by the broker

#### Scenario: Out-of-window double execution

- **WHEN** Job A crashes, Job B completes and releases its unique key, and Job A
  is redelivered later
- **THEN** Job B is enqueued and executed a second time

### Requirement: Callback Keying

The system SHALL provide a mechanism,
`build_continuation_keyed::<J>(key, payload)`, for callers to explicitly provide
an idempotency key for continuation. This is REQUIRED when the executing job's ID
is not a stable anchor across retries or generations.

The fan-in callback enqueued under the stable `fanin:{fanin_id}:callback` key
SHALL be at-least-once, not exactly-once, and callers MUST make it idempotent.
The key deduplicates only within the live window, as every `unique_key` does. It
is released once the callback job completes, so a watcher generation that is
redelivered after the callback has already run and acked MAY enqueue the callback
a second time. The system MUST NOT claim the callback fires exactly once, and the
callback handler MUST tolerate running more than once.

#### Scenario: Firing fan-in callback

- **WHEN** a watcher job generation calls `build_continuation_keyed` with a
  stable `fanin:{fanin_id}:callback` key and enqueues it
- **THEN** the callback job is enqueued using the explicitly provided key

#### Scenario: Callback is at-least-once across watcher redelivery

- **WHEN** a watcher generation enqueues the callback under the stable callback
  key, the callback runs and acks, and that same watcher generation is then
  redelivered and again observes all dependencies captured
- **THEN** the watcher MAY enqueue the callback a second time, and the system
  SHALL NOT guarantee the callback ran exactly once
- **AND** the callback handler is REQUIRED to be idempotent so a second run is safe

### Requirement: Fan-In Watcher Self-Rescheduling

The system SHALL implement fan-in aggregation using a self-rescheduling
`FanInWatcherJob` that captures each dependency's output from a `ResultStore`
and delivers the aggregated outputs to the callback. The watcher MUST NOT fail
or `RetryLater` when dependencies are merely incomplete; it MUST enqueue a
next-generation watcher via `enqueue_in(delay)` and successfully acknowledge
itself to avoid dead-letter queue pollution.

Capture SHALL be monotonic across generations: once a dependency's result value
has been captured from the `ResultStore`, that value SHALL be carried forward in
the watcher payload for the remainder of the fan-in's lifetime, and subsequent
generations SHALL poll only the dependencies not yet captured. A result-store
eviction of an already-captured dependency's result MUST NOT regress the fan-in.
The callback SHALL fire once every dependency's value has been captured.

For each dependency whose value is not yet captured, the watcher SHALL classify
the dependency against the broker's durable, retention-independent by-id state
before reading the `ResultStore`:

- If the broker classifies it `Live`, it is still pending and is carried
  forward.
- Else if the broker classifies it `DeadLettered`, the watcher SHALL fail the
  fan-in promptly with an error naming the failed dependency.
- Else the broker classifies it `CompletedOrUnknown`; the watcher SHALL read its
  result from the `ResultStore`.
- If the completed result is present, the watcher SHALL capture the value and
  treat the dependency as done.
- If the completed result is absent, the dependency completed but its result was
  evicted before capture; the watcher SHALL fail the fan-in with an error naming
  the dependency.

When every dependency's value has been captured, the watcher SHALL build a
`FanInResults<C>` payload, containing the caller's context `C` plus each
dependency's opaque output bytes in dependency order, and enqueue the callback
under a stable `fanin:{fanin_id}:callback` key. The callback is therefore a
fan-in and an aggregation: it receives the dependency outputs, not merely a
signal that they completed. A captured value carried forward in the payload is
immune to later eviction; only a value evicted before any capture fails the
fan-in.
The serialized wire shape of that payload SHALL be stable and verified by a
round-trip test because the watcher builds it without knowing `C`.

#### Scenario: Incomplete dependencies

- **WHEN** a fan-in watcher job runs and some dependency's value is not yet
  captured
- **AND** the broker classifies that dependency as `Live`
- **THEN** it enqueues a next-generation watcher carrying the already-captured
  values and the still-pending dependencies forward
- **AND** it acks itself

#### Scenario: All values captured

- **WHEN** a fan-in watcher job runs and every dependency's value is captured
- **THEN** it builds `FanInResults<C>` and enqueues the callback under the stable
  callback key
- **AND** it acks itself

#### Scenario: FanInResults wire shape round-trips

- **WHEN** the watcher builds the callback payload without knowing the caller's
  context type
- **THEN** the payload SHALL deserialize as `FanInResults<C>` for the callback's
  context type
- **AND** dependency outputs SHALL remain in dependency order

#### Scenario: Already-captured dependency is evicted before siblings finish

- **WHEN** a dependency's result is captured in one generation, then evicted from
  the `ResultStore` before a slower sibling completes
- **THEN** the watcher SHALL NOT re-poll or re-require the evicted dependency,
  and SHALL retain its captured value
- **AND** the fan-in SHALL still complete and deliver that value once the
  remaining dependencies are captured

#### Scenario: Dead-lettered dependency fails the fan-in fast

- **WHEN** a dependency exhausts its retries and is dead-lettered
- **THEN** the watcher SHALL classify it `DeadLettered` and fail the fan-in
  promptly
- **AND** the failure SHALL name the dead-lettered dependency

#### Scenario: A result evicted before capture fails the fan-in

- **WHEN** a dependency completed but its result is absent from the `ResultStore`
  before the watcher ever captured it, and the broker classifies it
  `CompletedOrUnknown`
- **THEN** the watcher SHALL fail the fan-in with an error naming the dependency

#### Scenario: Stale result for live dependency is ignored

- **WHEN** a dependency has bytes in the `ResultStore`
- **AND** the broker classifies that dependency as `Live`
- **THEN** the watcher SHALL NOT capture those bytes
- **AND** the dependency SHALL be carried forward as incomplete

### Requirement: Fan-in dependency identity is causally guaranteed

A fan-in submitted through the facade `Client::fan_in` SHALL guarantee that every
dependency id recorded in the watcher payload denotes a job that was actually
persisted. To this end, `fan_in` SHALL reject any dependency that carries a
`unique_key` — returning an error and submitting nothing — because the atomic
batch enqueue deduplicates `unique_key` collisions and could otherwise drop a
dependency while its id still rides in the watcher payload. With this guarantee,
the watcher's treatment of a `CompletedOrUnknown` classification as "completed"
is causally sound: for a dependency originating from `fan_in`, `CompletedOrUnknown`
can only mean the job was acked, never that it was never enqueued.

#### Scenario: A dependency carrying a unique_key is rejected

- **WHEN** `Client::fan_in` is called with a dependency that carries a `unique_key`
- **THEN** it SHALL return an error and submit nothing (neither the dependencies
  nor the watcher are enqueued)

#### Scenario: Dependencies without unique keys are submitted with the watcher

- **WHEN** `Client::fan_in` is called with dependencies that carry no `unique_key`
- **THEN** the dependencies and the watcher SHALL be submitted, and each
  dependency id recorded in the watcher payload SHALL denote one of the persisted
  dependency jobs

### Requirement: Fan-In Watcher Payload Validation

`FanInWatcherPayload` is a serialized internal job payload and MUST NOT be
treated as a trust boundary. A watcher job SHALL validate its payload invariants
before reading dependency results, rescheduling itself, or enqueueing the
callback.

#### Scenario: Valid watcher payload runs

- **WHEN** a watcher payload has non-empty unique dependencies, valid callback
  metadata, a positive generation, and captured values for a subset of its
  dependencies
- **THEN** the watcher SHALL continue with normal dependency classification

#### Scenario: Forged payload with no dependencies fails

- **WHEN** a watcher payload contains no dependencies
- **THEN** the watcher SHALL fail the job as malformed
- **AND** it SHALL NOT enqueue a callback

#### Scenario: Captured unknown dependency fails

- **WHEN** a watcher payload contains a captured value whose dependency ID is
  not listed in the dependency set
- **THEN** the watcher SHALL fail the job as malformed
- **AND** it SHALL NOT use that captured value

#### Scenario: Duplicate dependencies fail

- **WHEN** a watcher payload repeats a dependency ID
- **THEN** the watcher SHALL fail the job as malformed
- **AND** it SHALL NOT enqueue a next-generation watcher
