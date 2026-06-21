# Client Specification

## Purpose

Defines the enqueue side: how a client serializes a typed job payload and submits
it to the broker.
## Requirements
### Requirement: Multi-lane enqueue (Fan-out)

The client SHALL support dispatching the same typed job to multiple lanes
simultaneously via an `enqueue_to_lanes` method. It MUST serialize the payload
exactly once and dispatch all instances via a single atomic `enqueue_batch`
broker call.

#### Scenario: Successful fan-out
- **WHEN** a client calls `enqueue_to_lanes` with multiple lanes
- **THEN** the payload is serialized exactly once and the jobs are atomically
  submitted to the broker

### Requirement: Typed enqueue

The client SHALL enqueue a job by serializing its typed payload and submitting a
`NewJob` (lane, kind, payload bytes, `max_attempts`, delay, priority) to the
broker, returning the assigned `JobId`. The client SHALL provide a `JobBuilder`
interface that allows chaining job properties (lane, delay, unique key,
priority) before submission. Terminal methods on the builder MUST support
enqueuing to a single lane or fanning out to multiple lanes.

The client MAY retain existing convenience methods for simpler use cases. It
SHALL support submitting jobs to its configured lane, which defaults to
`"default"` and MAY be set with `with_lane`. The client SHALL support a delayed
enqueue via `enqueue_in(delay, payload)`, submitting a `NewJob` carrying that
delay; `enqueue` is equivalent to `enqueue_in` with a zero delay. The client
SHALL default to priority `0` but MAY allow callers to specify a custom
priority.

When no payload store is configured, the client SHALL reject an inline payload
larger than `worklane_core::spi::MAX_ENVELOPE_BYTES` before submitting it to the
broker. When a payload store is configured and an enqueue path offloads payload
bytes before broker submission, any later broker submission failure SHALL trigger
a best-effort attempt to delete the offloaded payload. When a unique-key enqueue
deduplicates to an existing live job, the client SHALL make a best-effort
attempt to delete the just-offloaded payload for the dropped job.

#### Scenario: Enqueue returns a job id

- **WHEN** `enqueue` is called with a typed job payload
- **THEN** the payload SHALL be serialized and a `NewJob` submitted to the broker
- **AND** the call SHALL return the new `JobId`

#### Scenario: Default lane

- **WHEN** a job is enqueued by a client whose lane has not been overridden
- **THEN** the submitted `NewJob` SHALL carry the lane `"default"`

#### Scenario: Configured lane

- **WHEN** a client configured with `with_lane("critical")` enqueues a job
- **THEN** the submitted `NewJob` SHALL carry the lane `"critical"`

#### Scenario: Default max attempts

- **WHEN** a job is enqueued without overriding `max_attempts`
- **THEN** the configured default `max_attempts` SHALL be used

#### Scenario: Serialization failure

- **WHEN** the payload cannot be serialized
- **THEN** `enqueue` SHALL return a serialization error
- **AND** it MUST NOT submit a job to the broker

#### Scenario: Oversized inline payload is rejected before submit

- **WHEN** an enqueue would carry an inline payload larger than the envelope cap
- **AND** no payload store is configured
- **THEN** the client SHALL return a serialization error
- **AND** it MUST NOT submit a job to the broker

#### Scenario: Broker failure cleans up offloaded payload

- **WHEN** an enqueue offloads its payload
- **AND** broker submission later fails
- **THEN** the client SHALL make a best-effort attempt to delete the offloaded
  payload
- **AND** it SHALL return the original broker error

#### Scenario: Deduplicated offload is cleaned up

- **WHEN** a unique-key enqueue offloads its payload
- **AND** the broker deduplicates that enqueue to an existing live job
- **THEN** the client SHALL make a best-effort attempt to delete the offloaded
  payload for the dropped job

#### Scenario: Delayed enqueue carries the delay

- **WHEN** `enqueue_in(delay, payload)` is called with a positive delay
- **THEN** the submitted `NewJob` SHALL carry that delay
- **AND** a plain `enqueue` SHALL submit a `NewJob` with a zero delay

#### Scenario: Default priority

- **WHEN** a job is enqueued without overriding priority
- **THEN** the submitted `NewJob` SHALL carry priority `0`

### Requirement: Per-call lane override

The client SHALL support enqueuing a job to a caller-specified lane for a single
call via `enqueue_to(lane, payload)`, overriding the client's configured lane for
that call only. A subsequent `enqueue` SHALL still use the client's configured
lane; `enqueue_to` SHALL NOT change it.

#### Scenario: Enqueue to an explicit lane

- **WHEN** `enqueue_to("critical", payload)` is called on a client configured for
  the default lane
- **THEN** the submitted `NewJob` SHALL carry the lane `"critical"`

#### Scenario: Configured lane is unchanged by an override

- **WHEN** a client enqueues via `enqueue_to("critical", …)` and then enqueues
  again via `enqueue`
- **THEN** the second job SHALL carry the client's configured lane, not
  `"critical"`

### Requirement: Registry-checked enqueue

The client SHALL support being configured with an optional `LaneRegistry`. When
no registry is configured, every enqueue path SHALL accept any well-formed lane,
exactly as without this capability. When a registry is configured, every enqueue
path — including the configured-lane enqueue, the per-call lane override, the
delayed enqueue, and the multi-lane fan-out — SHALL verify each target lane is a
member of the registry before submitting to the broker. If any target lane is
not a member, the call SHALL return `Error::UnknownLane` carrying the offending
lane name and MUST NOT submit any job to the broker.

#### Scenario: Enqueue to a registered lane succeeds

- **WHEN** a client configured with a registry containing `"email"` enqueues to
  lane `"email"`
- **THEN** the job SHALL be submitted to the broker
- **AND** the call SHALL return the new `JobId`

#### Scenario: Enqueue to an unregistered lane is rejected

- **WHEN** a client configured with a registry containing `"email"` enqueues to
  lane `"emial"`
- **THEN** the call SHALL return `Error::UnknownLane` naming `"emial"`
- **AND** it MUST NOT submit any job to the broker

#### Scenario: No registry preserves dynamic lanes

- **WHEN** a client with no registry configured enqueues to lane `"emial"`
- **THEN** the job SHALL be submitted to the broker exactly as today
- **AND** no `Error::UnknownLane` SHALL be returned

#### Scenario: Fan-out is all-or-nothing against the registry

- **WHEN** a client configured with a registry containing `"email"` (but not
  `"sms"`) fans a job out to lanes `"email"` and `"sms"`
- **THEN** the call SHALL return `Error::UnknownLane` naming `"sms"`
- **AND** no job SHALL be submitted to any lane, including `"email"`

### Requirement: Must-use Builders

Public builder values and chainable configuration methods SHALL be annotated so
the Rust compiler warns when callers ignore the returned configured value.

#### Scenario: Ignored builder result warns

- **WHEN** a caller invokes a chainable builder method and ignores its returned
  value
- **THEN** normal Rust linting SHALL be able to warn that the configured value
  was not used
