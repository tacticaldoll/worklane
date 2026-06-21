# Job Model Specification

## Purpose

Defines how a background job is declared and represented: the typed `Job` trait,
the opaque `JobEnvelope` the broker stores, and payload (de)serialization.

## Requirements

### Requirement: Typed job definition

A job SHALL be defined by implementing the `Job` trait, which declares an
associated serde-serializable `Payload`, a unique `KIND` string identifier, and
an async `run` method that consumes the payload.

#### Scenario: Defining a job

- **WHEN** a type implements `Job` with `KIND = "send_email"` and a serde `Payload`
- **THEN** it can be enqueued and dispatched to its handler by that kind

### Requirement: Opaque job envelope

The system SHALL represent an enqueued job as a `JobEnvelope` carrying a
unique `JobId`, the `lane` it was enqueued to, the job `kind`, an opaque
serialised `payload` as bytes, the job's `priority`, the current `attempts`,
`max_attempts`, and an optional `trace_context` map
(`Option<HashMap<String, String>>`) for distributed trace propagation.
The broker SHALL treat the payload and `trace_context` as opaque and MUST NOT
depend on Rust handler types. The `JobContext` passed to the handler SHALL
expose this full metadata — including `kind`, `priority`, and the
`trace_context` map — to support advanced handler capabilities such as reading
or forwarding the propagation headers without re-parsing the envelope.

#### Scenario: Envelope carries identity and payload

- **WHEN** a job is enqueued
- **THEN** its envelope SHALL have a unique `JobId`, the `lane` it was enqueued
  to, the job's `kind`, the serialized payload bytes, the specified `priority`,
  `attempts = 0`, the configured `max_attempts`, and `trace_context` set to
  whatever the caller injected (or `None` if not injected)

#### Scenario: Distinct identities

- **WHEN** two jobs are enqueued
- **THEN** they SHALL have different `JobId` values

#### Scenario: JobContext exposes the full envelope metadata

- **WHEN** a handler runs and receives its `JobContext`
- **THEN** the `JobContext` SHALL expose the job's `kind`, `priority`, and
  `trace_context` (the same value carried on the envelope, or `None` if the
  caller injected no trace context)

### Requirement: Cooperative cancellation signal

A `JobContext` SHALL expose an **advisory** cooperative-cancellation signal
(`is_cancelled`). The worker SHALL set it when it abandons the job's reservation
lease — the lease was lost (a heartbeat came back stale, so the job will be
redelivered) or the handler exceeded its timeout. A handler MAY observe it at
safe points and return early to avoid doing work that will be thrown away.
Ignoring it SHALL be safe: delivery is at-least-once regardless, and a handler
that never checks it behaves identically to one in a worker that does not signal
cancellation.

#### Scenario: Cancellation is signalled on lease loss

- **WHEN** a handler is running and the worker detects its reservation lease was
  lost (a heartbeat is rejected as stale)
- **THEN** the handler's `JobContext` SHALL report cancelled via `is_cancelled`
- **AND** ignoring the signal SHALL remain safe — the job is redelivered under
  at-least-once delivery

### Requirement: Payload serialization

The client SHALL serialize a job's typed payload to bytes, and the worker SHALL
deserialize the payload for the matching job kind. A deserialization failure
SHALL be reported as an error and MUST NOT panic.

#### Scenario: Round-trip

- **WHEN** a payload is serialized at enqueue and deserialized at dispatch
- **THEN** the worker SHALL receive a value equal to the originally enqueued payload

#### Scenario: Corrupt payload

- **WHEN** a payload cannot be deserialized for its kind
- **THEN** the job SHALL be failed with a serialization error rather than panic
