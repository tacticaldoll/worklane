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

The system SHALL represent an enqueued job as a `JobEnvelope` carrying a unique
`JobId`, the job `kind`, an opaque serialized `payload` as bytes, the current
`attempts`, and `max_attempts`. The broker SHALL treat the payload as opaque and
MUST NOT depend on Rust handler types.

#### Scenario: Envelope carries identity and payload

- **WHEN** a job is enqueued
- **THEN** its envelope SHALL have a unique `JobId`, the job's `kind`, the
  serialized payload bytes, `attempts = 0`, and the configured `max_attempts`

#### Scenario: Distinct identities

- **WHEN** two jobs are enqueued
- **THEN** they SHALL have different `JobId` values

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
