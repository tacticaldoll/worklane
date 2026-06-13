## MODIFIED Requirements

### Requirement: Opaque job envelope

The system SHALL represent an enqueued job as a `JobEnvelope` carrying a unique
`JobId`, the `lane` it was enqueued to, the job `kind`, an opaque serialized
`payload` as bytes, the current `attempts`, and `max_attempts`. The broker SHALL
treat the payload as opaque and MUST NOT depend on Rust handler types.

#### Scenario: Envelope carries identity and payload

- **WHEN** a job is enqueued
- **THEN** its envelope SHALL have a unique `JobId`, the `lane` it was enqueued
  to, the job's `kind`, the serialized payload bytes, `attempts = 0`, and the
  configured `max_attempts`

#### Scenario: Distinct identities

- **WHEN** two jobs are enqueued
- **THEN** they SHALL have different `JobId` values
