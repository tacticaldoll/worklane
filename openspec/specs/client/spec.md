# Client Specification

## Purpose

Defines the enqueue side: how a client serializes a typed job payload and submits
it to the broker.

## Requirements

### Requirement: Typed enqueue

The client SHALL enqueue a job by serializing its typed payload and submitting a
`NewJob` (lane, kind, payload bytes, `max_attempts`) to the broker, returning the
assigned `JobId`. The client SHALL submit jobs to its configured lane, which
defaults to `"default"` and MAY be set with `with_lane`.

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
