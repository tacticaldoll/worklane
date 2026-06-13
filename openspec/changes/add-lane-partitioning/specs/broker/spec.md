## ADDED Requirements

### Requirement: Lane-scoped reserve

`reserve(lane)` SHALL only return jobs that were enqueued to that lane. A job on
one lane MUST NOT be returned by a `reserve` on a different lane. Lanes are
arbitrary strings with no registration; a lane that no worker reserves SHALL
retain its jobs indefinitely, which is a deliberate operator responsibility.

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

## MODIFIED Requirements

### Requirement: Enqueue

The broker SHALL accept a `NewJob` and store it as a visible `JobEnvelope` with a
freshly assigned `JobId`, the lane carried by the `NewJob`, and `attempts = 0`,
returning the `JobId`.

#### Scenario: Enqueue makes a job reservable

- **WHEN** a job is enqueued to a lane
- **THEN** a `reserve` on that lane SHALL be able to return it

#### Scenario: Stored envelope retains its lane

- **WHEN** a job is enqueued to lane `"critical"`
- **THEN** the stored envelope SHALL carry lane `"critical"`
- **AND** that lane SHALL be preserved through reservation and dead-lettering

### Requirement: Backend-agnostic payloads

The broker SHALL operate only on opaque envelopes and MUST NOT depend on Rust
handler types or inspect payload contents.

#### Scenario: Opaque handling

- **WHEN** any broker operation processes a job
- **THEN** it SHALL use only envelope fields (`id`, `lane`, `kind`, `payload`
  bytes, `attempts`, `max_attempts`)
- **AND** it MUST NOT deserialize the payload
