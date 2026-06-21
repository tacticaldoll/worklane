# Result Backend

## Purpose

Defines the pluggable result storage system for capturing outputs from completed
jobs. This enables higher-level orchestration patterns like Workflow Canvas
(Chords/Chains) and provides a type-safe way for clients to retrieve job
results, while keeping the core `Broker` trait isolated from KV-store duties.

## Requirements

### Requirement: Result Store Interface

The `ResultStore` trait SHALL provide methods to store and retrieve opaque bytes
keyed by a `JobId`. It MUST NOT depend on the `Broker` trait. Storage TTL
(Time-To-Live) SHALL be an implementation detail of the backend, not part of the
`store` API contract.

#### Scenario: Store and retrieve a result
- **WHEN** a result is stored for a `JobId`
- **THEN** calling `get` with the same `JobId` SHALL return the stored bytes

#### Scenario: Retrieving an unknown result
- **WHEN** `get` is called for a `JobId` with no stored result
- **THEN** the store SHALL return `None` (not an error, and not empty bytes)

### Requirement: Typed Job Output

The `Job` trait SHALL define a strongly-typed `Output` that is
serde-serializable. A job handler returning success SHALL return an instance of
this `Output`.

#### Scenario: Handler returns typed data
- **WHEN** a job executes successfully
- **THEN** its handler SHALL return an `Ok` result containing the defined `Output` type

### Requirement: Worker Egress Before Ack

The `Worker` SHALL optionally accept a configured `ResultStore`. If configured,
when a job completes successfully, the worker MUST serialize the job's `Output`
to bytes and persist it in the `ResultStore` *before* acknowledging (acking) the
job to the broker.

#### Scenario: Egress before ack
- **WHEN** a job succeeds and a `ResultStore` is configured
- **THEN** the worker SHALL store the serialized result
- **AND** it SHALL `ack` the job in the broker only if the store operation succeeds

#### Scenario: Store failure routes to retry
- **WHEN** a job succeeds but storing the result fails
- **THEN** the worker SHALL NOT `ack` the job
- **AND** the job SHALL follow the standard failure path (retry or dead-letter)

### Requirement: Typed Client Retrieval

The `Client` SHALL provide a `get_result<T>` method to retrieve and deserialize
the output of a job from the `ResultStore` given its `JobId`. Before returning a
stored value, the client MUST classify the job through the configured `Broker`.
Only `JobState::CompletedOrUnknown` SHALL permit returning stored bytes as a
typed result. `JobState::Live` and `JobState::DeadLettered` SHALL return
`Ok(None)` even if stale bytes exist in the result store.

#### Scenario: Client retrieves output

- **WHEN** a client calls `get_result<T>` with a valid `JobId` whose result is
  stored
- **AND** the broker classifies that job as `CompletedOrUnknown`
- **THEN** it SHALL return the deserialized typed output

#### Scenario: Live job result is hidden

- **WHEN** result bytes exist for a `JobId`
- **AND** the broker classifies that job as `Live`
- **THEN** `get_result<T>` SHALL return `Ok(None)`

#### Scenario: Dead-lettered job result is hidden

- **WHEN** result bytes exist for a `JobId`
- **AND** the broker classifies that job as `DeadLettered`
- **THEN** `get_result<T>` SHALL return `Ok(None)`

#### Scenario: Deserialization failure

- **WHEN** the stored bytes cannot be deserialized as type `T`
- **AND** the broker classifies that job as `CompletedOrUnknown`
- **THEN** `get_result<T>` SHALL return a deserialization error

### Requirement: Positive Result TTLs Expire

Result store implementations that support a configured TTL SHALL apply every
positive TTL as an expiring write. Sub-second positive TTL values MUST NOT be
rounded down to permanent retention.

#### Scenario: Sub-second TTL expires

- **WHEN** a result store is configured with a positive TTL shorter than one
  second
- **AND** a result is stored
- **THEN** that result SHALL expire after the configured TTL window

#### Scenario: Permanent retention remains explicit

- **WHEN** a result store is configured with no TTL or a zero TTL
- **THEN** stored results SHALL use that backend's permanent-retention mode
