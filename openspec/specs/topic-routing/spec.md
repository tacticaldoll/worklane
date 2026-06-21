# Topic Routing

## Purpose

Defines the semantic behavior of the lightweight `worklane-pubsub` crate, which
maps string topics to underlying worker lanes. This capability is built purely
over the existing `Broker::enqueue_batch` and `Client::enqueue_to_lanes`
primitives and does not require topic knowledge within the core contract.

## Requirements

### Requirement: Topic Publisher API

The system SHALL provide a `Publisher` abstraction that allows users to map
string topics to one or more `Lane` destinations and publish jobs to those
topics through the current builder-based publishing API. The baseline public API
MUST NOT ship pre-deprecated publishing methods.

#### Scenario: Successful fan-out

- **WHEN** a user builds a publish operation for a payload and a topic
  configured with 3 lanes
- **THEN** the payload is serialized exactly once
- **AND** an atomic batch enqueue is dispatched to the underlying broker with 3
  `NewJob` records, one for each lane
- **AND** the current publishing API returns the `Vec<JobId>` of the enqueued
  jobs

#### Scenario: Unknown topic

- **WHEN** a user attempts to publish to a topic that has not been configured in
  the `Publisher`
- **THEN** the operation SHALL fail with a clear error indicating the topic is
  unknown
- **AND** no jobs are enqueued to the broker

#### Scenario: No pre-deprecated publish method

- **WHEN** the baseline public API is compiled with warnings denied
- **THEN** publishing examples and tests SHALL use the current API
- **AND** no pre-deprecated `Publisher::publish` method SHALL be required

### Requirement: Publisher Configuration

The `Publisher` SHALL be configurable via a builder pattern to register
topic-to-lanes mappings.

#### Scenario: Registering a topic
- **WHEN** a user calls
  `Publisher::new(client).route("events", vec![lane1, lane2])`
- **THEN** subsequent calls to publish to `"events"` SHALL target `lane1` and
  `lane2`

#### Scenario: Overwriting a topic route
- **WHEN** a user registers routes for a topic that is already registered
- **THEN** the new route definition SHALL replace the old route definition

### Requirement: Independent Crate

The pub/sub abstraction SHALL be provided in a standalone crate named
`worklane-pubsub`, depending on `worklane` but separate from it.

#### Scenario: Core independence
- **WHEN** a user imports the base `worklane` crate
- **THEN** they do not transitively depend on any pub/sub specific logic or data
  structures, ensuring the core remains ignorant of topics.
