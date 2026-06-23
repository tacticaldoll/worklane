## ADDED Requirements

### Requirement: Broker author SPI

The system SHALL document `worklane_core::spi` as the broker-author extension
surface. SPI items SHALL encode shared backend decisions such as envelope
encoding, receipt encoding, duration conversion, stale-reservation construction,
redaction, and backend name validation helpers. The SPI SHALL NOT be re-exported
from the `worklane` facade.

#### Scenario: Broker author uses SPI helpers

- **WHEN** a broker author implements a durable broker
- **THEN** they SHALL be able to use documented `worklane_core::spi` helpers for
  shared storage and validation decisions
- **AND** those helpers SHALL be available without depending on a first-party
  broker crate

#### Scenario: Application user uses facade

- **WHEN** an application user depends on the `worklane` facade to enqueue and
  run jobs
- **THEN** the facade SHALL NOT require them to use broker-author SPI items
- **AND** SPI documentation SHALL identify the audience as broker authors

#### Scenario: Backend-local helper is not promoted

- **WHEN** a helper only serves one backend's implementation convenience
- **THEN** it SHALL stay in that backend crate
- **AND** it SHALL NOT be documented as shared SPI

### Requirement: Modular broker conformance suites

`worklane-test` SHALL expose a mandatory lifecycle conformance suite and
separate optional capability suites. The lifecycle suite SHALL assert only
through the minimal lifecycle contract and a harness adapter. Optional suites
SHALL assert only through the capability they validate and the harness support
needed to isolate scenarios.

#### Scenario: Lifecycle-only broker is tested

- **WHEN** a broker implements the minimal lifecycle contract but no optional
  capability traits
- **THEN** the broker author SHALL be able to run the lifecycle conformance
  suite
- **AND** the test wiring SHALL NOT require dead-letter inspection, queue stats,
  scheduled enqueue, batch enqueue, or result storage

#### Scenario: Optional capability suite is tested

- **WHEN** a broker implements an optional capability
- **THEN** the broker author SHALL be able to opt into that capability's
  conformance suite
- **AND** the suite SHALL fail if the capability violates its specified
  semantics

#### Scenario: Optional capability is omitted visibly

- **WHEN** a broker author does not opt into an optional capability suite
- **THEN** their compatibility claim SHALL NOT imply support for that capability
- **AND** the guide or matrix SHALL make the omitted capability visible

### Requirement: Custom broker compatibility claim

A custom broker compatibility claim SHALL state which conformance suites pass:
the mandatory lifecycle suite and any optional capability suites. Passing the
lifecycle suite SHALL mean the broker satisfies the lifecycle contract; it SHALL
NOT imply support for optional capabilities that were not tested.

#### Scenario: Complete compatibility claim

- **WHEN** a custom broker passes the lifecycle suite and selected optional
  capability suites
- **THEN** its documentation SHALL be able to claim compatibility for exactly
  those suites
- **AND** users SHALL be able to distinguish core lifecycle support from optional
  capability support

#### Scenario: Unsupported capability is claimed

- **WHEN** a custom broker claims support for an optional capability
- **AND** that capability's conformance suite has not passed
- **THEN** the claim SHALL be considered invalid by the project documentation
  policy

### Requirement: Conformance guide for broker authors

The project SHALL provide a custom broker conformance guide that explains how to
wire a private or third-party broker into `worklane-test`, how to choose
mandatory and optional suites, how to isolate scenarios, and how to interpret
passing and failing results.

#### Scenario: New broker author follows the guide

- **WHEN** a broker author reads the custom broker conformance guide
- **THEN** they SHALL know which crate to add as a dev-dependency
- **AND** they SHALL know how to provide the required harness
- **AND** they SHALL know how to run the mandatory lifecycle suite

#### Scenario: Capability-specific broker follows the guide

- **WHEN** a broker author implements scheduled enqueue or dead-letter
  inspection
- **THEN** the guide SHALL explain how to opt into the matching capability suite
- **AND** the guide SHALL explain that passing the lifecycle suite alone is not
  enough to claim that optional capability

#### Scenario: Conformance failure occurs

- **WHEN** a conformance scenario fails for a custom broker
- **THEN** the guide SHALL direct the author to fix the broker behavior rather
  than weakening the shared lifecycle contract
