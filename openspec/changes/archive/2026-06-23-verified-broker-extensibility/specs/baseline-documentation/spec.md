## ADDED Requirements

### Requirement: Lifecycle semantics guide

The repository SHALL provide stable documentation that summarizes the verified
job lifecycle semantics for users and operators. The guide SHALL cover enqueue,
delayed visibility, reserve, lease expiry, stale resolution, ack, retry, defer,
extend, fail, dead-lettering, uniqueness, scheduling, and at-least-once
execution. The guide SHALL link to OpenSpec as the source of truth and SHALL NOT
replace the normative specs.

#### Scenario: Reader needs lifecycle behavior

- **WHEN** a reader wants to understand how worklane handles reservation,
  retry, failure, or dead-lettering
- **THEN** stable project documentation SHALL provide a lifecycle semantics
  guide
- **AND** the guide SHALL link to the relevant OpenSpec capabilities

#### Scenario: Guide avoids a second contract

- **WHEN** lifecycle behavior is described in the guide
- **THEN** it SHALL be presented as a readable summary of OpenSpec requirements
- **AND** it SHALL NOT introduce behavior that is absent from the specs

#### Scenario: At-least-once boundary is documented

- **WHEN** the guide describes delivery guarantees
- **THEN** it SHALL state that execution is at-least-once
- **AND** it SHALL state that handlers must be idempotent

### Requirement: Broker conformance matrix

The repository SHALL provide a conformance matrix that distinguishes the
mandatory lifecycle suite from optional capability suites for each supported
broker. The matrix SHALL identify which suites pass for in-memory, SQLite,
PostgreSQL, and Redis brokers.

#### Scenario: Supported broker matrix is read

- **WHEN** a user reads the conformance matrix
- **THEN** they SHALL be able to identify which lifecycle and optional
  capability suites each supported broker passes
- **AND** optional capability support SHALL be distinguishable from lifecycle
  support

#### Scenario: Optional capability is absent

- **WHEN** a broker does not support an optional capability
- **THEN** the matrix SHALL mark that capability as absent or not applicable
- **AND** it SHALL NOT imply that passing the lifecycle suite provides that
  capability

#### Scenario: Matrix is updated after conformance changes

- **WHEN** a broker gains or loses a conformance-tested capability
- **THEN** the conformance matrix SHALL be updated in the same change

### Requirement: Custom broker guide is stable documentation

The repository SHALL provide stable documentation for custom broker authors that
explains the broker SPI, conformance-suite wiring, compatibility claims, and
common portability pitfalls. Deferred or speculative backend work SHALL remain
in `BACKLOG.md`.

#### Scenario: Broker author looks for extension docs

- **WHEN** a reader wants to implement a custom broker
- **THEN** stable documentation SHALL direct them to the SPI and conformance
  guide
- **AND** they SHALL NOT need to copy implementation details from a first-party
  broker crate to understand the supported extension path

#### Scenario: Speculative backend is mentioned

- **WHEN** documentation mentions a backend that is not implemented
- **THEN** that backend SHALL remain documented as deferred work in `BACKLOG.md`
- **AND** stable custom-broker documentation SHALL focus on the extension
  contract rather than promising that backend
