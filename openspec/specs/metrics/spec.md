# Metrics Specification

## Purpose

Defines the optional `worklane-metrics` facade for observing core loop outcomes
without changing broker behavior.

## Requirements

### Requirement: Metrics Facade

The `worklane-metrics` crate SHALL expose a lightweight metrics facade for
observing core loop outcomes without changing broker behavior. Metrics
integration MUST remain optional and MUST NOT be required by the base
`worklane` crate.

#### Scenario: Metrics crate is optional

- **WHEN** a user depends on the base `worklane` crate
- **THEN** they SHALL NOT be forced to depend on metrics-specific code paths

#### Scenario: Metrics observe worker outcomes

- **WHEN** worker attempts start, stop, retry, fail, or ack
- **THEN** the metrics facade SHALL be able to record those outcomes through
  stable observation points

### Requirement: Metrics Documentation

Repository documentation SHALL list the metrics crate as a shipped supporting
crate and SHALL describe its purpose without presenting it as future work.

#### Scenario: Metrics listed in crate inventory

- **WHEN** a reader inspects the workspace crate inventory
- **THEN** `worklane-metrics` SHALL appear with a current-purpose description
