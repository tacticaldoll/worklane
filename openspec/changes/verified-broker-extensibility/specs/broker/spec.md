## ADDED Requirements

### Requirement: Minimal lifecycle broker contract

The shared broker core SHALL contain only the operations required for the job
lifecycle loop: enqueue, reserve, ack, retry, defer, extend, fail, and classify.
Each operation SHALL preserve the existing lifecycle semantics for visibility,
reservation receipts, stale resolution, attempts, dead-lettering, uniqueness,
lanes, priority, and opaque envelopes.

Operations that are not required to run the lifecycle loop SHALL NOT be required
by the core lifecycle trait.

#### Scenario: Core lifecycle implementation is sufficient

- **WHEN** a broker implements the core lifecycle contract and no optional
  capability traits
- **THEN** a client SHALL be able to enqueue a job
- **AND** a worker SHALL be able to reserve, run, ack, retry, defer, extend, or
  fail that job according to the existing lifecycle semantics
- **AND** the broker SHALL be eligible to run the mandatory lifecycle
  conformance suite

#### Scenario: Optional inspection is absent

- **WHEN** a broker implements the core lifecycle contract but not dead-letter
  inspection
- **THEN** it SHALL still be a valid lifecycle broker
- **AND** code requiring dead-letter inspection SHALL detect that the capability
  is absent instead of assuming the core broker provides it

#### Scenario: Lifecycle semantics are unchanged

- **WHEN** a first-party broker is migrated to the split contract
- **THEN** its enqueue, reserve, ack, retry, defer, extend, fail, and classify
  behavior SHALL remain compatible with the existing broker requirements
- **AND** its conformance tests for those lifecycle scenarios SHALL still pass

### Requirement: Explicit optional broker capabilities

Behavior outside the minimal lifecycle loop SHALL be exposed through explicit
optional capability traits. Optional capabilities include batch enqueue,
dead-letter inspection and maintenance, queue-depth statistics, scheduled
enqueue, and result storage when present.

A consumer that needs an optional capability SHALL request that capability
explicitly and SHALL fail predictably when the selected broker does not provide
it.

#### Scenario: Capability is present

- **WHEN** a broker implements an optional capability trait
- **THEN** a consumer that requests that capability SHALL be able to use it
  through the public capability surface
- **AND** the broker SHALL be eligible to run that capability's conformance
  suite

#### Scenario: Capability is absent

- **WHEN** a consumer requests an optional capability from a broker that does not
  implement it
- **THEN** the consumer SHALL receive an explicit absence or unsupported
  capability result
- **AND** the consumer SHALL NOT infer support from the core lifecycle contract

#### Scenario: Optional capability does not change lifecycle behavior

- **WHEN** a broker adds or removes support for an optional capability
- **THEN** the core lifecycle semantics SHALL remain unchanged
- **AND** mandatory lifecycle conformance SHALL NOT depend on that optional
  capability

### Requirement: Portable broker contract changes

Any new broker core operation or required capability SHALL be justified against
both SQL-style and Redis-style implementations before implementation begins. The
change design SHALL record the portability argument and rejected alternatives.

#### Scenario: Proposed core operation is portable

- **WHEN** a change proposes a new required broker operation
- **THEN** its design SHALL explain how a SQL broker can implement it
- **AND** its design SHALL explain how a Redis broker can implement it
- **AND** implementation SHALL NOT begin until the portability argument is
  recorded

#### Scenario: Proposed operation is implementation-specific

- **WHEN** a proposed operation depends on live references, full in-memory scans,
  or synchronous visibility assumptions
- **THEN** it SHALL NOT be added to the required broker core
- **AND** it SHALL be rejected, kept backend-local, or exposed through a
  narrower optional capability with its own portability argument
