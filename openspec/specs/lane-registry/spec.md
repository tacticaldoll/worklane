# lane-registry Specification

## Purpose
Defines the opt-in `LaneRegistry`: a client-side set of known lanes that, when
configured on the `Client`, makes every enqueue path reject an unregistered
target lane with `Error::UnknownLane` (submitting nothing). The default stays
permissive to preserve dynamic lanes, so the guard must be opted into.
## Requirements
### Requirement: Lane registry is an opt-in set of known lanes

The system SHALL provide a `LaneRegistry` value type representing a set of known
`Lane` values. The registry SHALL be constructible from an iterator of lanes and
SHALL offer a membership test that returns whether a given `Lane` is a member.
Membership SHALL be exact `Lane` equality; the registry SHALL NOT perform fuzzy
or suggestion matching.

#### Scenario: Build a registry and test membership

- **WHEN** a `LaneRegistry` is built from the lanes `"email"` and `"reports"`
- **THEN** testing membership of `"email"` SHALL return true
- **AND** testing membership of `"emial"` SHALL return false

#### Scenario: Empty registry contains nothing

- **WHEN** a `LaneRegistry` is built from an empty set of lanes
- **THEN** testing membership of any lane SHALL return false

### Requirement: Registry is client-side and broker-agnostic

The `LaneRegistry` SHALL be a value-level construct used on the enqueue side
only. It SHALL NOT be stored in or enforced by any `Broker`, and brokers and
workers SHALL gain no knowledge of the full set of lanes from this capability.

#### Scenario: Registry does not alter broker or worker contracts

- **WHEN** a `LaneRegistry` is used to guard enqueues
- **THEN** the `Broker` trait, its storage, and the worker reserve loop SHALL be
  unchanged
- **AND** a worker SHALL continue to reserve only the single lane it is
  configured for

