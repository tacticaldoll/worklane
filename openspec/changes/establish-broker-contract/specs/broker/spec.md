## ADDED Requirements

### Requirement: Injectable time source

A broker SHALL derive all time-based decisions (job visibility, lease expiry, and
retry scheduling) from an injectable clock rather than reading wall-clock time
directly, so that its lease and visibility semantics are deterministic and
portable across deployments and verifiable by the shared contract suite.

#### Scenario: Visibility advances by injected time

- **WHEN** a broker is constructed with a clock and that clock is advanced past a
  reserved job's lease without an intervening ack, retry, or fail
- **THEN** the job SHALL become reservable again
- **AND** this transition SHALL depend on the injected clock, not on wall-clock time

#### Scenario: Scheduled visibility tracks injected time

- **WHEN** a job is retried with a delay and the injected clock is advanced by that delay
- **THEN** the job SHALL become reservable again, independent of wall-clock time
