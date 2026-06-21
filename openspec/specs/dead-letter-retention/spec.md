# dead-letter-retention Specification

## Purpose
Defines the opt-in `RetentionPolicy` that bounds a broker's dead-letter store by
`max_age` and/or `max_count`, enforced lazily per lane on `fail` (the default is
unbounded). It bounds the otherwise-unbounded growth of the dead-letter store
without a background runtime.
## Requirements
### Requirement: Opt-in dead-letter retention policy

A broker SHALL support an optional `RetentionPolicy` with independent
`max_age` and `max_count` bounds, each optional. When no policy is configured,
or both bounds are unset, the broker SHALL retain dead-letter records without
bound, exactly as without this capability. The policy SHALL be configured per
broker and SHALL apply only to dead-letter records, never to live jobs.

#### Scenario: No policy retains everything

- **WHEN** a broker with no retention policy dead-letters many jobs on a lane
- **THEN** every dead-letter record SHALL remain readable
- **AND** the dead-letter count SHALL equal the number of failed jobs

### Requirement: max_count bounds dead-letters by sequence

The broker SHALL, when a max-count bound of N is configured, retain at most the N
most-recently dead-lettered records for a lane after a job is dead-lettered
there, dropping older records by enqueue sequence (`seq`). Pruning SHALL be
scoped to the affected lane.

#### Scenario: Exceeding max_count drops the oldest

- **WHEN** a broker configured with `max_count = 3` dead-letters 5 jobs on one
  lane in order
- **THEN** the dead-letter count for that lane SHALL be 3
- **AND** the 3 retained records SHALL be the 3 most recently dead-lettered
- **AND** a different lane's dead-letters SHALL be unaffected

### Requirement: max_age drops aged dead-letters on failure

The broker SHALL, when a max-age bound is configured, drop a lane's dead-letter
records whose age exceeds that bound after a job is dead-lettered on the lane,
measuring age by the injected clock against a stored dead-letter timestamp.
Enforcement is write-driven: it occurs on `fail`, so a lane that stops failing
MAY retain aged records until its next failure or a manual purge.

#### Scenario: Aged records are dropped on the next failure

- **WHEN** a broker configured with `max_age` dead-letters a job, then the
  injected clock advances beyond `max_age`, then another job is dead-lettered on
  the same lane
- **THEN** the first (now-aged) record SHALL be dropped
- **AND** the just-dead-lettered record SHALL be retained

#### Scenario: Idle lane is not time-bounded until the next failure

- **WHEN** a broker configured with `max_age` dead-letters a job and the clock
  advances beyond `max_age` with no further failures on that lane
- **THEN** the aged record MAY still be present until the next `fail` or an
  explicit `purge_dead_letters`

### Requirement: max_age and max_count compose when both are set

When both bounds are configured, each SHALL be enforced independently on `fail`:
a record SHALL be dropped if it is older than `max_age` OR beyond the `max_count`
most-recent records for the lane. Enabling one bound SHALL NOT disable the other.

#### Scenario: Both bounds are enforced together

- **WHEN** a broker configured with both `max_age` and `max_count` dead-letters
  more than `max_count` jobs on a lane within `max_age`
- **THEN** the dead-letter count for that lane SHALL be bounded to `max_count`
  (the count bound applies though every record is still within `max_age`)
- **WHEN** the clock then advances beyond `max_age` and another job is
  dead-lettered on the same lane
- **THEN** the now-aged survivors SHALL be dropped, leaving only the
  just-dead-lettered record

