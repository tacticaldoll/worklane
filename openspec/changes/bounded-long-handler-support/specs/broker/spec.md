## ADDED Requirements

### Requirement: Lease extension

`extend(receipt)` SHALL re-apply the broker's visibility lease to the job
currently held under `receipt`, keeping it invisible to other `reserve` calls
for a fresh lease measured from the current time, only when the receipt is the
current valid receipt for the job's active reservation. A receipt that is
unknown, superseded, or whose lease has already expired SHALL be rejected with a
stale-reservation error, and the broker MUST NOT change the job's lease,
schedule, or visibility due to that stale receipt. `extend` SHALL NOT change the
job's `attempts`. The lease duration is owned by the broker (as for `reserve`);
`extend` takes no caller-supplied duration.

#### Scenario: Extend holds the job past its original lease

- **WHEN** a reserved job is extended with its current receipt before its lease
  expires, and the clock then advances past the original lease but within the
  re-applied lease
- **THEN** a `reserve` on that lane SHALL NOT return the job
- **AND** the job SHALL still be resolvable (ack / retry / fail) with that receipt

#### Scenario: Extend after lease expiry rejected

- **WHEN** a reserved job's lease expires before it is extended
- **THEN** extending with the expired receipt SHALL fail with a stale-reservation error
- **AND** the job SHALL remain available for a current reservation
- **AND** its `attempts` and schedule SHALL be unchanged by the rejected extend

#### Scenario: Superseded receipt cannot extend

- **WHEN** a reserved job's lease expires, the job is reserved again, and the
  first receipt is used to extend
- **THEN** the extend SHALL fail with a stale-reservation error
- **AND** the current reservation SHALL remain valid and its lease unchanged

### Requirement: Reserved lease window is observable

A `Reservation` returned by `reserve` SHALL convey the lease duration the broker
applied, so a caller can schedule lease maintenance (for example a heartbeat
that calls `extend`) without reading the broker's clock. The conveyed duration
SHALL equal the lease the broker uses to hide the reserved job.

#### Scenario: Reservation conveys the broker's lease

- **WHEN** a broker configured with a known lease duration reserves a job
- **THEN** the returned reservation SHALL convey that lease duration
