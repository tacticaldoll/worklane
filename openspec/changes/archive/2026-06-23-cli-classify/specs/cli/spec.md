## ADDED Requirements

### Requirement: Job classification command

The CLI SHALL provide a `classify <job-id>` command that reports a job's
lifecycle state by id using the existing `Broker::classify` contract method. It
SHALL print exactly one of the three `JobState` values — `Live`, `DeadLettered`,
or `CompletedOrUnknown`. The command is lane-agnostic (it takes a job id only)
and MUST NOT add broker-native inspection methods.

#### Scenario: Live job is classified

- **WHEN** `wl classify <job-id>` is invoked for a job that is pending or
  in-flight under a lease
- **THEN** the CLI SHALL report the job as `Live`

#### Scenario: Dead-lettered job is classified

- **WHEN** `wl classify <job-id>` is invoked for a job that exhausted its
  attempts or was failed
- **THEN** the CLI SHALL report the job as `DeadLettered`

#### Scenario: Completed or unknown job is classified

- **WHEN** `wl classify <job-id>` is invoked for a job that was acked, or for an
  id that never existed
- **THEN** the CLI SHALL report the job as `CompletedOrUnknown`

#### Scenario: Invalid job id is rejected

- **WHEN** `wl classify <job-id>` is invoked with a value that is not a valid job
  id
- **THEN** the CLI SHALL print a parse error and exit with a non-zero code
- **AND** it SHALL NOT connect to a broker

#### Scenario: Classification uses the portable broker contract

- **WHEN** `wl classify <job-id>` is invoked against any supported broker
- **THEN** the command SHALL obtain the state through the portable
  `Broker::classify` method
- **AND** it SHALL NOT require backend-native tooling

#### Scenario: Output format selection

- **WHEN** `wl classify <job-id> --format json` is invoked
- **THEN** the CLI SHALL print the state as a JSON object
- **AND** the default format (no `--format`) SHALL be a human-readable line
