# cli Specification

## Purpose
Define the `worklane-cli` (`wl`) operator command-line tool for inspecting and
maintaining a worklane broker out-of-band from the application: connecting to a
SQLite, PostgreSQL, or Redis broker and listing, requeuing, and reporting on
dead-lettered jobs and lane health. It is an operational surface over the
existing `Broker` contract and adds nothing to that contract.

## Requirements
### Requirement: Dead-letter listing command

The CLI SHALL provide a `dead-letters list <lane>` command that reads up to
`--limit N` (default: 50) dead-letter records for the specified lane from the
configured broker and prints them to stdout. The default output format SHALL
be one JSON object per line (JSON Lines). When `--format table` is specified,
it SHALL print a human-readable table instead.
Output serialization failures SHALL be reported as command errors and MUST NOT
panic.

#### Scenario: List dead letters in JSON-lines format

- **WHEN** `wl --broker <b> dead-letters list <lane>` is invoked against a
  broker that has dead-letter records for `<lane>`
- **THEN** the CLI SHALL print one JSON object per line, each containing at
  least the job `id`, `kind`, `attempts`, and `error` fields
- **AND** exit with code 0

#### Scenario: List dead letters in table format

- **WHEN** `wl --broker <b> dead-letters list <lane> --format table` is
  invoked
- **THEN** the CLI SHALL print a human-readable table with at least `id`,
  `kind`, `attempts`, and `error` columns

#### Scenario: Empty dead-letter store

- **WHEN** `wl dead-letters list <lane>` is invoked and no dead-letter records
  exist for `<lane>`
- **THEN** the CLI SHALL print nothing (empty output) and exit with code 0

#### Scenario: JSON serialization failure returns an error

- **WHEN** a dead-letter row cannot be serialized for JSON-lines output
- **THEN** the CLI SHALL return a command error
- **AND** it SHALL NOT panic

### Requirement: Dead-letter requeue command

The CLI SHALL provide a `dead-letters requeue <id>` command that obtains the
broker's `DeadLetterStore` (via `Broker::dead_letter_store`) and calls
`DeadLetterStore::requeue` for the given job ID, moving the dead-lettered job
back to its original lane as a visible job. If the broker exposes no
`DeadLetterStore`, the command SHALL print an error and exit non-zero.

#### Scenario: Successful requeue

- **WHEN** `wl dead-letters requeue <id>` is invoked with a valid dead-letter
  job ID
- **THEN** the CLI SHALL call `DeadLetterStore::requeue(id)` and print a
  confirmation message, then exit with code 0

#### Scenario: Unknown job ID

- **WHEN** `wl dead-letters requeue <id>` is invoked with a job ID that has
  no dead-letter record
- **THEN** the CLI SHALL print an error message to stderr and exit with a
  non-zero code

### Requirement: Dead-letter purge command

The CLI SHALL provide a `dead-letters purge <lane>` command that obtains the
broker's `DeadLetterStore` (via `Broker::dead_letter_store`) and calls
`DeadLetterStore::purge_dead_letters` for the given lane, permanently removing
all of its dead-letter records (printing an error and exiting non-zero if the
broker exposes no `DeadLetterStore`). Because the purge is irreversible, the command SHALL prompt
for confirmation and abort on any non-affirmative answer unless `--yes`/`-y` is
given. The chosen connection source is announced and credentials are never
printed.

#### Scenario: Successful purge

- **WHEN** `wl dead-letters purge <lane> --yes` is invoked against a broker with
  dead-letter records for `<lane>`
- **THEN** the CLI SHALL call `DeadLetterStore::purge_dead_letters(lane)`, print
  how many records were removed, and exit with code 0

#### Scenario: Purge aborts without confirmation

- **WHEN** `wl dead-letters purge <lane>` is invoked without `--yes` and the
  prompt is not affirmatively answered (including EOF / no TTY)
- **THEN** the CLI SHALL NOT purge and SHALL exit without removing any records

### Requirement: Stats command

The CLI SHALL provide a `stats <lane>` command that reports the dead-letter
count and live pending job count for the specified lane using existing `Broker`
contract methods. The command MUST NOT add broker-native inspection methods.

#### Scenario: Stats output

- **WHEN** `wl stats <lane>` is invoked
- **THEN** the CLI SHALL print the dead-letter count for `<lane>`
- **AND** it SHALL print the pending live job count for `<lane>`

#### Scenario: Stats uses portable broker contract

- **WHEN** `wl stats <lane>` is invoked against any supported broker
- **THEN** the command SHALL obtain counts through the portable `Broker`
  contract
- **AND** it SHALL NOT require backend-native tooling

### Requirement: Broker selection

The CLI SHALL support connecting to SQLite, Postgres, and Redis brokers via
global flags:

- `--broker sqlite --db <path>` for SQLite
- `--broker postgres --url <url>` (or `DATABASE_URL` env var) for Postgres
- `--broker redis --url <url>` (or `REDIS_URL` env var) for Redis

An unrecognised `--broker` value or a missing required flag SHALL cause the
CLI to print a usage error and exit with a non-zero code.

#### Scenario: SQLite broker selected

- **WHEN** `--broker sqlite --db ./jobs.db` is provided
- **THEN** the CLI SHALL open the SQLite database at `./jobs.db` and use it
  as the broker for the command

#### Scenario: Missing required flag

- **WHEN** `--broker sqlite` is provided without `--db`
- **THEN** the CLI SHALL print a usage error and exit non-zero

#### Scenario: Env-var credentials for Postgres

- **WHEN** `--broker postgres` is provided and `DATABASE_URL` is set in the
  environment
- **THEN** the CLI SHALL use `DATABASE_URL` as the connection URL
