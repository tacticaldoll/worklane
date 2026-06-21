# Lane Identifier Specification

## Purpose

Defines what a lane *is*: the `Lane` type that identifies the partition a job is
enqueued to and reserved from. A lane is a validated newtype rather than a bare
string, so a value of another kind cannot be used as a lane and malformed names
are rejected at construction. Validation is portable — only the invariant every
broker can honour — and lanes round-trip through storage without re-validation.

## Requirements

### Requirement: Lane is a validated type

A lane SHALL be represented by a distinct `Lane` type, not a bare string, so that
a value of another kind (for example a job `kind`) cannot be passed where a lane
is expected. `Lane` SHALL be constructed only through fallible conversions
(`TryFrom<&str>`, `TryFrom<String>`, `FromStr`) that validate the input and
return a `LaneError` on failure.

#### Scenario: A well-formed name produces a lane

- **WHEN** `Lane` is constructed from `"critical"`
- **THEN** construction SHALL succeed
- **AND** the resulting lane's string value SHALL be `"critical"`

#### Scenario: Construction is fallible

- **WHEN** `Lane` is constructed from an invalid string
- **THEN** construction SHALL return a `LaneError` rather than a `Lane`

### Requirement: Portable lane validation

`Lane` construction SHALL enforce only the portable invariant that every broker
can honour: the name SHALL be non-empty, SHALL NOT exceed 256 bytes, SHALL NOT
contain control characters, and SHALL NOT have leading or trailing whitespace.
Backend-specific constraints (such as a broker's key-delimiter charset) SHALL
NOT be enforced by `Lane`.

#### Scenario: Empty name is rejected

- **WHEN** `Lane` is constructed from `""`
- **THEN** construction SHALL fail with a `LaneError`

#### Scenario: Over-length name is rejected

- **WHEN** `Lane` is constructed from a string longer than 256 bytes
- **THEN** construction SHALL fail with a `LaneError`

#### Scenario: Control characters are rejected

- **WHEN** `Lane` is constructed from a string containing a control character (for
  example `"a\nb"`)
- **THEN** construction SHALL fail with a `LaneError`

#### Scenario: Surrounding whitespace is rejected

- **WHEN** `Lane` is constructed from `" critical "`
- **THEN** construction SHALL fail with a `LaneError`

#### Scenario: A backend-specific character is still accepted

- **WHEN** `Lane` is constructed from a name containing a character some broker
  reserves for its keys (for example `"a:b"`)
- **THEN** construction SHALL succeed, because that constraint is not portable

### Requirement: Default lane

There SHALL be a default lane whose name is `"default"`, obtainable without
fallible construction. Constructing a `Lane` from the string `"default"` SHALL
produce a lane equal to the default lane.

#### Scenario: Default lane name

- **WHEN** the default `Lane` is obtained
- **THEN** its string value SHALL be `"default"`

### Requirement: Lanes round-trip without re-validation

`Lane` SHALL serialize as a bare string identical to its name, so that persisted
job envelopes are unchanged by the introduction of the type. Deserializing a
stored lane SHALL NOT re-run validation: a lane that was persisted SHALL
deserialize back into an equal `Lane` even if its name would not pass current
construction validation.

#### Scenario: Transparent string serialization

- **WHEN** a `Lane` with name `"critical"` is serialized
- **THEN** the serialized form SHALL be the string `"critical"`

#### Scenario: Stored lane deserializes without validation

- **WHEN** a lane string that current validation would reject is deserialized
  into a `Lane`
- **THEN** deserialization SHALL succeed and yield a `Lane` with that name
