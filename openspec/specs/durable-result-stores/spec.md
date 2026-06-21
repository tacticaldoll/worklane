# Durable Result Stores

## Purpose

Defines the requirements for the pluggable `ResultStore` across all durable broker backends.

## Requirements

### Requirement: Result store contract
Every `ResultStore` implementation SHALL satisfy a backend-agnostic contract,
verified by the shared conformance suite in `worklane-test`
(`result_store_scenarios`): round-trip fidelity, the `unknown key -> None`
boundary, last-writer-wins overwrite, and key isolation. The store is a pure
key/value egress for opaque bytes and MUST NOT interpret the payload.

#### Scenario: Round-trip
- **WHEN** `store` is called with a JobId and bytes, then `get` is called for the same JobId
- **THEN** `get` returns exactly the bytes that were stored

#### Scenario: Unknown key returns None (boundary)
- **WHEN** `get` is called for a JobId that was never stored
- **THEN** the call succeeds and returns `None` — not an error and not empty bytes

#### Scenario: Overwrite is last-writer-wins (edge)
- **WHEN** `store` is called twice for the same JobId with different bytes
- **THEN** a subsequent `get` returns the bytes from the second `store`

#### Scenario: Keys are isolated (edge)
- **WHEN** bytes are stored under one JobId
- **THEN** a `get` for a different JobId returns `None`, never the other key's
  value

### Requirement: SqliteResultStore Implementation

The `worklane-sqlite` crate SHALL provide a `SqliteResultStore` that implements
`worklane_core::ResultStore`. It MUST persist results as opaque blobs in a
`results` table.

#### Scenario: SQLite store and get
- **WHEN** a user calls `store` on `SqliteResultStore` with a JobId and bytes
- **THEN** the bytes are saved in the `results` table under that JobId
- **THEN** a subsequent `get` for that JobId retrieves the exact bytes

### Requirement: PostgresResultStore Implementation

The `worklane-postgres` crate SHALL provide a `PostgresResultStore` that
implements `worklane_core::ResultStore`. It MUST persist results as opaque
`BYTEA` in a `results` table.

#### Scenario: Postgres store and get
- **WHEN** a user calls `store` on `PostgresResultStore` with a JobId and bytes
- **THEN** the bytes are saved in the `results` table under that JobId
- **THEN** a subsequent `get` for that JobId retrieves the exact bytes

### Requirement: RedisResultStore Implementation

The `worklane-redis` crate SHALL provide a `RedisResultStore` that implements
`worklane_core::ResultStore`. It MUST persist results as raw byte strings keyed
by `worklane:result:<job_id>`.

#### Scenario: Redis store and get
- **WHEN** a user calls `store` on `RedisResultStore` with a JobId and bytes
- **THEN** the bytes are saved at the Redis key `worklane:result:<job_id>`
- **THEN** a subsequent `get` for that JobId retrieves the exact bytes

### Requirement: Configurable Redis TTL

The `RedisResultStore` SHALL allow configuring a TTL (Time-To-Live) for stored
results, preventing unbounded memory growth.

#### Scenario: Expiring results
- **WHEN** a TTL is configured on the store
- **THEN** stored results are automatically deleted by Redis after the TTL expires
- **THEN** subsequent calls to `get` will correctly return `None`
