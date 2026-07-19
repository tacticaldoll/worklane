## Why

The operator CLI can list and maintain dead letters and report a lane's pending
count, but it cannot answer the single most common operational question: *"what
state is this job in?"* `Broker::classify` already provides that answer as a core
lifecycle operation — it is simply not reachable from `wl`. Exposing it completes
operator lifecycle visibility without adding any broker surface.

## What Changes

- Add a `wl classify <job-id>` command that parses a `JobId`, calls the existing
  `Broker::classify`, and prints the resulting `JobState`.
- Support an output `--format` option (human-readable line by default, plus
  `json`) consistent with the existing dead-letter listing command's convention.
- Reject an unparseable job id with a clear error before connecting to a broker.
- No change to the `Broker` trait, `worklane-core`, or any backend: `classify`
  already exists on the core trait and is reachable through `Arc<dyn Broker>`.

## Capabilities

### New Capabilities

<!-- none -->

### Modified Capabilities

- `cli`: Adds a job-classification command requirement alongside the existing
  dead-letter and stats commands.

## Impact

- Affects `worklane-cli` only: a new `Commands::Classify` variant in
  `crates/worklane-cli/src/main.rs` and a `cmd/classify.rs` handler.
- No change to `worklane-core`, the `Broker` trait, or any broker crate.
- No new dependencies. Works against every durable backend (SQLite, Postgres,
  Redis) through the existing broker-selection flags.
