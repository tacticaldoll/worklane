## Context

`worklane-cli` (`wl`) already exposes dead-letter list/requeue/purge and a
`stats <lane>` command, both built strictly on portable `Broker` contract
methods. `Broker::classify(id) -> JobState` is a core lifecycle operation
(`Live` / `DeadLettered` / `CompletedOrUnknown`, lane-agnostic, point lookup by
id) that has no CLI command. This change adds that command and nothing else.

## Goals / Non-Goals

**Goals:**

- Let an operator ask "what state is job X in?" from `wl`, against any durable
  backend, using only the existing `Broker::classify` method.
- Match the existing command conventions (broker-selection flags, `--format`
  output option, clear non-zero exit on bad input).

**Non-Goals:**

- No new `Broker` trait surface or `worklane-core` change.
- No new lifecycle counts (`running`/`delayed`/`failed`) — `QueueStats` only
  exposes `pending_count`; richer counts would require new broker surface and are
  out of scope (BACKLOG: operator lifecycle inspection).
- No bulk/scan classification — `classify` is a single point lookup by id.

## Decisions

### Reuse `Broker::classify` unchanged; CLI-only addition

The command is a thin adapter: parse the `<job-id>` argument into a `JobId`, call
`broker.classify(id)` on the `Arc<dyn Broker>` the existing broker-selection flow
returns, and render the `JobState`. No backend-native tooling, mirroring the
`stats` command's "portable contract only" rule.

*Broker design gate:* this adds no broker operation — `classify` already exists
on the core trait and is reachable through `Arc<dyn Broker>`, so the SQL/Redis
portability question is already answered by the shipped `classify`
implementations. Nothing to record beyond this note.

### Parse the job id before connecting

An unparseable id is rejected with a non-zero exit before any broker connection
is opened, so a typo fails fast and cheaply rather than after I/O. Alternative
(connect first, parse later) rejected: it wastes a connection and muddies the
error.

### Render `JobState` by its three canonical names

Default output is a single human-readable line naming the state (`Live`,
`DeadLettered`, `CompletedOrUnknown`); `--format json` emits a small JSON object,
matching the dead-letter listing command's format convention so scripted callers
have a stable shape. The three names are taken verbatim from the `JobState` enum
to avoid inventing a second vocabulary for the same states.

## Risks / Trade-offs

- **`CompletedOrUnknown` is ambiguous by design** (an acked job and a never-seen
  id are indistinguishable) → this is an existing property of `Broker::classify`,
  not introduced here; the command surfaces it honestly rather than guessing, and
  the state name itself communicates the ambiguity.

## Open Questions

- None. Scope is a single command over an existing contract method.
