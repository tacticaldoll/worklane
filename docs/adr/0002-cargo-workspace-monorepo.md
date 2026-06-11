# 2. Use a Cargo workspace monorepo

Date: 2026-06-11

## Status

Accepted

## Context

worklane separates a public facade, core abstractions, and broker
implementations. These evolve together but are published as distinct crates.

## Decision

Use a single Cargo workspace monorepo. v0.1 members: `worklane-core` (traits,
job model, envelope, errors), `worklane-memory` (in-memory broker), and
`worklane` (facade / public API). Durable brokers (`worklane-redis`) and proc
macros (`worklane-macros`) are added later.

## Consequences

- Shared `[workspace.package]` metadata (version, edition, license, authors).
- Crates can be released independently while developed together.
- The `Broker` trait lives in `worklane-core`, so broker crates depend only on
  core, not on the facade.
