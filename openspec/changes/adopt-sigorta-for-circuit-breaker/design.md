## Context

`CircuitBreaker` currently performs two responsibilities inside one small module:

- Worklane owns the policy (`failure_threshold`, `open_duration`), the per-kind
  keying (`Mutex<HashMap<String, BreakerState>>`), and reads the wall clock
  (`Instant::now()`) at the top of `admit`/`record`.
- An in-process `BreakerState` enum (`Closed`/`Open`/`HalfOpen`) implements the
  closed/open/half-open transition rules, including bounding a half-open probe's
  lifetime so a probe that never reports back cannot wedge a kind forever.

Sigorta 0.1.1 implements only the second responsibility, as a sans-I/O,
`Instant`-explicit core — and its own `0.1.1` release exists specifically because
this breaker's own evidence (its `HalfOpen { probe_expires }` bound) was found
missing from Sigorta's first distillation. The mechanism now matches this
breaker's real behavior exactly.

## Goals / Non-Goals

**Goals:**
- Adopt the published `sigorta` facade through the existing workspace dependency
  convention (matching how `lengkap` was adopted).
- Preserve `CircuitBreaker`'s public API (`CircuitBreakerPolicy`, `new`, `admit`,
  `record`) and every existing observable behavior exactly — no change to
  `execution.rs`'s call sites, no change to when a job is deferred or for how long.
- Keep the ambient clock read, the `Mutex`, and the per-kind `HashMap` in Worklane's
  own wrapper — these are consumer concerns Sigorta's core deliberately does not own.
- Reuse the existing unit test suite as the behavioral-equivalence proof, adding
  nothing new to it (no behavior changed for it to cover).

**Non-Goals:**
- Change when a job is deferred, for how long, or how the probe/cooldown timing
  works from an external caller's point of view.
- Expose `sigorta::Sigorta`, `Decision`, or `Event` through Worklane's public API.
- Add Worklane vocabulary, async, storage, or I/O to Sigorta.
- Change `Broker`, `ResultStore`, any backend, or the Tianheng constitution.

## Decisions

### Depend on the published facade, not the core directly

Declare `sigorta = "0.1.1"` in `[workspace.dependencies]` and consume it only from
the `worklane` facade, matching `lengkap`'s existing precedent exactly. Depending on
`sigorta-contract` directly was rejected for the same reason `lengkap-contract` was:
application consumers should depend on the curated, intended public surface.

This adds an external dependency but no new workspace edge — `worklane`'s Tianheng
boundary (`restrict_workspace_dependencies_to(["worklane-core"])`) governs workspace
crates only and is unaffected, exactly as it was unaffected by adopting `lengkap`.

### Replace `BreakerState` with `sigorta::Sigorta`, keep everything else

`states: Mutex<HashMap<String, BreakerState>>` becomes
`states: Mutex<HashMap<String, Sigorta>>`. `admit` and `record` still read
`Instant::now()` themselves (Sigorta core takes `now` explicitly; the ambient read
stays exactly where it already was, at Worklane's edge) and still look up or insert a
fresh `Sigorta::new(self.policy.failure_threshold, self.policy.open_duration)` per
kind via the map's `entry` API.

`admit` maps `Decision::Admitted | Decision::Probing` to `None` (both were
indistinguishable `None` returns in the hand-rolled version too — Sigorta's
`Probing` is a strictly more informative signal Worklane does not need to act on
differently yet, matching this change's non-goal of not changing external timing
behavior) and `Decision::Rejected { retry_after, .. }` to `Some(retry_after)`.
`record` maps `success: bool` to `Event::Success`/`Event::Failure` and discards the
returned `Sigorta`'s only purpose is to be re-inserted into the map.

Alternative considered: surface `Probing` up through `admit`'s own return type (e.g.
`enum Admission { Admit, Probe, Defer(Duration) }`) so a caller could someday log or
meter a probe distinctly from a routine admit — rejected as out of scope: no current
caller in `execution.rs` needs that distinction, and adding it now would be
speculative surface with no forcing use, the same discipline Sigorta's own `AGENTS.md`
applies to itself.

### Reuse the existing test suite as the equivalence proof

All five existing unit tests (`opens_after_threshold_consecutive_failures`,
`success_resets_the_failure_run`, `only_one_probe_is_admitted_when_the_cooldown_elapses`,
`a_failed_probe_reopens_and_a_successful_probe_closes`, `breakers_are_per_kind`) are
kept unchanged and must continue passing unmodified — they already exercise
`CircuitBreaker`'s public API and real timing (`std::thread::sleep`), so they are
the direct evidence that swapping the internals changed nothing observable.

## Risks / Trade-offs

- **A future Sigorta release changes the decision contract.** → Depend on the
  compatible `0.1` line, matching `lengkap`'s own precedent; review future lockfile
  updates against this module's existing test suite.
- **The stale-probe-replacement behavior (new in Sigorta 0.1.1) was never actually
  exercised by Worklane's own hand-rolled code before now, because both used the
  identical mechanism independently.** → No behavior actually changes for Worklane:
  its own `HalfOpen { probe_expires }` already did exactly this; adopting Sigorta
  0.1.1 (not 0.1.0) is what makes the two mechanisms equivalent, not a new capability
  Worklane is gaining.
- **Tianheng does not govern external dependencies or private runtime behavior.** →
  Keep that effect explicit here and verify the existing facade boundary still
  passes after implementation.

## Migration Plan

Implement on a fresh branch from `main`. Sync and archive this change, run the
complete local Definition of Done, open a PR, wait for CI, and squash-merge to
`main`.

Rollback before merge is a branch deletion. After merge, revert in a normal
reviewed change; no durable data migration is needed — the breaker's state is
in-process and per-worker, never persisted.

## Open Questions

None.
