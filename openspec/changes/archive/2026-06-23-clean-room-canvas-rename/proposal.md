## Why

worklane's workflow-composition API and docs carry Celery-derived vocabulary —
the `Canvas` trait, the `Chord*` types/`ChordPolicy`, the `Client::chord*`
methods, and the Chain/Group/Chord topology names. Celery was only pre-repo
inspiration, never a position worklane takes; reusing its distinctive coinage
(especially "chord") without it being our stance reads as borrowed. The clean
fix is neutral, industry-standard fan-in/fan-out vocabulary. This renames public
API items, so per the API-stability rule it is a **breaking** change shipped as
**0.2.0** — and now (0.1.0 baseline, contract still declared mutable pre-1.0,
~no external users) is the cheapest time to do it.

## What Changes

- **BREAKING (supported public API):**
  - `Canvas` trait → `Workflow`
  - `ChordResults` → `FanInResults`
  - `ChordPolicy` → `FanInPolicy`
  - `Client::chord` → `Client::fan_in`; `Client::chord_with_policy` →
    `Client::fan_in_with_policy`
- **Rename (doc-hidden, not supported public API):** `ChordWatcherJob` →
  `FanInWatcherJob`, `ChordWatcherPayload` → `FanInWatcherPayload` (both
  `#[doc(hidden)]` — reachable for conformance tests only).
- **Internal, behavior-preserving renames:** topology prose Chain→Sequence,
  Group→FanOut, Chord→FanIn; unique-key prefixes `chain:`→`sequence:`,
  `cw:`→`fiw:`, `chord:`→`fanin:`; durable watcher job kind
  `"worklane:chord_watcher"`→`"worklane:fan_in_watcher"`; the private
  `chord_id` field and `chord_results_payload` helper; the source module
  `canvas`→`workflow`.
- **Specs:** rename the `workflow-canvas` capability to `workflow` (folder +
  title) and restate its requirements in the new vocabulary; fix incidental
  references in `result-backend`, `broker`, and `payload-store`.
- **Docs/config:** remove Celery from `README.md`, `docs/architecture.md`,
  `openspec/config.yaml`, `AGENTS.md`; rewrite `docs/design/workflow-canvas.md`;
  fix `BACKLOG.md`.
- **Version:** workspace `0.1.0 → 0.2.0`. Add a `CHANGELOG.md` 0.2.0 entry.
- **No behavior changes.** Every lifecycle/aggregation requirement is preserved
  exactly; only names change.

## Capabilities

### New Capabilities

- `workflow`: the renamed `workflow-canvas` capability (the rename + folder move
  is part of this change). Its requirements are the existing ones restated in
  fan-in/fan-out vocabulary — no behavior added.

### Modified Capabilities

- `workflow-canvas`: terminology only — all `Canvas`/`Chord*`/chord vocabulary
  in every requirement and scenario renamed to `Workflow`/`FanIn*`/fan-in, and
  the capability is renamed to `workflow`. **No WHEN/THEN behavior changes.**
- `result-backend`, `broker`, `payload-store`: incidental non-normative
  references to the renamed types/concepts updated; no behavior change.

## Impact

- **BREAKING public API** (`worklane` facade exports). Migration map in
  `design.md`. Rationale + rejected alternatives recorded in `design.md` per the
  API-stability rule (this repo records rationale in the OpenSpec change, not in
  ADRs).
- **Code:** `crates/worklane/src/{canvas.rs→workflow.rs, client.rs,
  client_builder.rs, lib.rs}`, `crates/worklane/tests/{canvas.rs→workflow.rs,
  chord_watcher.rs→fan_in_watcher.rs}`, `crates/worklane-test/src/scenarios/
  unique.rs`, `crates/worklane-redis/src/lib.rs`.
- **Version:** workspace `0.1.0 → 0.2.0` (breaking-change axis under 0.x).
- **Migration caveat:** renaming the durable watcher job kind and internal key
  prefixes can orphan in-flight workflows across the upgrade — acceptable at
  0.2.0 with no users; drain workers before upgrading (documented in
  `design.md`).
- **Out of scope:** any worker/perf/behavior change (e.g. the parked
  worker-reservation optimization). This change is rename + docs + version only.
