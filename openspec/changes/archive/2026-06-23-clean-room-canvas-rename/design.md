## Context

The workflow-composition layer was modelled on Celery's "Canvas" and uses its
vocabulary: the `Canvas` trait, `ChordResults`/`ChordPolicy`/`ChordWatcherJob`/
`ChordWatcherPayload`, the Chain/Group/Chord topology names, the
`Client::chord`/`chord_with_policy` methods, internal `chain:`/`chord:`/`cw:`
unique keys, and the `worklane:chord_watcher` job kind. Celery was only pre-repo
inspiration. "Chord" in particular is a distinctive Celery coinage; the neutral,
industry-standard names are fan-out (parallel) and fan-in
(parallel-then-aggregate). This change adopts that vocabulary across the public
API, specs, and docs. It is a pure rename — no behavior changes — and it renames
public API items, so it is a breaking change shipped as 0.2.0.

This repo records breaking-change rationale **in the OpenSpec change (here in
design.md)**, not in ADRs (it has no ADR convention).

## Goals / Non-Goals

**Goals:**
- One consistent fan-in/fan-out vocabulary across code, specs, and docs.
- Zero behavior change; every existing requirement preserved verbatim except for
  the renamed terms.
- A clean, deliberate 0.2.0 with a migration map and the rationale recorded here.

**Non-Goals:**
- No behavior, performance, or lifecycle change. The parked worker-reservation
  optimization and any perf work are out of scope.
- No `Broker`/core-trait change (the rename is confined to the `worklane` facade
  and docs).
- No deprecated aliases of the old names (pre-1.0, clean break).

## Decisions

**1. fan-in / fan-out naming map (applied everywhere).**

| Old | New | Kind |
|-----|-----|------|
| `Canvas` (trait) | `Workflow` | public API (breaking) |
| `ChordResults` | `FanInResults` | public API (breaking) |
| `ChordPolicy` | `FanInPolicy` | public API (breaking) |
| `Client::chord` | `Client::fan_in` | public API (breaking) |
| `Client::chord_with_policy` | `Client::fan_in_with_policy` | public API (breaking) |
| `ChordWatcherJob` | `FanInWatcherJob` | `#[doc(hidden)]` |
| `ChordWatcherPayload` | `FanInWatcherPayload` | `#[doc(hidden)]` |
| field `chord_id` | `fanin_id` | private |
| fn `chord_results_payload` | `fan_in_results_payload` | private |
| module `canvas` | `workflow` | private (file + `mod` + `crate::` path) |
| topology: Chain / Group / Chord | Sequence / FanOut / FanIn | prose |
| key `chain:{id}:{kind}` | `sequence:{id}:{kind}` | internal |
| key `chord:{id}:callback` | `fanin:{id}:callback` | internal |
| key `cw:{id}:{gen}` | `fiw:{id}:{gen}` | internal |
| job kind `"worklane:chord_watcher"` | `"worklane:fan_in_watcher"` | durable |
| capability `workflow-canvas` | `workflow` | spec folder + title |

- *Why fan-in/fan-out:* the most widely recognized distributed-systems pairing;
  Group=fan-out maps naturally to Chord=fan-in. Neutral and unowned, so no
  attribution is needed and nothing reads as borrowed.

**2. Rejected alternatives** (recorded here per the API-stability rule):
- *Keep Celery's vocabulary, drop only the "Celery" mentions* — rejected:
  keeping the distinctive coinage (esp. "chord") while erasing the source is the
  move that most reads as appropriation.
- *Docs-only cleanup, keep the type names* — rejected: leaves "chord" in the
  public API, so the product still speaks Celery; a 0.1.1 that doesn't achieve
  the clean-room goal.
- *Defer until 1.0* — rejected: renaming public items is a breaking change;
  pre-1.0 with ~no users is the cheapest time it will ever be.

**3. No deprecated aliases — a clean break.** Old names are removed, not
re-exported as deprecated aliases. Pre-1.0, a clean break is cheaper to carry.

**4. Behavior frozen; the spec delta is terminology-only.** The capability delta
restates each existing requirement and WHEN/THEN scenario verbatim, swapping only
the vocabulary. No scenario is added, removed, or altered in meaning.

**5. Capability rename mechanic.** The delta is authored under the existing
capability path (`specs/workflow-canvas/`) so the OpenSpec validator/sync locate
the requirements in the current spec. The folder + title rename to `workflow`
happens as the **final apply/post-sync step** (`git mv specs/workflow-canvas
specs/workflow`), keeping delta/sync operating on a stable capability name.

## Risks / Trade-offs

- **Breaking downstream callers** → migration map below; done pre-1.0.
- **In-flight workflows orphaned across upgrade** → renaming the durable watcher
  job kind (`worklane:chord_watcher` → `worklane:fan_in_watcher`) and the
  internal key prefixes means a workflow mid-flight when a worker upgrades from
  0.1.x could have a watcher persisted under the old kind (no registered handler
  post-upgrade) or a generation keyed under the old prefix. Acceptable at 0.2.0
  with no users; **drain workers before upgrading**. A hot upgrade would be a
  separate migration change.
- **Spec drift (accidental behavior change during rename)** → the delta is a pure
  vocabulary swap; a reviewer diffs for renamed-nouns-only.
- **Missed reference** → a repo-wide grep for the old vocabulary is the final
  task gate (scoped to the live surface; historical CHANGELOG entries and the
  bench handoff keep their accurate names).

## Migration Plan

**0.1.x → 0.2.0 caller migration (rename only):**

```
Canvas                 -> Workflow
ChordResults           -> FanInResults
ChordPolicy            -> FanInPolicy
Client::chord          -> Client::fan_in
Client::chord_with_policy -> Client::fan_in_with_policy
```

No signature or behavior changes — a mechanical find/replace on the names.

**Operational caveat:** drain workers before upgrading if any fan-in is in
flight; the internal key/kind rename does not carry an in-flight watcher across
the upgrade.

## Open Questions

- Exact extent of `result-backend` / `payload-store` references — confirmed
  during authoring to be single non-normative prose mentions (no requirement
  delta needed; direct prose edits at apply). Resolved.
