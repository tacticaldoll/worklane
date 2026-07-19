# AGENTS.md

Meta-guideline for any AI coding agent (Claude Code, Codex, Cursor, Copilot,
Gemini, …) working in this repository. **Read this first.**

## This project uses OpenSpec (spec-driven development)

The source of truth lives in `openspec/`, which is version-controlled and
agent-agnostic. Do not invent your own process — follow OpenSpec.

- `openspec/specs/` — the living specification of what the system currently **is**.
  This is the single source of truth.
- `openspec/changes/` — active change proposals as delta specs
  (`ADDED` / `MODIFIED` / `REMOVED` requirements).
  `openspec/changes/archive/` holds completed ones.

Per-agent command files (e.g. `.claude/`) are **per-clone generated shims and are
not committed**. After cloning, generate your own with:

```bash
openspec init --tools <your-agent>   # e.g. claude, codex, cursor, github-copilot
```

## Workflow — follow this, do not implement ad hoc

```
explore → propose → apply → sync → archive
```

1. **Explore** (optional): think and investigate only. Never write feature code
   outside of a change.
2. **Propose**: create a change with `proposal.md` (what & why), `design.md` (how),
   and `tasks.md` (implementation steps).
3. **Apply**: implement tasks one at a time, checking each off in `tasks.md`.
4. **Sync**: merge the change's delta specs back into `openspec/specs/`.
5. **Archive**: move the completed change to
   `openspec/changes/archive/YYYY-MM-DD-<name>/`.

### Agent-agnostic interface (OpenSpec CLI)

If your agent has no OpenSpec slash commands, drive the workflow via the CLI:

```bash
openspec list [--json] [--specs]                       # list active changes / specs
openspec new change "<name>"                           # scaffold a change
openspec status --change "<name>" --json               # artifact build order & status
openspec instructions <artifact> --change "<name>"     # per-artifact guidance
openspec archive <name>                                # archive a completed change
```

Claude Code users additionally have slash commands:
`/opsx:explore`, `/opsx:propose`, `/opsx:apply`, `/opsx:sync`, `/opsx:archive`.

## Rules

- Before implementing anything, read the relevant files in `openspec/specs/` and
  the active change's artifacts.
- Do not write feature code without an active change proposal that contains tasks.
- Keep changes minimal and scoped to the task being implemented.
- Treat `openspec/specs/` as the truth: reflect requirement changes there via the
  **sync** step, not by editing code silently.
- **Spec Quality Gate**: Never leave a delta spec or main spec in "Draft"
  status when applying or archiving. Delta specs MUST contain comprehensive
  BDD-style (`WHEN`/`THEN`) scenarios covering success, failure, and edge cases
  *before* implementation begins.
- **When generating `tasks.md` (via `/opsx:propose`)**, ALWAYS include this
  step at the end: "Update BACKLOG.md with the ✓ shipped status after
  archiving."
- Wrap all Markdown files (including `BACKLOG.md`) at ~80 columns to maintain
  consistent formatting.

## Change prioritization

When comparing possible changes, prefer the one that protects the core contract
earliest:

1. **Correctness foundations:** changes that prevent invalid lifecycle states,
   stale resolution, data loss, duplicate mutation, or broken at-least-once
   semantics.
2. **Specified feature completeness:** changes that make already-declared API or
   spec concepts fully real, such as lane partitioning.
3. **Operator and developer ergonomics:** polling loops, dashboards, CLI
   convenience, metrics, and other workflows around the core loop.
4. **Scale-out features:** concurrency, durable brokers, schedulers, and
   distributed behavior, after the underlying contract is strong enough to
   support them.

Do not add concurrency or durable-broker scope merely because a correctness
foundation enables it. Keep the enabling contract change separate and small.

## Strategy guardrail

`worklane` is a verified lifecycle queue: its core contract is the job
lifecycle, not a general transport API. The lifecycle is enqueue, reserve, ack,
retry, fail, lease expiry, dead-lettering, scheduling, and uniqueness. Backends
are interchangeable only when they pass the same behavioral conformance suite
for that lifecycle.

Use these tests before adding core surface area:

- A feature belongs in the broker contract only when every supported backend can
  implement it with the same observable lifecycle semantics.
- A backend feature is not portable until it can be expressed against SQL and
  Redis without changing public lifecycle semantics.
- If a feature can live as a handler, wrapper, CLI command, metric,
  documentation pattern, or application adapter, it stays out of the core
  broker contract.
  Example: a built-in HTTP webhook dispatcher stays out of core because making
  HTTP calls is an application-level concern that can be implemented as a
  standard job handler.

Non-goals for the core:

- It is not a general message bus.
- It is not a workflow engine at the broker layer.
- It is not a transport abstraction over every broker technology.
- It is not dashboard-first operations software.
- It does not promise exactly-once execution.
- It does not replace idempotent handlers.
- It does not own application architecture.

When choosing between strategic improvements, prefer lifecycle correctness,
then cross-backend behavioral conformance, then operator visibility into the
lifecycle, then developer ergonomics around existing primitives, then
higher-level orchestration patterns, then new backend breadth.

## Design principles

General and meant to outlast any specific module. The concrete gates below
(API stability, the Broker design gate) are applications of these.

- **Least commitment.** Do only what a present, concrete need requires.
  Introduce an abstraction together with its first real consumer — the consumer
  proves its shape; build no seam that has no user yet. Improvements noticed but
  not yet needed are recorded in `BACKLOG.md`, not folded into the current change.

- **Minimal contracts.** A shared contract (trait, interface, API) carries only
  what every implementation must honour. Implementation-specific conveniences
  stay behind the implementation — surfaced through a per-implementation adapter
  when tests or callers need them, never promoted onto the contract. A minimal
  contract is what keeps implementations substitutable and changes non-breaking.
  - *In practice:* when building shared infrastructure, sort the existing code by
    kind — lift what every implementation must honour up into the shared layer,
    keep conveniences down behind the implementation — and prove the sort with a
    reusable conformance suite that asserts only through the contract plus a
    per-implementation adapter, so conveniences cannot leak back in.

- **Separate knowledge by its kind.** What the system *does* (behaviour), *how*
  we build and evolve it (process, discipline), and what we have chosen *not to
  do yet* (deferred work) are different kinds of knowledge with different
  lifetimes. Behaviour is specified (`openspec/specs/`), discipline is governed
  (this file), deferred work is listed (`BACKLOG.md`). Put each where its kind
  belongs.

- **Promote only proven patterns.** A pattern used once is a hypothesis: record
  it and let the next use test it; promote it to a rule here only when it has
  held across more than one change. This applies to these rules themselves —
  govern slowly, from practice, not from a single good idea.
  - *Hypothesis under test (not yet a rule):* open questions are gated by kind,
    not banned. `openspec/specs/` (the
    contract / shipped truth) MUST NOT carry an open question at any point. A
    `design.md` MAY capture one; if its answer changes the delta spec or public
    API it MUST be resolved before **apply**, otherwise (implementation-level) it
    MAY ride into apply and MUST be resolved before **sync**. Test this on the
    next change before promoting it.

## API stability and evolution

An application of *Minimal contracts*: the public API is a long-term promise;
favor changes that can grow without breaking callers.

- Public types expected to gain fields or variants — `JobEnvelope`, `NewJob`,
  `JobContext`, `Reservation`, `DeadLetter`, and `Error` — MUST be
  `#[non_exhaustive]`.
- New fields and variants SHALL be added additively. Removing or renaming a
  public field, variant, method, or trait item is a breaking change that
  requires a major-version bump and rationale in the OpenSpec change.
- `JobEnvelope` is the durable, on-the-wire job format. Treat any field added to
  it as part of the storage contract every present and future broker must carry.

## Broker design gate

An application of *Minimal contracts* to the broker: the `Broker` trait is the
load-bearing abstraction; protect its portability.

- Any change to the `Broker` trait MUST be answerable as a SQL or Redis
  implementation (e.g. "how would Postgres do this with
  `SELECT … FOR UPDATE SKIP LOCKED`?"). Capture that answer in the change's
  `design.md`.
- Any change to the `Broker` trait or core job traits MUST record the rationale,
  portability argument, and rejected alternatives in the OpenSpec change's
  `design.md` or the synced capability spec.
- Do not add methods only an in-memory broker can satisfy: returning live
  references into broker state, requiring a full scan of all jobs, or assuming
  synchronous visibility.
- Do not treat the trait as stable until it has been validated against at least
  one durable backend without changing it. A required trait change is a signal
  to revise the contract while it is still cheap.
  - *Validated:* the `worklane-sqlite` durable broker passes the full
    `worklane-test` conformance suite (both tiers) with the
    `Broker` trait and every `worklane-core` type unchanged — the first
    durable-backend confirmation that the contract is portable, not
    in-memory-shaped.

## Boundary enforcement (executable governance)

The crate-graph invariants above are no longer prose alone. `crates/worklane-governance`
declares them as a [`tianheng`](https://crates.io/crates/tianheng) `Constitution` and a
CI job (`governance` in `.github/workflows/rust.yml`) reacts when the graph
drifts. Run it locally with:

```bash
cargo run -p worklane-governance -- check --manifest-path Cargo.toml
```

Currently enforced (severity `enforce`, the default):

- **worklane-core portability** — `worklane-core` must not depend on any other
  workspace crate. This is the *Broker design gate* and *Minimal contracts* made
  executable: the contract root stays backend-agnostic.
- **Backend substitutability** — each durable backend (`worklane-sqlite`,
  `worklane-postgres`, `worklane-redis`) may depend on only `worklane-core` among
  workspace crates, never on another backend or the facade. The rule scopes to
  normal `[dependencies]`, so the dev-dependency on `worklane-test` (the
  conformance suite that proves substitutability) is allowed without listing it.

Scope is deliberately *least-commitment*: only invariants this file already
asserts are encoded. Further candidates (facade-direction rules, intra-crate
module layering) are deferred in `BACKLOG.md`, not pre-built.

This gate governs one of two independent axes. `tianheng` reacts to
**architectural-boundary drift** — the spatial shape of the crate-graph and the
module-import graph: what may depend on, import, expose, or implement what.
**Public-API compatibility drift** — whether a public type may evolve without
breaking callers, including the `#[non_exhaustive]` discipline under *API
stability and evolution* above — is a *different kind* of invariant on a
separate axis (evolution over time, not spatial containment), with its own
observation source. It is **not** in `tianheng`'s scope, and its absence there
is not a gap to fill: attribute and semver policy belong to a semver-diff
reaction (e.g. `cargo-semver-checks`), kept as its own gate if and when that
axis is made reactive. Today it is held by the API-stability rules above plus
human review; do not push it into the architectural constitution.

Operating rule: both boundaries govern *workspace* dependencies, whose
membership `tianheng` derives from `cargo metadata` — so a newly added workspace
crate is governed by default, with no hand-maintained crate list to update.
Relaxing or removing a boundary follows the same discipline as a `Broker`-trait
change: record why here before doing it.

Rationale and rejected alternatives (per *Broker design gate* discipline of
recording the "how else"):

- **Why a binary + CI job, not a `#[test]`** — keeping enforcement out of
  `cargo test` lets the rules run as their own fast, dependency-free gate
  (alongside `lint`/`deny`) and matches `tianheng`'s intended `check
  --manifest-path` usage; the test-embedded alternative was rejected as harder
  to invoke locally with a clear exit code.
- **Why `restrict_workspace_dependencies_to`, not a hand-listed forbid** — the
  closed workspace allowlist derives its members from `cargo metadata`, so a new
  crate is forbidden by default until explicitly allowed. The earlier
  hand-maintained forbid list (a `WORKSPACE_CRATES` array) inverted the safe
  default: a crate never added to it was silently ungoverned. `tianheng`'s
  static dimension (圭表 / `guibiao`) carries the membership-derived rule, so
  the migration removed the array.
- **Why `tianheng`, the successor to `modou`** — `worklane` was `modou`'s first
  real consumer (the *least commitment* test: introduce the abstraction with its
  first consumer). `modou` has since evolved into the `tianheng` constellation,
  which decomposes the engine into static (圭表 / `guibiao`), semantic
  (渾儀 / `hunyi`), and runtime (漏刻 / `louke`) observation dimensions behind one
  `Constitution` + `run` shell. `worklane` tracks that evolution; the swap was a
  drop-in because `tianheng` re-exports the same `Constitution` / `CrateBoundary`
  / `run` surface, and `worklane` still declares only the static crate-graph
  dimension. Maturity risk is owned, not external.

This is process/discipline knowledge, so it lives here and not in
`openspec/specs/` — it changes no job-lifecycle behavior and carries no delta
spec.

## Language

- Write all OpenSpec artifacts (specs, proposals, designs, tasks) and code
  comments in **English** for consistency and tooling compatibility.
- Converse with users in the language they use (e.g. reply in Chinese to a
  Chinese prompt). The artifact language policy above is independent of this.

## Commit And Integration Governance

### Branch Commits

- Use Conventional Commits: `type(scope): summary`.
- Write the subject in English, lowercase imperative mood, at no more than 72 characters.
- Use the body to record motivation, important decisions, constraints, compatibility, and verification when that context exists.
- Do not append pull request or issue numbers to the subject or body.
- Development branches may contain multiple coherent commits because the pull request is squash-merged.

### Pull Requests

- Branch from `main` and open every change directly against `main`.
- Make the pull request title the intended squash commit subject.
- Give every pull request a non-empty body explaining why the change is needed, what changed, consequential decisions or tradeoffs, compatibility, and verification.
- Rebase the branch onto the current `main` before final verification.
- Do not introduce a release integration branch between a change and `main`.

### Squash Merges

- Squash-merge every verified pull request into `main`.
- Make the squash subject exactly the approved pull request title.
- Give every squash commit a non-empty body distilled from the approved pull request body.
- Do not append a pull request number, issue number, or URL to the squash subject or body.
- Every content-changing commit on `main`, including release preparation, must come from a squash-merged pull request.
- The imported `v0.1.0` root predates this governance and is the sole historical exception.
- Keep `main` releasable after every merge.

### Attribution

- Do not include AI, agent, model, tool, automation, or generation attribution in commits, pull requests, tags, changelogs, or release notes.
- A `Co-authored-by` trailer is allowed only for a real human contributor.

### Release Finalization

- Prepare release content in a pull request whose squash subject is exactly `chore(release): prepare X.Y.Z`.
- Give the release preparation squash commit a non-empty body describing scope, compatibility, metadata changes, and verification.
- Run the complete release gates after that commit reaches `main`.
- Finalize with annotated tag `vX.Y.Z` on that commit, with message exactly `release: X.Y.Z`.
- Push the tag without another commit. Release branches and empty release commits are not part of the flow.

## Build, test, and Definition of Done

Canonical commands, run from the workspace root:

- Build: `cargo build`
- Test: `cargo test`
- Lint: `cargo clippy --all-targets -- -D warnings`
- Format: `cargo fmt --all` (check with `cargo fmt --all --check`)

**Definition of Done** for any task or change: the code builds, `cargo test`
passes, `cargo clippy` is clean (no warnings), and `cargo fmt --all --check`
passes. Satisfy all four before checking a task off in `tasks.md` or archiving a
change.

## Project

`worklane` — typed background jobs for Rust services. A small, Rust-native async
job runner: typed payload → envelope → broker reserve → dispatch by kind → run
handler → ack / retry / fail / dead-letter.

- **Core principle:** protect the core loop. Everything outside
  enqueue/reserve/dispatch/ack/retry/fail/dead-letter is backlog — see `BACKLOG.md`.
- **Layout:** Cargo workspace. Core crates: `worklane-core` (traits, job model,
  envelope, errors), `worklane-memory` (in-memory broker for dev/tests),
  `worklane` (facade / public API, worker, client, workflow). Durable brokers:
  `worklane-sqlite`, `worklane-postgres`, `worklane-redis`. Supporting crates:
  `worklane-scheduler` (cron), `worklane-pubsub` (topic routing),
  `worklane-otel` (trace-context propagation), `worklane-cli` (operator CLI),
  `worklane-test` (shared broker conformance suite).
- **Terminology:** prefer `lane` over `queue` where it fits the brand.
- **Stack:** Rust (edition 2024), tokio async, serde payloads, tracing logs.
- **Source of truth:** job-lifecycle semantics live in `openspec/specs/` — decide
  them there deliberately; do not improvise them in code.
