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

## API stability and evolution

An application of *Minimal contracts*: the public API is a long-term promise;
favor changes that can grow without breaking callers.

- Public types expected to gain fields or variants — `JobEnvelope`, `NewJob`,
  `JobContext`, `Reservation`, `DeadLetter`, and `Error` — MUST be
  `#[non_exhaustive]`.
- New fields and variants SHALL be added additively. Removing or renaming a
  public field, variant, method, or trait item is a breaking change that
  requires a major-version bump and an ADR recording the rationale.
- `JobEnvelope` is the durable, on-the-wire job format. Treat any field added to
  it as part of the storage contract every present and future broker must carry.

## Broker design gate

An application of *Minimal contracts* to the broker: the `Broker` trait is the
load-bearing abstraction; protect its portability.

- Any change to the `Broker` trait MUST be answerable as a SQL or Redis
  implementation (e.g. "how would Postgres do this with
  `SELECT … FOR UPDATE SKIP LOCKED`?"). Capture that answer in the change's
  `design.md`.
- Do not add methods only an in-memory broker can satisfy: returning live
  references into broker state, requiring a full scan of all jobs, or assuming
  synchronous visibility.
- Do not treat the trait as stable until it has been validated against at least
  one durable backend without changing it. A required trait change is a signal
  to revise the contract while it is still cheap.

## Language

- Write all OpenSpec artifacts (specs, proposals, designs, tasks) and code
  comments in **English** for consistency and tooling compatibility.
- Converse with users in the language they use (e.g. reply in Chinese to a
  Chinese prompt). The artifact language policy above is independent of this.

## Commits

- Use Conventional Commits: `type(scope): summary` — lowercase, imperative
  mood, summary ≤ 72 chars. Types: `feat`, `fix`, `docs`, `refactor`, `test`,
  `chore`, `build`, `ci`.
- Scope is optional; prefer the OpenSpec change name or capability
  (e.g. `feat(add-auth): add password reset endpoint`).
- Write commit messages in English (per the Language policy).
- Never bundle unrelated changes into one commit.

### Commit flow across the OpenSpec lifecycle

- **Propose:** one `docs(<change>): propose …` for proposal/design/specs/tasks.
- **Apply:** one or more `feat|fix(<change>): …`. Implement against the change's
  delta specs (the contract) and verify with the DoD plus tests that encode the
  spec scenarios. Commit per coherent, compiling milestone — not per checkbox,
  not one mega-commit.
- **Sync:** a `docs(specs): sync <change>` commit promoting the *verified* delta
  specs into `openspec/specs/`. Triggered by verification passing, never before;
  may run per shipped increment for long-running changes.
- **Archive:** a `chore(openspec): archive <change>` commit that files the
  completed change away.

During apply the delta spec in `changes/<change>/specs/` is the verification
target; `openspec/specs/` records shipped truth and changes only at sync.
For a short, copyable checklist, see `docs/development-flow.md`.

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
- **Layout:** Cargo workspace. v0.1 crates: `worklane-core` (traits, job model,
  envelope, errors), `worklane-memory` (in-memory broker for dev/tests),
  `worklane` (facade / public API).
- **Terminology:** prefer `lane` over `queue` where it fits the brand.
- **Stack:** Rust (edition 2024), tokio async, serde payloads, tracing logs.
- **Source of truth:** job-lifecycle semantics live in `openspec/specs/` — decide
  them there deliberately; do not improvise them in code.
