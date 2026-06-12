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
