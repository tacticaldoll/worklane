# Development Flow

This project uses OpenSpec for spec-driven development. `AGENTS.md` is the
authoritative contributor and agent guide; this file is a short checklist for
running one change with a matching commit rhythm.

## One Change

1. Explore the current specs and code before editing:
   - `openspec list --specs`
   - `openspec list`
   - read the relevant files under `openspec/specs/`
2. Propose the change:
   - `openspec new change "<change-name>"`
   - write `proposal.md`, `design.md`, `tasks.md`, and delta specs
   - commit as `docs(<change-name>): propose <summary>`
3. Apply the change:
   - implement against `openspec/changes/<change-name>/specs/`
   - check off tasks only after the relevant code and tests pass
   - commit coherent compiling milestones as `feat(<change-name>): <summary>` or
     `fix(<change-name>): <summary>`
4. Sync verified semantics:
   - promote the verified delta specs into `openspec/specs/`
   - commit as `docs(specs): sync <change-name>`
5. Archive the completed change:
   - `openspec archive <change-name>`
   - commit as `chore(openspec): archive <change-name>`

## Commit Granularity

Apply commits should be larger than individual task checkboxes and smaller than
an entire risky feature. Prefer one commit per coherent milestone that builds,
tests, and preserves the spec contract.

Good milestones:

- add a new public API plus focused tests
- add one broker behavior and its spec scenarios
- wire an implementation through the facade after lower-level tests pass

Avoid:

- committing unrelated docs, refactors, and behavior together
- checking off `tasks.md` before the Definition of Done passes
- syncing `openspec/specs/` before the implementation has been verified

## Change Priority

When two changes compete, prefer the one that protects correctness of the core
loop before extending visible behavior. A contract fix such as reservation
receipts should come before a feature-completeness change such as lane
partitioning, because it prevents future durable brokers or concurrent workers
from inheriting an unsafe resolution model.

Use this order:

1. correctness foundations
2. specified feature completeness
3. operator and developer ergonomics
4. scale-out features

## Definition of Done

Run these from the workspace root before checking off a task, syncing specs, or
archiving a change:

```bash
cargo build
cargo test
cargo clippy --all-targets -- -D warnings
cargo fmt --all --check
cargo deny check         # supply-chain gate (advisories/licenses/bans/sources)
cargo run -p worklane-governance -- check --manifest-path Cargo.toml  # crate-boundary gate
```
