# Development Flow

This project uses OpenSpec for spec-driven development. `AGENTS.md` is the
authoritative contributor and agent guide; this file is a short checklist for
running one change with a matching commit rhythm.

## Branches and Releases

`main` is release-only: it holds exactly one commit per published version,
`release: X.Y.Z`, tagged `vX.Y.Z`. No development happens on `main`.

Development for the next version happens on a `release/X.Y.Z` branch, started
from the previous `release:` commit. Work reaches a release through two squashes:

1. **Feature branch → `release/X.Y.Z` (squash).** Develop a coherent milestone
   on a feature branch; its WIP commits are free-form and discarded by the
   squash. The PR squash-merges into the dev branch as **one** Conventional
   Commit (`feat|fix|docs|refactor|…`) obeying every `## Commits` rule in
   `AGENTS.md`. The PR title becomes the squash subject, so it is equally bound
   by the self-describing, no-numbers, and no-AI-signature rules.
2. **`release/X.Y.Z` → `main` (release cut).** When the version is published to
   crates.io, collapse the dev branch into a single `release: X.Y.Z` commit on
   `main` and tag it `vX.Y.Z`.

**Honesty rule:** a `release: X.Y.Z` commit's tree must equal the source
actually published to crates.io for that version. Anchor the cut to the real
publish point (verified against the crates.io publish time), not to whatever the
dev-branch tip happens to be at the moment of the cut.

The release cut drops the dev branch's per-PR commits from `main` — including
the OpenSpec `propose`/`sync`/`archive` commits — but nothing is lost: the
granular history stays on `release/X.Y.Z`, and the spec artifacts under
`openspec/specs/` plus archived changes remain in the tree.

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

Under the squash model above, one milestone = one PR = one squashed commit on
the dev branch. The granularity below therefore describes what belongs in a
single squashed PR, not raw feature-branch WIP commits (those are discarded by
the squash). The OpenSpec lifecycle phases map onto PRs, so a long change may
land as several squashed commits (e.g. a `propose` PR, one or more apply PRs, a
`sync` PR) rather than one.

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
