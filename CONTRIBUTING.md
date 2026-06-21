# Contributing

Thanks for helping improve `worklane`.

This repository uses OpenSpec for spec-driven development. The current behavior
contracts live in `openspec/specs/`; active changes live in
`openspec/changes/`. Start with [`AGENTS.md`](AGENTS.md) and
[`docs/development-flow.md`](docs/development-flow.md). Release operators should
also use [`docs/release-checklist.md`](docs/release-checklist.md).

## Workflow

Use the repository flow:

```text
explore -> propose -> apply -> sync -> archive
```

Feature work and public API changes need an OpenSpec change before
implementation. Bug fixes that change observable behavior should also update or
add the relevant spec scenario.

Keep changes small and focused. The `Broker` trait is the central portability
contract, so changes to it must explain how SQL and Redis implementations can
honor the same behavior.

## Verification

Run the Definition of Done before marking a task complete:

```sh
cargo build
cargo test
cargo clippy --all-targets -- -D warnings
cargo fmt --all --check
```

For release-facing documentation changes, also run:

```sh
RUSTDOCFLAGS="-D warnings -D missing_docs" cargo doc --workspace --no-deps
```

Live PostgreSQL and Redis tests are gated by
`WORKLANE_POSTGRES_TEST_URL` and `WORKLANE_REDIS_TEST_URL`. They skip when the
variables are unset and fail when the variables are set but unreachable.

## Pull Requests

Pull requests should include:

- The related OpenSpec change or a note explaining why none is needed.
- The verification commands that were run.
- Any public API, storage, broker-contract, or MSRV impact.
- Migration or compatibility notes for users when relevant.
