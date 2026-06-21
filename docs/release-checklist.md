# Release Checklist

Checklist for the first crates.io release of the workspace crates.

Run commands from the workspace root unless a step says otherwise. Do not publish
from a dirty worktree.

## 1. External Blockers

Crates.io name ownership is a hard blocker. Confirm every name is available or
already owned by the intended publisher before publishing:

- `worklane`
- `worklane-core`
- `worklane-memory`
- `worklane-sqlite`
- `worklane-postgres`
- `worklane-redis`
- `worklane-scheduler`
- `worklane-pubsub`
- `worklane-otel`
- `worklane-metrics`
- `worklane-cli`
- `worklane-test`

Use crates.io search or `cargo info <crate-name>`. If any name is owned by
someone else, stop and decide whether to rename before publishing. Do not
publish a partial set under mixed ownership.

Also confirm:

- The crates.io token belongs to the intended owner.
- The repository URL in `Cargo.toml` is public and correct.
- `CHANGELOG.md` has the release entry.
- `SECURITY.md` has a valid reporting path.
- `docs/known-limitations.md` states current support boundaries.
- `docs/benchmarks.md` describes the benchmark scope and command.

## 2. Local Release Gates

Run the normal Definition of Done:

```sh
cargo fmt --all --check
cargo build
cargo test
cargo clippy --all-targets -- -D warnings
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

Run release-facing gates:

```sh
RUSTDOCFLAGS="-D warnings -D missing_docs" cargo doc --workspace --no-deps
RUSTDOCFLAGS="-D warnings -D missing_docs" cargo doc --workspace --no-deps --all-features
CARGO_TARGET_DIR=/tmp/worklane-package-check cargo package --workspace
cargo +1.85.0 check --workspace --all-targets
cargo +1.85.0 check --workspace --all-targets --all-features
cargo check --workspace --all-targets --all-features
cargo metadata --format-version 1 --all-features > /tmp/worklane-deny-metadata.json
cargo deny check --metadata-path /tmp/worklane-deny-metadata.json advisories bans licenses sources
```

The package gate intentionally verifies packaged tarballs instead of only
checking the path workspace. The MSRV gate verifies the declared
`rust-version = "1.85"` contract. The all-features gates protect optional public
surfaces such as TLS support from drifting outside CI coverage.

## 3. Publish Order

Publish in this order:

1. `worklane-core`
2. `worklane-test`
3. `worklane-memory`
4. `worklane-sqlite`
5. `worklane-postgres`
6. `worklane-redis`
7. `worklane`
8. `worklane-scheduler`
9. `worklane-pubsub`
10. `worklane-otel`
11. `worklane-metrics`
12. `worklane-cli`

For the first release, dependent crates cannot all be dry-run before publishing
anything: `cargo publish --dry-run` resolves versioned workspace dependencies
from the crates.io index, so `worklane-test` cannot dry-run until
`worklane-core` is already visible in the index. The workspace package gate in
section 2 is the all-crates preflight; after that, run a dry-run immediately
before publishing each crate whose dependencies are already published.

For each crate in order, run:

```sh
cargo publish --dry-run -p <crate>
cargo publish -p <crate>
```

If a dry-run fails, fix the issue before publishing that crate.

After each publish, wait for the crates.io index to show the crate before
publishing a crate that depends on it:

```sh
cargo info <crate>@0.1.0
```

This order publishes the shared contract first, then the conformance suite used
by broker package verification, then brokers and the facade, then optional
integration crates and the CLI.

## 4. Post-Publish Verification

After all crates are published:

- Confirm each crates.io page shows version `0.1.0`, license, repository,
  keywords, categories, and description.
- Confirm docs.rs builds every crate without failed builds.
- Confirm `cargo add worklane worklane-memory` works in a scratch project.
- Confirm the README install snippet resolves against crates.io versions.
- Tag the release after the published set is complete:

```sh
git tag v0.1.0
git push origin v0.1.0
```

Create a GitHub release from the tag using the `CHANGELOG.md` `0.1.0` entry.
