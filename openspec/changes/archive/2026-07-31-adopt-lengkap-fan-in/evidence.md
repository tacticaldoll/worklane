# Lengkap Adoption Evidence

## Registry Boundary

Cargo resolved the adopted product from crates.io:

- `lengkap` 0.1.0, checksum
  `4fbbfa2cd5a7dac040e04c8473dd38fcc7516a1e82cff1c33e8598abc55e8aee`
- `lengkap-contract` 0.1.0, checksum
  `a0c658452115f0b1b7756f2f4683524d5c3dd9b4257f410fe579ddf8ed25be6d`

Both lock entries use
`registry+https://github.com/rust-lang/crates.io-index`. No adjacent path or
Cargo patch source remains.

The formal implementation promotes the fit proven on local spike commit
`cc9da6652beb8b5b804054d48f5a3207fef23d5e`. The source adapter is unchanged
except that the dependency now resolves from the registry and the OpenSpec
artifacts describe a mergeable adoption.

## Boundary Result

- Lengkap receives only an in-memory assembly and located produced or
  impossible findings.
- Worklane retains broker classification and result-store reads.
- Worklane restores and serializes the existing watcher checkpoint shape.
- Prior checkpoint tuple order is retained, and newly captured values append in
  dependency order.
- Worklane retains generation limits, delayed rescheduling, failure wording,
  callback payload construction, payload offload, and callback enqueueing.
- Public Worklane API, serialized payload shape, core and broker traits,
  backends, and Tianheng law are unchanged.
- The `worklane` facade retains `worklane-core` as its only workspace
  dependency.

## Focused Equivalence

The following passed:

- checkpoint slot and tuple-order unit test: 1 test;
- fan-in watcher integration suite: 7 tests;
- workflow integration suite: 6 tests.

The multi-generation eviction scenario additionally proves that callback
results remain in dependency order after checkpoint restoration.

## Complete Verification

The following passed:

- `cargo build`
- `cargo test`
- `cargo clippy --all-targets -- -D warnings`
- `cargo fmt --all --check`
- `cargo +1.85.0 check --workspace --all-targets`
- `cargo deny check`
- `cargo run -p worklane-governance -- check --manifest-path Cargo.toml`
- `openspec validate --all --strict`
- `cargo package --workspace --allow-dirty` with an isolated target directory

Cargo-deny retained the repository's existing duplicate-version and unused ISC
allowance warnings but returned success. Tianheng returned clean with the
existing informational coverage report of 5 uncovered crates out of 13; no
accepted boundary or coverage state changed.
