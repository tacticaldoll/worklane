# Summary

Describe the change and why it is needed.

## OpenSpec

- Related change:
- Specs synced: yes / no / not needed

## Verification

Commands run:

- [ ] `cargo build`
- [ ] `cargo test`
- [ ] `cargo clippy --all-targets -- -D warnings`
- [ ] `cargo fmt --all --check`

Additional release-facing checks, when relevant:

- [ ] `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps`
- [ ] `cargo package --workspace`
- [ ] `cargo +1.85.0 check --workspace --all-targets`

## Impact

- Public API impact:
- Broker contract impact:
- Storage / migration impact:
- MSRV impact:
- Compatibility notes:
