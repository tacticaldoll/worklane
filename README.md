# worklane

> Typed background jobs for Rust services.

`worklane` is a small, Rust-native async background job runner: enqueue typed
jobs and run workers with retries, ack/fail semantics, dead-lettering, and
pluggable brokers.

> **Status: pre-alpha / experimental.** The API is being designed and is not yet
> functional. Do not depend on it yet.

## Planned core loop

```text
typed payload -> envelope -> broker reserve -> dispatch by kind
              -> run handler -> ack / retry / fail / dead-letter
```

## Workspace

| Crate | Role |
|-------|------|
| `worklane` | Public-facing facade API |
| `worklane-core` | Traits, job model, envelope, errors |
| `worklane-memory` | In-memory broker for dev/tests |

## Development

This project uses spec-driven development via
[OpenSpec](https://github.com/Fission-AI/OpenSpec). See [`AGENTS.md`](AGENTS.md)
for the workflow, `openspec/specs/` for the authoritative job-lifecycle
semantics, and [`BACKLOG.md`](BACKLOG.md) for deferred ideas.

## License

Licensed under either of [MIT](LICENSE-MIT) or
[Apache-2.0](LICENSE-APACHE) at your option.
