# worklane

> Typed background jobs for Rust services.

`worklane` is a small, Rust-native async background job runner: enqueue typed
jobs and run workers with retries, ack/fail semantics, dead-lettering, and
pluggable brokers.

> **Status: experimental (0.0.1).** The core loop works with the in-memory
> broker; the API may still change. Durable brokers are not built yet.

## Core loop

```text
typed payload -> envelope -> broker reserve -> dispatch by kind
              -> run handler -> ack / retry / fail / dead-letter
```

## Quick start

```rust
use std::sync::Arc;
use serde::{Deserialize, Serialize};
use worklane::{async_trait, Client, HandlerResult, Job, JobContext, Worker};
use worklane_memory::InMemoryBroker;

#[derive(Serialize, Deserialize)]
struct SendEmail { user_id: u64 }

struct SendEmailJob;

#[async_trait]
impl Job for SendEmailJob {
    type Payload = SendEmail;
    const KIND: &'static str = "send_email";
    async fn run(&self, _ctx: JobContext, payload: SendEmail) -> HandlerResult {
        println!("sending email to user {}", payload.user_id);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let broker = Arc::new(InMemoryBroker::new());
    let client = Client::new(broker.clone());

    let mut worker = Worker::new(broker.clone());
    worker.register(SendEmailJob)?;

    client.enqueue::<SendEmailJob>(SendEmail { user_id: 42 }).await?;
    worker.run_until_idle().await?;
    Ok(())
}
```

Run it with `cargo run -p worklane --example basic`.

> **Delivery is at-least-once.** A job may run more than once (after a lease
> expiry or a crash), so **handlers must be idempotent.**

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
semantics, [`docs/development-flow.md`](docs/development-flow.md) for the
change/commit checklist, and [`BACKLOG.md`](BACKLOG.md) for deferred ideas.

## License

Licensed under either of [MIT](LICENSE-MIT) or
[Apache-2.0](LICENSE-APACHE) at your option.
