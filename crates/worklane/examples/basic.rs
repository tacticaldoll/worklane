//! A minimal end-to-end example: enqueue a typed job and run it to completion
//! against the in-memory broker.
//!
//! Run with: `cargo run -p worklane --example basic`

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use worklane::{Client, HandlerResult, Job, JobContext, Worker, async_trait};
use worklane_memory::InMemoryBroker;

#[derive(Serialize, Deserialize)]
struct SendEmail {
    user_id: u64,
}

struct SendEmailJob;

#[async_trait]
impl Job for SendEmailJob {
    type Payload = SendEmail;
    type Output = ();
    const KIND: &'static str = "send_email";

    async fn run(&self, ctx: JobContext, payload: SendEmail) -> HandlerResult<()> {
        println!(
            "sending email to user {} (attempt {})",
            payload.user_id,
            ctx.attempts + 1
        );
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let broker = Arc::new(InMemoryBroker::new());
    let client = Client::new(broker.clone());

    let mut worker = Worker::new(broker.clone());
    worker.register(SendEmailJob)?;

    client
        .enqueue::<SendEmailJob>(SendEmail { user_id: 42 })
        .await?;

    // The builder configures advanced options before enqueuing. `build_job`
    // serializes the payload eagerly, so it returns a `Result`; here we enqueue
    // immediately (no delay) so the example runs to completion in one pass.
    client
        .build_job::<SendEmailJob>(SendEmail { user_id: 43 })?
        .with_unique_key("welcome_email_43")
        .with_priority(10)
        .enqueue()
        .await?;

    let worker = worker.build()?;
    worker.run_until_idle().await?;

    println!(
        "done: {} live job(s), {} dead-lettered",
        broker.len(),
        broker.dead_letters().len()
    );
    Ok(())
}
