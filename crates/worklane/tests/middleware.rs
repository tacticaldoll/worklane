//! Middleware wraps handler dispatch onion-style: outermost-first in, last out;
//! a middleware may short-circuit the handler.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use worklane::{
    Client, Error, HandlerResult, Job, JobContext, Middleware, Next, Result, Worker, async_trait,
};
use worklane_memory::InMemoryBroker;

#[derive(Serialize, Deserialize)]
struct Unit;

type Log = Arc<Mutex<Vec<String>>>;

/// Records `"{tag}-before"` / `"{tag}-after"` around the rest of the chain.
struct Recorder {
    tag: &'static str,
    log: Log,
}

#[async_trait]
impl Middleware for Recorder {
    async fn handle(&self, ctx: JobContext, payload: &[u8], next: Next<'_>) -> Result<Vec<u8>> {
        self.log
            .lock()
            .unwrap()
            .push(format!("{}-before", self.tag));
        let result = next.run(ctx, payload).await;
        self.log.lock().unwrap().push(format!("{}-after", self.tag));
        result
    }
}

/// Short-circuits: returns an error without calling `next`, so the handler never runs.
struct Block;

#[async_trait]
impl Middleware for Block {
    async fn handle(&self, _ctx: JobContext, _payload: &[u8], _next: Next<'_>) -> Result<Vec<u8>> {
        Err(Error::Handler("blocked by middleware".into()))
    }
}

struct LoggingJob {
    log: Log,
}

#[async_trait]
impl Job for LoggingJob {
    type Payload = Unit;
    type Output = ();
    const KIND: &'static str = "logged";
    async fn run(&self, _ctx: JobContext, _p: Unit) -> HandlerResult<()> {
        self.log.lock().unwrap().push("handler".to_string());
        Ok(())
    }
}

struct CountingJob {
    runs: Arc<AtomicUsize>,
}

#[async_trait]
impl Job for CountingJob {
    type Payload = Unit;
    type Output = ();
    const KIND: &'static str = "counted";
    async fn run(&self, _ctx: JobContext, _p: Unit) -> HandlerResult<()> {
        self.runs.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn middleware_wraps_the_handler_outermost_first() {
    let broker = Arc::new(InMemoryBroker::new());
    Client::new(broker.clone())
        .enqueue::<LoggingJob>(Unit)
        .await
        .unwrap();

    let log: Log = Arc::new(Mutex::new(Vec::new()));
    let mut worker = Worker::new(broker.clone())
        .with_middleware(Arc::new(Recorder {
            tag: "A",
            log: log.clone(),
        }))
        .with_middleware(Arc::new(Recorder {
            tag: "B",
            log: log.clone(),
        }));
    worker.register(LoggingJob { log: log.clone() }).unwrap();
    let worker = worker.build().unwrap();
    worker.run_until_idle().await.unwrap();

    assert_eq!(
        *log.lock().unwrap(),
        vec!["A-before", "B-before", "handler", "B-after", "A-after"],
        "first-registered middleware is the outermost layer"
    );
}

#[tokio::test]
async fn middleware_can_short_circuit_the_handler() {
    let broker = Arc::new(InMemoryBroker::new());
    // One attempt: the short-circuit failure dead-letters it.
    Client::new(broker.clone())
        .with_max_attempts(1)
        .enqueue::<CountingJob>(Unit)
        .await
        .unwrap();

    let runs = Arc::new(AtomicUsize::new(0));
    let mut worker = Worker::new(broker.clone()).with_middleware(Arc::new(Block));
    worker.register(CountingJob { runs: runs.clone() }).unwrap();
    let worker = worker.build().unwrap();
    worker.run_until_idle().await.unwrap();

    assert_eq!(
        runs.load(Ordering::SeqCst),
        0,
        "the handler must not run when short-circuited"
    );
    assert_eq!(
        broker.dead_letters().len(),
        1,
        "the short-circuit error dead-letters the job"
    );
}
