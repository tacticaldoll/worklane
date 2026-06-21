use worklane_core::{Broker, Lane, NewJob};

/// Build a validated [`Lane`] from a known-good test name.
pub(crate) fn lane(name: &str) -> Lane {
    Lane::try_from(name).expect("valid test lane")
}

pub(crate) fn job(name: &str) -> NewJob {
    NewJob::new(lane(name), "ok", b"null".to_vec(), 3)
}

/// Helper to enqueue and immediately fail a job to create a dead-letter record.
pub(crate) async fn dead_letter<B: Broker + ?Sized>(
    b: &B,
    new: NewJob,
    error: &str,
) -> worklane_core::JobId {
    let lane = new.lane.clone();
    let id = b.enqueue(new).await.unwrap();
    let r = b.reserve(&lane).await.unwrap().expect("job");
    b.fail(r.receipt, error.to_string()).await.unwrap();
    id
}

mod capabilities;
pub use capabilities::*;
mod lifecycle;
pub use lifecycle::*;
mod dead_letters;
pub use dead_letters::*;
mod classify;
pub use classify::*;
mod requeue;
pub use requeue::*;
mod concurrent;
pub use concurrent::*;
mod unique;
pub use unique::*;
mod batch;
pub use batch::*;
mod scheduled;
pub use scheduled::*;
mod priority;
pub use priority::*;
mod timed;
pub use timed::*;
mod retention;
pub use retention::*;
mod poison;
pub use poison::*;
mod pending;
pub use pending::*;
mod defer;
pub use defer::*;
mod job_id;
pub use job_id::*;
