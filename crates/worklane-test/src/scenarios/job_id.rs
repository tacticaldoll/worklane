use super::{job, lane};
use crate::BrokerContractHarness;
use worklane_core::{Broker, NewJob, QueueStats};

/// Enqueue is idempotent on `JobId`: a second enqueue of a job carrying an id a
/// live job already has returns that same id and creates **no** second job. This
/// is the broker's "no two live jobs share an id" invariant, enforced.
pub async fn enqueue_is_idempotent_on_job_id<H: BrokerContractHarness>(h: &H) {
    let b = h.broker();
    let l = lane("job_id_dedup");

    let first = job("job_id_dedup");
    let id = first.id;
    let returned = b.enqueue(first).await.unwrap();
    assert_eq!(returned, id);

    // A NewJob reusing the same id (e.g. a client retrying an enqueue with the
    // same job) must dedup to the existing job, not create a second.
    let mut again = NewJob::new(l.clone(), "ok", b"null".to_vec(), 3);
    again.id = id;
    let returned_again = b.enqueue(again).await.unwrap();
    assert_eq!(
        returned_again, id,
        "a colliding id returns the existing job's id"
    );

    // Exactly one job is live on the lane.
    assert_eq!(
        b.pending_count(&l).await.unwrap(),
        1,
        "a duplicate-id enqueue must not create a second job"
    );
}

/// Distinct ids are independent: two jobs with different ids both enqueue.
pub async fn distinct_job_ids_both_enqueue<H: BrokerContractHarness>(h: &H) {
    let b = h.broker();
    let l = lane("job_id_distinct");
    b.enqueue(job("job_id_distinct")).await.unwrap();
    b.enqueue(job("job_id_distinct")).await.unwrap();
    assert_eq!(
        b.pending_count(&l).await.unwrap(),
        2,
        "two jobs with distinct (freshly minted) ids both enqueue"
    );
}
