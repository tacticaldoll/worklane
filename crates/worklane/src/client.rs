use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use worklane_core::{Broker, Error, Job, JobId, Lane, LaneRegistry, NewJob, Result, to_payload};

/// The default `max_attempts` applied to enqueued jobs. Re-exported from
/// `worklane-core` so `worklane::DEFAULT_MAX_ATTEMPTS` is stable.
pub use worklane_core::DEFAULT_MAX_ATTEMPTS;

/// Enqueues typed jobs onto a broker.
#[must_use = "this value must be used"]
pub struct Client {
    pub(crate) broker: Arc<dyn Broker>,
    result_store: Option<Arc<dyn worklane_core::ResultStore>>,
    /// Optional Claim Check store: payloads larger than `offload_threshold` are
    /// offloaded here and replaced with a compact reference at enqueue.
    payload_store: Option<Arc<dyn worklane_core::PayloadStore>>,
    offload_threshold: usize,
    pub(crate) default_max_attempts: u32,
    pub(crate) lane: Lane,
    priority: u8,
    /// Optional set of known lanes. When `Some`, every enqueue path rejects a
    /// lane that is not a member; when `None`, any well-formed lane is accepted.
    lane_registry: Option<LaneRegistry>,
}

impl Client {
    /// Create a client over the given broker, enqueuing to the default lane.
    pub fn new(broker: Arc<dyn Broker>) -> Self {
        Client {
            broker,
            result_store: None,
            payload_store: None,
            offload_threshold: crate::DEFAULT_OFFLOAD_THRESHOLD,
            default_max_attempts: DEFAULT_MAX_ATTEMPTS,
            lane: Lane::default(),
            priority: 0,
            lane_registry: None,
        }
    }

    /// Enqueue a batch through the broker's
    /// [`BatchEnqueue`](worklane_core::BatchEnqueue) capability.
    ///
    /// Returns [`Error::UnsupportedCapability`](worklane_core::Error::UnsupportedCapability)
    /// when the configured broker does not provide batch enqueue. Internal helper
    /// behind the public batch and fan-in APIs so the capability lookup and its
    /// absence error live in exactly one place.
    pub(crate) async fn enqueue_batch(&self, jobs: Vec<NewJob>) -> Result<Vec<JobId>> {
        match self.broker.batch_enqueue() {
            Some(cap) => cap.enqueue_batch(jobs).await,
            None => Err(Error::UnsupportedCapability("batch enqueue".into())),
        }
    }

    /// Set an optional result store to retrieve job outputs (builder style).
    #[must_use = "this value must be used"]
    pub fn with_result_store(mut self, result_store: Arc<dyn worklane_core::ResultStore>) -> Self {
        self.result_store = Some(result_store);
        self
    }

    /// Offload large payloads to a [`PayloadStore`](worklane_core::PayloadStore)
    /// (Claim Check, builder style): a payload larger than the offload threshold
    /// (default [`DEFAULT_OFFLOAD_THRESHOLD`](crate::DEFAULT_OFFLOAD_THRESHOLD)) is
    /// stored externally and replaced with a compact reference, keeping the queue
    /// lean. The worker must be given the **same** store via
    /// [`Worker::with_payload_store`](crate::Worker::with_payload_store) to resolve
    /// it. Set the threshold with [`with_offload_threshold`](Self::with_offload_threshold).
    ///
    /// Note: if a unique-key enqueue deduplicates to an existing job, a payload
    /// offloaded for the dropped job is orphaned in the store (a later sweep
    /// reclaims it) — combine large payloads and unique keys with that in mind.
    #[must_use = "this value must be used"]
    pub fn with_payload_store(
        mut self,
        payload_store: Arc<dyn worklane_core::PayloadStore>,
    ) -> Self {
        self.payload_store = Some(payload_store);
        self
    }

    /// Set the payload-offload threshold in bytes (builder style). Only meaningful
    /// with [`with_payload_store`](Self::with_payload_store). Payloads larger than
    /// this are offloaded; payloads at or below it stay inline.
    #[must_use = "this value must be used"]
    pub fn with_offload_threshold(mut self, threshold: usize) -> Self {
        self.offload_threshold = threshold;
        self
    }

    /// Offload `payload` if a store is configured and it exceeds the threshold,
    /// returning a compact reference; otherwise return it unchanged.
    pub(crate) async fn maybe_offload(&self, payload: Vec<u8>) -> Result<Vec<u8>> {
        match &self.payload_store {
            Some(store) if payload.len() > self.offload_threshold => {
                let key = store.put(&payload).await?;
                Ok(worklane_core::claim_check::make_reference(&key))
            }
            _ if payload.len() > worklane_core::spi::MAX_ENVELOPE_BYTES => {
                Err(Error::Serialization(format!(
                    "job payload is {} bytes, over the {}-byte inline limit; \
                     configure a PayloadStore to offload large payloads",
                    payload.len(),
                    worklane_core::spi::MAX_ENVELOPE_BYTES
                )))
            }
            _ => Ok(payload),
        }
    }

    pub(crate) async fn cleanup_deduped_offload(
        &self,
        submitted: JobId,
        returned: JobId,
        payload: &[u8],
    ) {
        if submitted == returned {
            return;
        }
        self.cleanup_offload(submitted, payload, "enqueue deduplicated")
            .await;
    }

    pub(crate) async fn cleanup_offload(&self, job_id: JobId, payload: &[u8], reason: &str) {
        let Some(store) = &self.payload_store else {
            return;
        };
        let Some(key) = worklane_core::claim_check::reference_key(payload) else {
            return;
        };
        if let Err(err) = store.delete(key).await {
            tracing::warn!(
                job_id = %job_id,
                %reason,
                error = %err,
                "failed to clean up claim-check payload"
            );
        }
    }

    pub(crate) async fn cleanup_offloads<'a>(
        &self,
        payloads: impl IntoIterator<Item = (JobId, &'a [u8])>,
        reason: &str,
    ) {
        for (job_id, payload) in payloads {
            self.cleanup_offload(job_id, payload, reason).await;
        }
    }

    /// Set the default `max_attempts` for enqueued jobs (builder style).
    #[must_use = "this value must be used"]
    pub fn with_max_attempts(mut self, max_attempts: u32) -> Self {
        self.default_max_attempts = max_attempts;
        self
    }

    /// Set the lane this client enqueues to (builder style). Defaults to
    /// [`Lane::default`].
    #[must_use = "this value must be used"]
    pub fn with_lane(mut self, lane: Lane) -> Self {
        self.lane = lane;
        self
    }

    /// Set the priority for enqueued jobs (builder style). Defaults to `0`.
    /// Higher values mean higher priority.
    #[must_use = "this value must be used"]
    pub fn with_priority(mut self, priority: u8) -> Self {
        self.priority = priority;
        self
    }

    /// Restrict enqueues to a known set of lanes (builder style).
    ///
    /// By default a client accepts any well-formed lane, so a typo like
    /// `"emial"` silently enqueues to a lane no worker reserves. With a registry
    /// configured, every enqueue path rejects a lane that is not a member,
    /// returning [`Error::UnknownLane`] and submitting nothing. This is opt-in:
    /// an unconfigured client keeps the dynamic-lane behavior unchanged.
    #[must_use = "this value must be used"]
    pub fn with_lane_registry(mut self, registry: LaneRegistry) -> Self {
        self.lane_registry = Some(registry);
        self
    }

    /// Verify `lane` against the configured registry, if any. Returns
    /// [`Error::UnknownLane`] for a non-member; `Ok(())` when no registry is
    /// configured or the lane is known.
    pub(crate) fn check_lane(&self, lane: &Lane) -> Result<()> {
        match &self.lane_registry {
            Some(reg) if !reg.contains(lane) => Err(Error::UnknownLane(lane.to_string())),
            _ => Ok(()),
        }
    }

    /// Enqueue a typed job to the client's configured lane. The payload is
    /// serialized before submission; a serialization failure returns an error
    /// and submits nothing.
    pub async fn enqueue<J: Job>(&self, payload: J::Payload) -> Result<JobId> {
        self.enqueue_to::<J>(self.lane.clone(), payload).await
    }

    /// Enqueue a typed job to `lane` for this call only, overriding the client's
    /// configured lane without changing it. The payload is serialized before
    /// submission; a serialization failure returns an error and submits nothing.
    pub async fn enqueue_to<J: Job>(&self, lane: Lane, payload: J::Payload) -> Result<JobId> {
        self.enqueue_inner::<J>(lane, Duration::ZERO, None, payload)
            .await
    }

    /// Enqueue a typed job to multiple lanes simultaneously. The payload is
    /// serialized once and fanned out. The jobs are submitted as a single
    /// atomic batch. A serialization failure returns an error and submits
    /// nothing.
    pub async fn enqueue_to_lanes<J: Job>(
        &self,
        lanes: impl IntoIterator<Item = Lane>,
        payload: J::Payload,
    ) -> Result<Vec<JobId>> {
        self.build_job::<J>(payload)?.enqueue_to_lanes(lanes).await
    }

    /// Enqueue a typed job to the client's configured lane, visible only after
    /// `delay`. The payload is serialized before submission; a serialization
    /// failure returns an error and submits nothing.
    pub async fn enqueue_in<J: Job>(&self, delay: Duration, payload: J::Payload) -> Result<JobId> {
        self.enqueue_inner::<J>(self.lane.clone(), delay, None, payload)
            .await
    }

    /// Enqueue a typed job to the client's configured lane with a uniqueness
    /// `key`: while a live job already holds the key, this returns that job's id
    /// and enqueues nothing. The payload is serialized before submission; a
    /// serialization failure returns an error and submits nothing.
    pub async fn enqueue_unique<J: Job>(
        &self,
        key: impl Into<String>,
        payload: J::Payload,
    ) -> Result<JobId> {
        self.enqueue_inner::<J>(self.lane.clone(), Duration::ZERO, Some(key.into()), payload)
            .await
    }

    /// Retrieve and deserialize the output of a successful job from the result store.
    /// Returns `Ok(None)` if no result is stored (e.g. the job is pending, failed, or expired).
    /// Returns an error if no result store is configured or if deserialization fails.
    pub async fn get_result<T: serde::de::DeserializeOwned>(
        &self,
        job_id: &JobId,
    ) -> Result<Option<T>> {
        let Some(store) = &self.result_store else {
            return Err(worklane_core::Error::ResultStore(
                "no result store configured".to_string(),
            ));
        };
        match self.broker.classify(*job_id).await? {
            worklane_core::JobState::CompletedOrUnknown => {}
            worklane_core::JobState::Live | worklane_core::JobState::DeadLettered => {
                return Ok(None);
            }
        }
        match store.get(job_id).await? {
            Some(bytes) => Ok(Some(worklane_core::from_payload(&bytes)?)),
            None => Ok(None),
        }
    }

    /// Create a `JobBuilder` to configure a job with specific properties before enqueuing.
    /// The payload is serialized immediately; a serialization failure returns an error.
    pub fn build_job<J: Job>(&self, payload: J::Payload) -> Result<JobBuilder<'_>> {
        let bytes = to_payload(&payload)?;
        let job = NewJob::new(self.lane.clone(), J::KIND, bytes, self.default_max_attempts)
            .with_priority(self.priority);
        Ok(JobBuilder { client: self, job })
    }

    /// The single enqueue path: serialize, build a `NewJob` for `lane` with the
    /// given `delay` and optional uniqueness key, and submit it.
    pub(crate) async fn enqueue_inner<J: Job>(
        &self,
        lane: Lane,
        delay: Duration,
        unique_key: Option<String>,
        payload: J::Payload,
    ) -> Result<JobId> {
        let mut builder = self
            .build_job::<J>(payload)?
            .with_lane(lane)
            .with_delay(delay);
        if let Some(key) = unique_key {
            builder = builder.with_unique_key(key);
        }
        builder.enqueue().await
    }
    /// Spawn a fan-in: a collection of independent dependency jobs that run in
    /// parallel, followed by a callback that runs only after all dependencies
    /// have completed successfully — receiving their aggregated outputs. The
    /// entire topology is enqueued atomically.
    ///
    /// The callback job `CB` is declared with `Payload = FanInResults<C>`: at fire
    /// time it receives the caller `context` plus each dependency's opaque output
    /// bytes, in dependency order. The callback cannot be passed as a pre-built
    /// `JobBuilder` because its payload (the results) does not exist at submit
    /// time; instead the callback kind comes from `CB::KIND` and its
    /// lane/priority/max_attempts from the client's defaults.
    ///
    /// **The callback fires at-least-once, not exactly-once — it MUST be
    /// idempotent.** Its `fanin:{fanin_id}:callback` key dedups only within the
    /// live window (like every `unique_key`): the key is released once the
    /// callback completes, so a watcher generation that is redelivered after the
    /// callback already ran (e.g. the watcher's own lease expired before it
    /// acked) can enqueue the callback a second time. This is consistent with the
    /// at-least-once delivery contract; design the callback to tolerate running
    /// more than once, exactly as for any handler.
    ///
    /// This uses the default [`FanInPolicy`](crate::workflow::FanInPolicy) (poll
    /// every 10s for up to ~24h). Use [`fan_in_with_policy`](Self::fan_in_with_policy)
    /// to tune the poll cadence or the pending-window bound.
    ///
    /// Requires the broker to provide the
    /// [`BatchEnqueue`](worklane_core::BatchEnqueue) capability; returns
    /// [`Error::UnsupportedCapability`](worklane_core::Error::UnsupportedCapability)
    /// when it does not.
    pub async fn fan_in<CB, C>(
        &self,
        fanin_id: String,
        dependencies: impl IntoIterator<Item = JobBuilder<'_>>,
        context: C,
    ) -> Result<()>
    where
        CB: Job<Payload = crate::workflow::FanInResults<C>>,
        C: serde::Serialize,
    {
        self.fan_in_with_policy::<CB, C>(fanin_id, dependencies, context, Default::default())
            .await
    }

    /// [`fan-in`](Self::fan_in) with an explicit
    /// [`FanInPolicy`](crate::workflow::FanInPolicy) controlling the watcher's poll
    /// cadence (`poll_delay_secs`) and the maximum number of polls
    /// (`max_generations`) before the fan-in fails. The worst-case wall-clock a
    /// fan-in stays pending is `poll_delay_secs * max_generations`.
    ///
    /// Requires the broker to provide the
    /// [`BatchEnqueue`](worklane_core::BatchEnqueue) capability; returns
    /// [`Error::UnsupportedCapability`](worklane_core::Error::UnsupportedCapability)
    /// when it does not.
    pub async fn fan_in_with_policy<CB, C>(
        &self,
        fanin_id: String,
        dependencies: impl IntoIterator<Item = JobBuilder<'_>>,
        context: C,
        policy: crate::workflow::FanInPolicy,
    ) -> Result<()>
    where
        CB: Job<Payload = crate::workflow::FanInResults<C>>,
        C: serde::Serialize,
    {
        // Guard the public tuning surface: a zero poll delay would make the
        // watcher re-poll with no wait, burning generations in a tight loop.
        // (max_generations >= 1 is enforced by the watcher payload constructor.)
        if policy.poll_delay_secs == 0 {
            return Err(Error::Broker(
                "FanInPolicy.poll_delay_secs must be >= 1; a zero delay would re-poll \
                 with no wait between generations"
                    .to_string(),
            ));
        }

        let mut deps = Vec::new();
        let mut dep_ids = Vec::new();
        let mut seen_dep_ids = HashSet::new();
        for dep in dependencies {
            let job = dep.into_inner();
            // A fan-in dependency must not carry a unique_key: the atomic batch
            // enqueue below deduplicates unique_key collisions, which could drop
            // the member while its id still rides in the watcher payload — a
            // phantom id that later classifies as CompletedOrUnknown and falsely
            // completes the fan-in. Reject before submitting anything so every
            // recorded dependency id denotes a persisted job.
            if job.unique_key.is_some() {
                return Err(Error::Broker(
                    "a fan-in dependency must not set a unique_key: the atomic batch \
                     enqueue could deduplicate the member away, leaving the watcher \
                     with a dependency id that was never persisted"
                        .to_string(),
                ));
            }
            if !seen_dep_ids.insert(job.id) {
                return Err(Error::Broker(format!(
                    "a fan-in dependency id appears more than once: {}",
                    job.id
                )));
            }
            dep_ids.push(job.id);
            deps.push(job);
        }

        // The sealed constructor enforces the invariants (generation 1, empty
        // capture set) and rejects a fan-in with no dependencies, submitting nothing.
        let watcher_payload = crate::workflow::FanInWatcherPayload::new(
            fanin_id,
            dep_ids,
            self.lane.to_string(),
            CB::KIND.to_string(),
            to_payload(&context)?,
            self.default_max_attempts,
            self.priority,
            policy.poll_delay_secs,
            policy.max_generations,
        )?;

        let watcher_job = self
            .build_job::<crate::workflow::FanInWatcherJob>(watcher_payload)?
            .into_inner();

        let mut batch = deps;
        batch.push(watcher_job);

        // Reject the whole topology before submitting if any dependency or the
        // watcher targets an unregistered lane.
        for job in &batch {
            self.check_lane(&job.lane)?;
        }

        // Offload oversized payloads (Claim Check) before submitting — every
        // dependency payload and the watcher's (which carries the callback
        // context) — so the fan-in uses the same offload path as a plain enqueue.
        // Without this an oversized dependency or context would bloat the queue or
        // be rejected by the envelope cap mid-topology. The watcher's self-rescheduled
        // generations and the final callback are offloaded on their own enqueue
        // paths. No-op unless a payload store is configured.
        for job in &mut batch {
            let payload = std::mem::take(&mut job.payload);
            job.payload = self.maybe_offload(payload).await?;
        }

        let submitted: Vec<(JobId, Vec<u8>)> = batch
            .iter()
            .map(|job| (job.id, job.payload.clone()))
            .collect();
        let batch_result = self.enqueue_batch(batch).await;
        if let Err(err) = batch_result {
            self.cleanup_offloads(
                submitted
                    .iter()
                    .map(|(job_id, payload)| (*job_id, payload.as_slice())),
                "fan-in batch enqueue failed",
            )
            .await;
            return Err(err);
        }

        Ok(())
    }
}

/// A builder for configuring and enqueuing a job with custom properties.
#[must_use = "this value must be used"]
pub struct JobBuilder<'a> {
    pub(crate) client: &'a Client,
    pub(crate) job: NewJob,
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use serde::{Deserialize, Serialize};
    use std::collections::HashSet;
    use std::sync::Mutex;
    use worklane_core::{BatchEnqueue, JobContext, JobState, PayloadStore, ResultStore};
    use worklane_memory::InMemoryBroker;

    #[derive(Serialize, Deserialize)]
    struct EmailJob {
        address: String,
    }

    #[async_trait]
    impl Job for EmailJob {
        const KIND: &'static str = "email";
        type Payload = Self;
        type Output = ();

        async fn run(
            &self,
            _ctx: worklane_core::JobContext,
            _payload: Self::Payload,
        ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
            Ok(())
        }
    }

    #[derive(Default)]
    struct CountingPayloadStore {
        keys: Mutex<HashSet<String>>,
        puts: Mutex<usize>,
        deletes: Mutex<usize>,
    }

    #[async_trait]
    impl PayloadStore for CountingPayloadStore {
        async fn put(&self, _payload: &[u8]) -> Result<String> {
            let key = JobId::new().to_string();
            self.keys.lock().unwrap().insert(key.clone());
            *self.puts.lock().unwrap() += 1;
            Ok(key)
        }

        async fn get(&self, _key: &str) -> Result<Option<Vec<u8>>> {
            Ok(None)
        }

        async fn delete(&self, key: &str) -> Result<()> {
            self.keys.lock().unwrap().remove(key);
            *self.deletes.lock().unwrap() += 1;
            Ok(())
        }
    }

    #[derive(Default)]
    struct FailingBroker;

    #[async_trait]
    impl BatchEnqueue for FailingBroker {
        async fn enqueue_batch(&self, _jobs: Vec<NewJob>) -> Result<Vec<JobId>> {
            Err(Error::Broker("forced batch enqueue failure".to_string()))
        }
    }

    #[async_trait]
    impl Broker for FailingBroker {
        async fn enqueue(&self, _job: NewJob) -> Result<JobId> {
            Err(Error::Broker("forced enqueue failure".to_string()))
        }

        fn batch_enqueue(&self) -> Option<&dyn BatchEnqueue> {
            Some(self)
        }

        async fn reserve(&self, _lane: &Lane) -> Result<Option<worklane_core::Reservation>> {
            Ok(None)
        }

        async fn ack(&self, _receipt: worklane_core::ReservationReceipt) -> Result<()> {
            Err(Error::Broker("forced ack failure".to_string()))
        }

        async fn retry(
            &self,
            _receipt: worklane_core::ReservationReceipt,
            _delay: Duration,
        ) -> Result<()> {
            Err(Error::Broker("forced retry failure".to_string()))
        }

        async fn defer(
            &self,
            _receipt: worklane_core::ReservationReceipt,
            _delay: Duration,
        ) -> Result<()> {
            Err(Error::Broker("forced defer failure".to_string()))
        }

        async fn extend(&self, _receipt: worklane_core::ReservationReceipt) -> Result<()> {
            Err(Error::Broker("forced extend failure".to_string()))
        }

        async fn fail(
            &self,
            _receipt: worklane_core::ReservationReceipt,
            _error: String,
        ) -> Result<()> {
            Err(Error::Broker("forced fail failure".to_string()))
        }

        async fn classify(&self, _id: JobId) -> Result<JobState> {
            Ok(JobState::CompletedOrUnknown)
        }
        // No dead-letter / queue-stats capability: the default `None` accessors
        // apply (this mock only exercises the core-loop failure paths).
    }

    #[derive(Serialize, Deserialize)]
    struct CallbackJob;

    #[async_trait]
    impl Job for CallbackJob {
        const KIND: &'static str = "callback";
        type Payload = crate::workflow::FanInResults<()>;
        type Output = ();

        async fn run(
            &self,
            _ctx: JobContext,
            _payload: Self::Payload,
        ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
            Ok(())
        }
    }

    #[derive(Default)]
    struct TestResultStore {
        data: Mutex<std::collections::HashMap<JobId, Vec<u8>>>,
    }

    #[async_trait]
    impl ResultStore for TestResultStore {
        async fn store(&self, job_id: &JobId, result: &[u8]) -> Result<()> {
            self.data.lock().unwrap().insert(*job_id, result.to_vec());
            Ok(())
        }

        async fn get(&self, job_id: &JobId) -> Result<Option<Vec<u8>>> {
            Ok(self.data.lock().unwrap().get(job_id).cloned())
        }
    }

    #[tokio::test]
    async fn enqueue_to_lanes_fans_out_batch() {
        let broker = Arc::new(InMemoryBroker::new());
        let client = Client::new(broker.clone());

        let payload = EmailJob {
            address: "user@example.com".to_string(),
        };

        let lanes = vec![
            Lane::try_from("lane1").unwrap(),
            Lane::try_from("lane2").unwrap(),
            Lane::try_from("lane3").unwrap(),
        ];

        let ids = client
            .enqueue_to_lanes::<EmailJob>(lanes, payload)
            .await
            .unwrap();
        assert_eq!(ids.len(), 3, "fan-out should return three ids");

        // Each fan-out job is a distinct job: the returned ids must all differ
        // (a shared JobId would collide in the result store, in `classify`, and
        // in the durable brokers' by-id state).
        let unique: std::collections::HashSet<_> = ids.iter().collect();
        assert_eq!(unique.len(), 3, "fan-out ids must all be distinct");

        // Verify the jobs arrived on the distinct lanes, and the reserved
        // envelopes carry distinct ids matching the returned ones.
        let mut reserved_ids = std::collections::HashSet::new();
        for lane_name in ["lane1", "lane2", "lane3"] {
            let l = Lane::try_from(lane_name).unwrap();
            let r = broker.reserve(&l).await.unwrap();
            let env = r.expect("job must be reservable on the lane").envelope;
            assert!(
                ids.contains(&env.id),
                "reserved id must be one of the returned ids"
            );
            reserved_ids.insert(env.id);
        }
        assert_eq!(
            reserved_ids.len(),
            3,
            "the reserved envelopes must have distinct ids"
        );
    }

    #[tokio::test]
    async fn build_job_composability() {
        let broker = Arc::new(InMemoryBroker::new());
        let client = Client::new(broker.clone());

        let payload = EmailJob {
            address: "user@example.com".to_string(),
        };

        // Enqueue with delay and unique key
        let id = client
            .build_job::<EmailJob>(payload)
            .unwrap()
            .with_delay(Duration::from_secs(60))
            .with_unique_key("test-key")
            .with_priority(5)
            .enqueue()
            .await
            .unwrap();

        // Since it is delayed, it should not be reservable immediately
        let lane = Lane::default();
        let reserve_result = broker.reserve(&lane).await.unwrap();
        assert!(
            reserve_result.is_none(),
            "delayed job should not be reservable immediately"
        );

        // Enqueue another with same unique key
        let payload2 = EmailJob {
            address: "other@example.com".to_string(),
        };
        let id2 = client
            .build_job::<EmailJob>(payload2)
            .unwrap()
            .with_unique_key("test-key")
            .enqueue()
            .await
            .unwrap();

        assert_eq!(id, id2, "unique key should deduplicate");
    }

    #[tokio::test]
    async fn unique_key_dedup_cleans_up_offloaded_payload() {
        let broker = Arc::new(InMemoryBroker::new());
        let store = Arc::new(CountingPayloadStore::default());
        let client = Client::new(broker)
            .with_payload_store(store.clone())
            .with_offload_threshold(0);

        let id = client
            .build_job::<EmailJob>(EmailJob {
                address: "first@example.com".to_string(),
            })
            .unwrap()
            .with_unique_key("dedup-cleanup")
            .enqueue()
            .await
            .unwrap();
        let deduped = client
            .build_job::<EmailJob>(EmailJob {
                address: "second@example.com".to_string(),
            })
            .unwrap()
            .with_unique_key("dedup-cleanup")
            .enqueue()
            .await
            .unwrap();

        assert_eq!(id, deduped);
        assert_eq!(*store.puts.lock().unwrap(), 2);
        assert_eq!(*store.deletes.lock().unwrap(), 1);
        assert_eq!(store.keys.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn failed_single_enqueue_cleans_up_offloaded_payload() {
        let store = Arc::new(CountingPayloadStore::default());
        let client = Client::new(Arc::new(FailingBroker))
            .with_payload_store(store.clone())
            .with_offload_threshold(0);

        let err = client
            .build_job::<EmailJob>(EmailJob {
                address: "single@example.com".to_string(),
            })
            .unwrap()
            .enqueue()
            .await
            .unwrap_err();

        assert!(matches!(err, Error::Broker(_)));
        assert_eq!(*store.puts.lock().unwrap(), 1);
        assert_eq!(*store.deletes.lock().unwrap(), 1);
        assert!(store.keys.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn failed_fanout_enqueue_cleans_up_all_offloaded_payloads() {
        let store = Arc::new(CountingPayloadStore::default());
        let client = Client::new(Arc::new(FailingBroker))
            .with_payload_store(store.clone())
            .with_offload_threshold(0);

        let err = client
            .build_job::<EmailJob>(EmailJob {
                address: "fanout@example.com".to_string(),
            })
            .unwrap()
            .enqueue_to_lanes(vec![
                Lane::try_from("a").unwrap(),
                Lane::try_from("b").unwrap(),
            ])
            .await
            .unwrap_err();

        assert!(matches!(err, Error::Broker(_)));
        assert_eq!(*store.puts.lock().unwrap(), 2);
        assert_eq!(*store.deletes.lock().unwrap(), 2);
        assert!(store.keys.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn failed_fanin_submission_cleans_up_dependency_and_watcher_offloads() {
        let store = Arc::new(CountingPayloadStore::default());
        let client = Client::new(Arc::new(FailingBroker))
            .with_payload_store(store.clone())
            .with_offload_threshold(0);
        let dependency = client
            .build_job::<EmailJob>(EmailJob {
                address: "dep@example.com".to_string(),
            })
            .unwrap();

        let err = client
            .fan_in::<CallbackJob, _>("cleanup-fan-in".to_string(), vec![dependency], ())
            .await
            .unwrap_err();

        assert!(matches!(err, Error::Broker(_)));
        assert_eq!(*store.puts.lock().unwrap(), 2);
        assert_eq!(*store.deletes.lock().unwrap(), 2);
        assert!(store.keys.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn failed_fanin_callback_enqueue_cleans_up_offloaded_payload() {
        let payload_store = Arc::new(CountingPayloadStore::default());
        let result_store = Arc::new(TestResultStore::default());
        let client = Arc::new(
            Client::new(Arc::new(FailingBroker))
                .with_payload_store(payload_store.clone())
                .with_offload_threshold(0),
        );
        let dep_id = JobId::new();
        result_store.store(&dep_id, b"done").await.unwrap();
        let watcher = crate::workflow::FanInWatcherJob {
            client,
            result_store,
        };
        let payload = crate::workflow::FanInWatcherPayload::new(
            "callback-cleanup".to_string(),
            vec![dep_id],
            "default".to_string(),
            CallbackJob::KIND.to_string(),
            worklane_core::to_payload(&()).unwrap(),
            1,
            0,
            0,
            10,
        )
        .unwrap();
        let ctx = JobContext::new(
            JobId::new(),
            Lane::default(),
            1,
            1,
            0,
            "watcher".to_string(),
            None,
        );

        let err = watcher.run(ctx, payload).await.unwrap_err();

        assert!(format!("{err}").contains("forced batch enqueue failure"));
        assert_eq!(*payload_store.puts.lock().unwrap(), 1);
        assert_eq!(*payload_store.deletes.lock().unwrap(), 1);
        assert!(payload_store.keys.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn get_result_is_gated_by_broker_lifecycle() {
        let broker = Arc::new(InMemoryBroker::new());
        let store = Arc::new(TestResultStore::default());
        let client = Client::new(broker.clone()).with_result_store(store.clone());
        let lane = Lane::default();

        let live_id = broker
            .enqueue(worklane_core::NewJob::new(
                lane.clone(),
                EmailJob::KIND,
                b"null".to_vec(),
                1,
            ))
            .await
            .unwrap();
        store
            .store(&live_id, &worklane_core::to_payload(&()).unwrap())
            .await
            .unwrap();
        let live_result: Option<()> = client.get_result(&live_id).await.unwrap();
        assert!(
            live_result.is_none(),
            "live jobs must hide stale result bytes"
        );

        let reservation = broker
            .reserve(&lane)
            .await
            .unwrap()
            .expect("live job is reservable");
        broker
            .fail(reservation.receipt, "boom".to_string())
            .await
            .unwrap();
        let dead_result: Option<()> = client.get_result(&live_id).await.unwrap();
        assert!(
            dead_result.is_none(),
            "dead-lettered jobs must hide stale result bytes"
        );

        let complete_id = broker
            .enqueue(worklane_core::NewJob::new(
                lane.clone(),
                EmailJob::KIND,
                b"null".to_vec(),
                1,
            ))
            .await
            .unwrap();
        let reservation = broker
            .reserve(&lane)
            .await
            .unwrap()
            .expect("complete job is reservable");
        store
            .store(&complete_id, &worklane_core::to_payload(&()).unwrap())
            .await
            .unwrap();
        broker.ack(reservation.receipt).await.unwrap();
        let completed_result: Option<()> = client.get_result(&complete_id).await.unwrap();
        assert!(
            completed_result.is_some(),
            "completed jobs may return stored result bytes"
        );
    }

    #[tokio::test]
    async fn unique_key_fanout_to_many_lanes_is_rejected() {
        let broker = Arc::new(InMemoryBroker::new());
        let client = Client::new(broker.clone());

        let lanes = vec![Lane::try_from("u1").unwrap(), Lane::try_from("u2").unwrap()];

        let result = client
            .build_job::<EmailJob>(EmailJob {
                address: "u@example.com".to_string(),
            })
            .unwrap()
            .with_unique_key("shared")
            .enqueue_to_lanes(lanes)
            .await;

        assert!(
            result.is_err(),
            "a unique key fanned to multiple lanes must be rejected, not silently collapsed"
        );
        // Nothing was enqueued on either lane.
        for lane_name in ["u1", "u2"] {
            let l = Lane::try_from(lane_name).unwrap();
            assert!(broker.reserve(&l).await.unwrap().is_none());
        }
    }

    #[tokio::test]
    async fn unique_key_fanout_to_single_lane_is_allowed() {
        let broker = Arc::new(InMemoryBroker::new());
        let client = Client::new(broker.clone());

        // One lane cannot collapse, so a unique key is fine.
        let ids = client
            .build_job::<EmailJob>(EmailJob {
                address: "u@example.com".to_string(),
            })
            .unwrap()
            .with_unique_key("solo")
            .enqueue_to_lanes(vec![Lane::try_from("s1").unwrap()])
            .await
            .unwrap();
        assert_eq!(ids.len(), 1);
    }

    #[tokio::test]
    async fn registry_allows_a_registered_lane() {
        let broker = Arc::new(InMemoryBroker::new());
        let client = Client::new(broker.clone())
            .with_lane_registry(LaneRegistry::new([Lane::try_from("email").unwrap()]));

        let id = client
            .enqueue_to::<EmailJob>(
                Lane::try_from("email").unwrap(),
                EmailJob {
                    address: "u@example.com".to_string(),
                },
            )
            .await
            .expect("registered lane should enqueue");
        // The job is reservable on the registered lane.
        let r = broker
            .reserve(&Lane::try_from("email").unwrap())
            .await
            .unwrap();
        assert_eq!(r.unwrap().envelope.id, id);
    }

    #[tokio::test]
    async fn registry_rejects_an_unregistered_lane_and_submits_nothing() {
        let broker = Arc::new(InMemoryBroker::new());
        let client = Client::new(broker.clone())
            .with_lane_registry(LaneRegistry::new([Lane::try_from("email").unwrap()]));

        let err = client
            .enqueue_to::<EmailJob>(
                Lane::try_from("emial").unwrap(),
                EmailJob {
                    address: "u@example.com".to_string(),
                },
            )
            .await
            .expect_err("a typo'd lane must be rejected");
        assert!(matches!(err, Error::UnknownLane(l) if l == "emial"));
        // Nothing was submitted, on the typo'd lane or the registered one.
        assert!(
            broker
                .reserve(&Lane::try_from("emial").unwrap())
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            broker
                .reserve(&Lane::try_from("email").unwrap())
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn no_registry_preserves_dynamic_lanes() {
        let broker = Arc::new(InMemoryBroker::new());
        // No registry configured: a typo'd lane is still accepted (today's behavior).
        let client = Client::new(broker.clone());
        client
            .enqueue_to::<EmailJob>(
                Lane::try_from("emial").unwrap(),
                EmailJob {
                    address: "u@example.com".to_string(),
                },
            )
            .await
            .expect("without a registry any well-formed lane is accepted");
        assert!(
            broker
                .reserve(&Lane::try_from("emial").unwrap())
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn registry_fanout_is_all_or_nothing() {
        let broker = Arc::new(InMemoryBroker::new());
        let client = Client::new(broker.clone())
            .with_lane_registry(LaneRegistry::new([Lane::try_from("email").unwrap()]));

        let err = client
            .enqueue_to_lanes::<EmailJob>(
                vec![
                    Lane::try_from("email").unwrap(),
                    Lane::try_from("sms").unwrap(),
                ],
                EmailJob {
                    address: "u@example.com".to_string(),
                },
            )
            .await
            .expect_err("an unregistered lane in the fan-out must fail the whole call");
        assert!(matches!(err, Error::UnknownLane(l) if l == "sms"));
        // No job on either lane, including the registered one.
        for lane_name in ["email", "sms"] {
            let l = Lane::try_from(lane_name).unwrap();
            assert!(broker.reserve(&l).await.unwrap().is_none());
        }
    }

    #[tokio::test]
    async fn build_job_fanout_with_properties() {
        let broker = Arc::new(InMemoryBroker::new());
        let client = Client::new(broker.clone());

        let payload = EmailJob {
            address: "fanout@example.com".to_string(),
        };

        let lanes = vec![Lane::try_from("f1").unwrap(), Lane::try_from("f2").unwrap()];

        let ids = client
            .build_job::<EmailJob>(payload)
            .unwrap()
            .with_priority(10)
            .enqueue_to_lanes(lanes)
            .await
            .unwrap();
        assert_eq!(ids.len(), 2);

        for lane_name in ["f1", "f2"] {
            let l = Lane::try_from(lane_name).unwrap();
            let _r = broker
                .reserve(&l)
                .await
                .unwrap()
                .expect("should be reservable");
        }
    }
}
