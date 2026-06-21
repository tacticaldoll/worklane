use crate::Client;
use async_trait::async_trait;
use worklane_core::{Job, JobContext, JobId, Result};

/// The Workflow Canvas extension trait.
/// Provides building blocks for Celery-style topologies (Chains, Chords) built entirely
/// in user-space over the core primitives.
#[async_trait]
pub trait Canvas {
    /// Create a `JobBuilder` for an idempotent sequential continuation.
    /// Allows mutating the continuation job (e.g., setting trace context) before enqueueing.
    fn build_continuation<'a, J: Job>(
        &'a self,
        ctx: &JobContext,
        payload: J::Payload,
    ) -> Result<crate::client::JobBuilder<'a>>;

    /// Create a `JobBuilder` for an idempotent continuation with an explicit key.
    /// Allows mutating the continuation job before enqueueing.
    fn build_continuation_keyed<'a, J: Job>(
        &'a self,
        key: String,
        payload: J::Payload,
    ) -> Result<crate::client::JobBuilder<'a>>;
}

#[async_trait]
impl Canvas for Client {
    fn build_continuation<'a, J: Job>(
        &'a self,
        ctx: &JobContext,
        payload: J::Payload,
    ) -> Result<crate::client::JobBuilder<'a>> {
        let key = format!("chain:{}:{}", ctx.id, J::KIND);
        Ok(self
            .build_job::<J>(payload)?
            .with_lane(ctx.lane.clone())
            .with_unique_key(key))
    }

    fn build_continuation_keyed<'a, J: Job>(
        &'a self,
        key: String,
        payload: J::Payload,
    ) -> Result<crate::client::JobBuilder<'a>> {
        Ok(self.build_job::<J>(payload)?.with_unique_key(key))
    }
}

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use worklane_core::NewJob;

/// The payload delivered to a chord callback: the caller's `context` plus each
/// dependency's opaque output bytes, in dependency order.
///
/// A chord is fan-out-then-aggregate — the callback runs *over the dependency
/// results*, not merely after they complete. Each entry of `results` is one
/// dependency's raw `Job::Output` bytes (as stored in the `ResultStore`); the
/// callback deserializes each itself (e.g. via `from_payload`). The callback job
/// is declared as `Job<Payload = ChordResults<C>>` and submitted with
/// [`Client::chord`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChordResults<C> {
    /// The caller-supplied context passed to [`Client::chord`].
    pub context: C,
    /// Each dependency's opaque output bytes, in dependency order.
    pub results: Vec<Vec<u8>>,
}

fn chord_results_payload(
    callback_payload: &[u8],
    results: Vec<Vec<u8>>,
) -> worklane_core::Result<Vec<u8>> {
    let context: serde_json::Value = serde_json::from_slice(callback_payload)
        .map_err(|e| worklane_core::Error::Serialization(e.to_string()))?;
    serde_json::to_vec(&serde_json::json!({ "context": context, "results": results }))
        .map_err(|e| worklane_core::Error::Serialization(e.to_string()))
}

/// Tuning for a chord watcher's poll loop, passed to
/// [`Client::chord_with_policy`](crate::Client::chord_with_policy).
///
/// The watcher re-checks its dependencies every `poll_delay_secs` and gives up
/// after `max_generations` polls, so the worst-case wall-clock a chord stays
/// pending before failing is `poll_delay_secs * max_generations`. The delay is in
/// whole seconds (matching the watcher's self-reschedule granularity); both
/// fields must be `>= 1`. [`Default`] polls every 10s for up to ~24h.
#[derive(Debug, Clone, Copy)]
pub struct ChordPolicy {
    /// Seconds between two consecutive dependency polls. Must be `>= 1`.
    pub poll_delay_secs: u64,
    /// Maximum number of polls before the chord fails. Must be `>= 1`.
    pub max_generations: u32,
}

impl Default for ChordPolicy {
    fn default() -> Self {
        Self {
            poll_delay_secs: 10,
            max_generations: 8640, // ~24h at 10s per poll
        }
    }
}

/// Payload for the internal `ChordWatcherJob`.
///
/// The fields are crate-private and [`ChordWatcherPayload::new`] is the only
/// supported constructor for normal callers, but this is still a serialized job
/// payload at the broker boundary. A caller with direct broker access can submit
/// malformed bytes, so the watcher validates its invariants again when it runs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChordWatcherPayload {
    /// The stable ID of the chord, used for the callback's idempotency key
    pub(crate) chord_id: String,
    /// The full list of chord dependencies, retained across generations (it is
    /// not shrunk; `collected` records which have been captured). Capture is
    /// monotonic: each generation captures any newly-available dependency output
    /// into `collected`, so a later eviction (e.g. a result TTL) of an
    /// already-captured dependency cannot regress the chord. A dependency that
    /// completed but whose result was evicted *before* it was ever captured fails
    /// the chord, because aggregation requires the value.
    ///
    /// Invariant: every id here MUST originate from [`Client::chord`], which
    /// rejects a dependency carrying a `unique_key` and submits the dependencies
    /// in the same atomic batch — so each id denotes a job that was actually
    /// persisted, and `classify` returning `CompletedOrUnknown` for it can only
    /// mean "acked", never "never enqueued".
    pub(crate) dependencies: Vec<JobId>,
    /// The generation of this watcher, used for the watcher's own idempotency key
    pub(crate) generation: u32,
    /// Delay between polling attempts in seconds
    pub(crate) poll_delay_secs: u64,
    /// Maximum number of generations (polling attempts) before failing
    pub(crate) max_generations: u32,

    /// Dependency outputs captured so far, as `(dependency id, output bytes)`.
    /// Carried forward across generations so a value captured early survives a
    /// later eviction (monotonic capture). When this covers every dependency the
    /// watcher aggregates the values, in `dependencies` order, into the callback.
    pub(crate) collected: Vec<(JobId, Vec<u8>)>,

    // Callback details (because J: Job is erased here).
    pub(crate) callback_lane: String,
    pub(crate) callback_kind: String,
    /// The serialized caller context (`C`); the watcher wraps it together with
    /// the captured results into a `ChordResults<C>` payload at fire time.
    pub(crate) callback_payload: Vec<u8>,
    pub(crate) callback_max_attempts: u32,
    pub(crate) callback_priority: u8,
}

impl ChordWatcherPayload {
    /// Build the initial watcher payload for a chord: generation 1, nothing
    /// captured yet. The only supported constructor — it enforces the invariants
    /// the watcher relies on (start at generation 1 with an empty capture set) and
    /// rejects degenerate inputs (no dependencies, or a zero generation bound) up
    /// front rather than letting them surface as a confusing mid-flight failure.
    ///
    /// The watcher captures each dependency's output for aggregation: a
    /// still-running dependency stays live and keeps the chord pending; a
    /// dead-lettered one fails the chord fast; a dependency whose result is
    /// evicted before capture fails the chord (the result TTL must outlive the
    /// chord until capture).
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        chord_id: String,
        dependencies: Vec<JobId>,
        callback_lane: String,
        callback_kind: String,
        callback_payload: Vec<u8>,
        callback_max_attempts: u32,
        callback_priority: u8,
        poll_delay_secs: u64,
        max_generations: u32,
    ) -> worklane_core::Result<Self> {
        if dependencies.is_empty() {
            return Err(worklane_core::Error::Broker(
                "a chord must have at least one dependency".to_string(),
            ));
        }
        if max_generations == 0 {
            return Err(worklane_core::Error::Broker(
                "a chord watcher needs at least one generation (max_generations >= 1)".to_string(),
            ));
        }
        Ok(Self {
            chord_id,
            dependencies,
            collected: Vec::new(),
            generation: 1,
            poll_delay_secs,
            max_generations,
            callback_lane,
            callback_kind,
            callback_payload,
            callback_max_attempts,
            callback_priority,
        })
    }

    /// The chord's dependency ids, in aggregation order (read-only).
    pub fn dependencies(&self) -> &[JobId] {
        &self.dependencies
    }

    /// The dependency outputs captured so far, as `(id, bytes)` (read-only).
    pub fn collected(&self) -> &[(JobId, Vec<u8>)] {
        &self.collected
    }

    fn validate(&self) -> std::result::Result<(), String> {
        if self.chord_id.is_empty() {
            return Err("chord watcher payload is malformed: chord_id is empty".to_string());
        }
        if self.dependencies.is_empty() {
            return Err(format!(
                "chord watcher payload for {} is malformed: no dependencies",
                self.chord_id
            ));
        }
        if self.generation == 0 {
            return Err(format!(
                "chord watcher payload for {} is malformed: generation must be positive",
                self.chord_id
            ));
        }
        if self.max_generations == 0 || self.generation > self.max_generations {
            return Err(format!(
                "chord watcher payload for {} is malformed: generation {} exceeds max {}",
                self.chord_id, self.generation, self.max_generations
            ));
        }
        if self.callback_kind.is_empty() {
            return Err(format!(
                "chord watcher payload for {} is malformed: callback kind is empty",
                self.chord_id
            ));
        }
        self.callback_lane
            .parse::<worklane_core::Lane>()
            .map_err(|err| {
                format!(
                    "chord watcher payload for {} is malformed: invalid callback lane: {err}",
                    self.chord_id
                )
            })?;

        let mut dependencies = HashSet::with_capacity(self.dependencies.len());
        for dep_id in &self.dependencies {
            if !dependencies.insert(*dep_id) {
                return Err(format!(
                    "chord watcher payload for {} is malformed: duplicate dependency {}",
                    self.chord_id, dep_id
                ));
            }
        }

        let mut captured = HashSet::with_capacity(self.collected.len());
        for (dep_id, _) in &self.collected {
            if !dependencies.contains(dep_id) {
                return Err(format!(
                    "chord watcher payload for {} is malformed: captured unknown dependency {}",
                    self.chord_id, dep_id
                ));
            }
            if !captured.insert(*dep_id) {
                return Err(format!(
                    "chord watcher payload for {} is malformed: duplicate captured dependency {}",
                    self.chord_id, dep_id
                ));
            }
        }
        Ok(())
    }
}

/// A self-rescheduling watcher job that polls the `ResultStore` for a chord's dependencies.
/// Once all dependencies are complete, it dispatches the callback job.
pub struct ChordWatcherJob {
    /// Client used to enqueue the callback or reschedule the watcher.
    pub client: Arc<Client>,
    /// Result store used to inspect dependency completion and payload bytes.
    pub result_store: Arc<dyn worklane_core::ResultStore>,
}

#[async_trait]
impl Job for ChordWatcherJob {
    type Payload = ChordWatcherPayload;
    type Output = ();
    const KIND: &'static str = "worklane:chord_watcher";

    async fn run(
        &self,
        ctx: JobContext,
        payload: Self::Payload,
    ) -> worklane_core::HandlerResult<Self::Output> {
        if let Err(msg) = payload.validate() {
            return Err(msg.into());
        }
        // Capture each dependency's output value (not just its presence). Capture
        // is monotonic: a value captured in an earlier generation is carried
        // forward in `collected`, so a later eviction of that result cannot
        // regress the chord. Only deps whose value has never been captured are
        // polled.
        let mut collected = payload.collected.clone();
        let mut captured: HashMap<JobId, usize> = collected
            .iter()
            .enumerate()
            .map(|(index, (id, _))| (*id, index))
            .collect();
        let mut pending = Vec::new();
        for dep_id in &payload.dependencies {
            if captured.contains_key(dep_id) {
                continue;
            }
            // Classify first. ResultStore bytes alone do not prove completion: a
            // worker writes results before acking, and a stale ack can leave bytes
            // behind while the broker still considers the job live.
            match self.client.broker.classify(*dep_id).await? {
                worklane_core::JobState::DeadLettered => {
                    return Err(format!(
                        "Chord {} cannot complete: dependency {} was dead-lettered",
                        payload.chord_id, dep_id
                    )
                    .into());
                }
                worklane_core::JobState::Live => {
                    pending.push(*dep_id);
                }
                worklane_core::JobState::CompletedOrUnknown => {
                    if let Some(bytes) = self.result_store.get(dep_id).await? {
                        let index = collected.len();
                        collected.push((*dep_id, bytes));
                        captured.insert(*dep_id, index);
                    } else {
                        return Err(format!(
                            "Chord {} cannot aggregate: dependency {} completed but its \
                             result was evicted before capture (increase the result TTL)",
                            payload.chord_id, dep_id
                        )
                        .into());
                    }
                }
            }
        }

        if !pending.is_empty() {
            // Some dependency has never been observed complete. Check the bound.
            if payload.generation >= payload.max_generations {
                return Err(format!(
                    "Chord {} exceeded max generations ({})",
                    payload.chord_id, payload.max_generations
                )
                .into());
            }

            // Reschedule self to poll again later. Clone the whole payload, bump
            // the generation, and carry the captured values forward (the full
            // dependency list stays intact; `collected` records which are done).
            let mut next_payload = payload.clone();
            // `saturating_add` so a corrupt/extreme generation value can never panic
            // on overflow; the `max_generations` bound terminates the chord long
            // before this saturates in any real configuration.
            next_payload.generation = payload.generation.saturating_add(1);
            next_payload.collected = collected;
            let next_gen = next_payload.generation;

            // Use a generation-keyed unique key to dedup identical retries of the same generation
            let key = format!("cw:{}:{}", payload.chord_id, next_gen);

            self.client
                .enqueue_inner::<ChordWatcherJob>(
                    ctx.lane.clone(),
                    std::time::Duration::from_secs(payload.poll_delay_secs),
                    Some(key),
                    next_payload,
                )
                .await?;

            // Ack this generation so it doesn't pollute the dead letter queue
            return Ok(());
        }

        // Every dependency's value is captured. Aggregate the outputs in
        // dependency order and deliver them to the callback as ChordResults<C>.
        let results: Vec<Vec<u8>> = payload
            .dependencies
            .iter()
            .map(|id| {
                // With no pending dependency every id is captured, so this is
                // normally infallible — but return an error instead of panicking if
                // an inconsistent (e.g. hand-forged or duplicated-id) payload ever
                // reaches here, so the chord fails cleanly rather than crashing the
                // worker task.
                captured
                    .get(id)
                    .and_then(|index| collected.get(*index))
                    .map(|(_, bytes)| bytes.clone())
                    .ok_or_else(|| -> worklane_core::HandlerError {
                        format!(
                            "chord {} internal inconsistency: dependency {} was not captured",
                            payload.chord_id, id
                        )
                        .into()
                    })
            })
            .collect::<std::result::Result<_, _>>()?;

        // Splice the caller context (opaque bytes) and the captured result bytes
        // into the ChordResults<C> wire form. The watcher does not know C, so the
        // helper builds the JSON object via serde_json::Value and its unit test
        // proves `serde_json::from_slice::<ChordResults<C>>` reads it back.
        let callback_payload = chord_results_payload(&payload.callback_payload, results)?;

        // Enqueue the callback exactly once. Validate the callback lane against the
        // client's registry before enqueue, like every other enqueue path,
        // keeping the "every enqueue path rejects an unregistered lane"
        // invariant uniform.
        let callback_lane: worklane_core::Lane = payload.callback_lane.parse()?;
        self.client.check_lane(&callback_lane)?;
        // Offload the aggregated callback payload (Claim Check) before enqueue. This
        // is the payload most likely to be large — it splices together every
        // dependency's output — so without offload a wide or heavy chord would be
        // rejected by the envelope cap right at the finish line. No-op unless a
        // payload store is configured. (The dependency payloads and the watcher were
        // offloaded when the chord was submitted.)
        let callback_payload = self.client.maybe_offload(callback_payload).await?;
        let callback_job = NewJob::new(
            callback_lane,
            payload.callback_kind,
            callback_payload,
            payload.callback_max_attempts,
        )
        .with_unique_key(format!("chord:{}:callback", payload.chord_id))
        .with_priority(payload.callback_priority);
        let callback_id = callback_job.id;
        let callback_payload = callback_job.payload.clone();

        if let Err(err) = self.client.broker.enqueue_batch(vec![callback_job]).await {
            self.client
                .cleanup_offload(
                    callback_id,
                    &callback_payload,
                    "chord callback enqueue failed",
                )
                .await;
            return Err(err.into());
        }

        // Ack the watcher
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct CallbackContext {
        label: String,
        generation: u32,
    }

    #[test]
    fn chord_results_payload_round_trips_wire_shape() {
        let context = CallbackContext {
            label: "done".to_string(),
            generation: 3,
        };
        let context_payload = worklane_core::to_payload(&context).unwrap();
        let payload =
            chord_results_payload(&context_payload, vec![vec![1, 2], vec![3, 4]]).unwrap();
        let decoded: ChordResults<CallbackContext> = worklane_core::from_payload(&payload).unwrap();

        assert_eq!(decoded.context, context);
        assert_eq!(decoded.results, vec![vec![1, 2], vec![3, 4]]);
    }
}
