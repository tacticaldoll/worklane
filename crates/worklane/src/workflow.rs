use crate::Client;
use async_trait::async_trait;
use lengkap::{Assembly, Decision, Finding, LocatedFinding};
use worklane_core::{Job, JobContext, JobId, Result};

/// The Workflow extension trait.
/// Provides building blocks for fan-in/fan-out topologies (sequences and fan-ins) built entirely
/// in user-space over the core primitives.
#[async_trait]
pub trait Workflow {
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
impl Workflow for Client {
    fn build_continuation<'a, J: Job>(
        &'a self,
        ctx: &JobContext,
        payload: J::Payload,
    ) -> Result<crate::client::JobBuilder<'a>> {
        let key = format!("sequence:{}:{}", ctx.id, J::KIND);
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
use std::collections::HashSet;
use std::sync::Arc;
use worklane_core::NewJob;

/// The payload delivered to a fan-in callback: the caller's `context` plus each
/// dependency's opaque output bytes, in dependency order.
///
/// A fan-in is fan-out-then-aggregate — the callback runs *over the dependency
/// results*, not merely after they complete. Each entry of `results` is one
/// dependency's raw `Job::Output` bytes (as stored in the `ResultStore`); the
/// callback deserializes each itself (e.g. via `from_payload`). The callback job
/// is declared as `Job<Payload = FanInResults<C>>` and submitted with
/// [`Client::fan_in`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FanInResults<C> {
    /// The caller-supplied context passed to [`Client::fan_in`].
    pub context: C,
    /// Each dependency's opaque output bytes, in dependency order.
    pub results: Vec<Vec<u8>>,
}

fn fan_in_results_payload(
    callback_payload: &[u8],
    results: Vec<Vec<u8>>,
) -> worklane_core::Result<Vec<u8>> {
    let context: serde_json::Value = serde_json::from_slice(callback_payload)
        .map_err(|e| worklane_core::Error::Serialization(e.to_string()))?;
    serde_json::to_vec(&serde_json::json!({ "context": context, "results": results }))
        .map_err(|e| worklane_core::Error::Serialization(e.to_string()))
}

/// Tuning for a fan-in watcher's poll loop, passed to
/// [`Client::fan_in_with_policy`](crate::Client::fan_in_with_policy).
///
/// The watcher re-checks its dependencies every `poll_delay_secs` and gives up
/// after `max_generations` polls, so the worst-case wall-clock a fan-in stays
/// pending before failing is `poll_delay_secs * max_generations`. The delay is in
/// whole seconds (matching the watcher's self-reschedule granularity); both
/// fields must be `>= 1`. [`Default`] polls every 10s for up to ~24h.
#[derive(Debug, Clone, Copy)]
pub struct FanInPolicy {
    /// Seconds between two consecutive dependency polls. Must be `>= 1`.
    pub poll_delay_secs: u64,
    /// Maximum number of polls before the fan-in fails. Must be `>= 1`.
    pub max_generations: u32,
}

impl Default for FanInPolicy {
    fn default() -> Self {
        Self {
            poll_delay_secs: 10,
            max_generations: 8640, // ~24h at 10s per poll
        }
    }
}

/// Payload for the internal `FanInWatcherJob`.
///
/// The fields are crate-private and [`FanInWatcherPayload::new`] is the only
/// supported constructor for normal callers, but this is still a serialized job
/// payload at the broker boundary. A caller with direct broker access can submit
/// malformed bytes, so the watcher validates its invariants again when it runs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FanInWatcherPayload {
    /// The stable ID of the fan-in, used for the callback's idempotency key
    pub(crate) fanin_id: String,
    /// The full list of fan-in dependencies, retained across generations (it is
    /// not shrunk; `collected` records which have been captured). Capture is
    /// monotonic: each generation captures any newly-available dependency output
    /// into `collected`, so a later eviction (e.g. a result TTL) of an
    /// already-captured dependency cannot regress the fan-in. A dependency that
    /// completed but whose result was evicted *before* it was ever captured fails
    /// the fan-in, because aggregation requires the value.
    ///
    /// Invariant: every id here MUST originate from [`Client::fan_in`], which
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
    /// the captured results into a `FanInResults<C>` payload at fire time.
    pub(crate) callback_payload: Vec<u8>,
    pub(crate) callback_max_attempts: u32,
    pub(crate) callback_priority: u8,
}

impl FanInWatcherPayload {
    /// Build the initial watcher payload for a fan-in: generation 1, nothing
    /// captured yet. The only supported constructor — it enforces the invariants
    /// the watcher relies on (start at generation 1 with an empty capture set) and
    /// rejects degenerate inputs (no dependencies, or a zero generation bound) up
    /// front rather than letting them surface as a confusing mid-flight failure.
    ///
    /// The watcher captures each dependency's output for aggregation: a
    /// still-running dependency stays live and keeps the fan-in pending; a
    /// dead-lettered one fails the fan-in fast; a dependency whose result is
    /// evicted before capture fails the fan-in (the result TTL must outlive the
    /// fan-in until capture).
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        fanin_id: String,
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
                "a fan-in must have at least one dependency".to_string(),
            ));
        }
        if max_generations == 0 {
            return Err(worklane_core::Error::Broker(
                "a fan-in watcher needs at least one generation (max_generations >= 1)".to_string(),
            ));
        }
        Ok(Self {
            fanin_id,
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

    /// The fan-in's dependency ids, in aggregation order (read-only).
    pub fn dependencies(&self) -> &[JobId] {
        &self.dependencies
    }

    /// The dependency outputs captured so far, as `(id, bytes)` (read-only).
    pub fn collected(&self) -> &[(JobId, Vec<u8>)] {
        &self.collected
    }

    fn validate(&self) -> std::result::Result<(), String> {
        if self.fanin_id.is_empty() {
            return Err("fan-in watcher payload is malformed: fanin_id is empty".to_string());
        }
        if self.dependencies.is_empty() {
            return Err(format!(
                "fan-in watcher payload for {} is malformed: no dependencies",
                self.fanin_id
            ));
        }
        if self.generation == 0 {
            return Err(format!(
                "fan-in watcher payload for {} is malformed: generation must be positive",
                self.fanin_id
            ));
        }
        if self.max_generations == 0 || self.generation > self.max_generations {
            return Err(format!(
                "fan-in watcher payload for {} is malformed: generation {} exceeds max {}",
                self.fanin_id, self.generation, self.max_generations
            ));
        }
        if self.callback_kind.is_empty() {
            return Err(format!(
                "fan-in watcher payload for {} is malformed: callback kind is empty",
                self.fanin_id
            ));
        }
        self.callback_lane
            .parse::<worklane_core::Lane>()
            .map_err(|err| {
                format!(
                    "fan-in watcher payload for {} is malformed: invalid callback lane: {err}",
                    self.fanin_id
                )
            })?;

        let mut dependencies = HashSet::with_capacity(self.dependencies.len());
        for dep_id in &self.dependencies {
            if !dependencies.insert(*dep_id) {
                return Err(format!(
                    "fan-in watcher payload for {} is malformed: duplicate dependency {}",
                    self.fanin_id, dep_id
                ));
            }
        }

        let mut captured = HashSet::with_capacity(self.collected.len());
        for (dep_id, _) in &self.collected {
            if !dependencies.contains(dep_id) {
                return Err(format!(
                    "fan-in watcher payload for {} is malformed: captured unknown dependency {}",
                    self.fanin_id, dep_id
                ));
            }
            if !captured.insert(*dep_id) {
                return Err(format!(
                    "fan-in watcher payload for {} is malformed: duplicate captured dependency {}",
                    self.fanin_id, dep_id
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq)]
enum FanInImpossible {
    DeadLettered(JobId),
    MissingResult(JobId),
}

fn restore_fan_in_assembly(
    dependencies: &[JobId],
    collected: &[(JobId, Vec<u8>)],
) -> worklane_core::Result<Assembly<Vec<u8>>> {
    let mut slots: Vec<Option<Vec<u8>>> = (0..dependencies.len()).map(|_| None).collect();

    for (dependency_id, bytes) in collected {
        let Some(index) = dependencies
            .iter()
            .position(|candidate| candidate == dependency_id)
        else {
            return Err(worklane_core::Error::Broker(format!(
                "fan-in checkpoint contains unknown dependency {dependency_id}"
            )));
        };
        if slots[index].is_some() {
            return Err(worklane_core::Error::Broker(format!(
                "fan-in checkpoint repeats dependency {dependency_id}"
            )));
        }
        slots[index] = Some(bytes.clone());
    }

    Ok(Assembly::from_slots(slots))
}

fn checkpoint_fan_in_assembly(
    dependencies: &[JobId],
    previous: &[(JobId, Vec<u8>)],
    assembly: Assembly<Vec<u8>>,
) -> worklane_core::Result<Vec<(JobId, Vec<u8>)>> {
    let mut slots = assembly.into_slots();
    let mut checkpoint = Vec::with_capacity(dependencies.len());

    // Preserve the prior payload's capture order, then append values first
    // observed in this generation in dependency order. Lengkap owns slot order;
    // Worklane continues to own its serialized checkpoint representation.
    for (dependency_id, _) in previous {
        let Some(index) = dependencies
            .iter()
            .position(|candidate| candidate == dependency_id)
        else {
            return Err(worklane_core::Error::Broker(format!(
                "fan-in checkpoint contains unknown dependency {dependency_id}"
            )));
        };
        let Some(bytes) = slots[index].take() else {
            return Err(worklane_core::Error::Broker(format!(
                "fan-in checkpoint lost previously captured dependency {dependency_id}"
            )));
        };
        checkpoint.push((*dependency_id, bytes));
    }

    checkpoint.extend(
        dependencies
            .iter()
            .copied()
            .zip(slots)
            .filter_map(|(dependency_id, value)| value.map(|bytes| (dependency_id, bytes))),
    );
    Ok(checkpoint)
}

/// A self-rescheduling watcher job that polls the `ResultStore` for a fan-in's dependencies.
/// Once all dependencies are complete, it dispatches the callback job.
pub struct FanInWatcherJob {
    /// Client used to enqueue the callback or reschedule the watcher.
    pub client: Arc<Client>,
    /// Result store used to inspect dependency completion and payload bytes.
    pub result_store: Arc<dyn worklane_core::ResultStore>,
}

#[async_trait]
impl Job for FanInWatcherJob {
    type Payload = FanInWatcherPayload;
    type Output = ();
    const KIND: &'static str = "worklane:fan_in_watcher";

    async fn run(
        &self,
        ctx: JobContext,
        payload: Self::Payload,
    ) -> worklane_core::HandlerResult<Self::Output> {
        if let Err(msg) = payload.validate() {
            return Err(msg.into());
        }
        let assembly = restore_fan_in_assembly(&payload.dependencies, &payload.collected)?;
        let unresolved: Vec<_> = assembly.unresolved_slots().collect();
        let mut findings = Vec::with_capacity(unresolved.len());

        // Worklane owns evidence discovery. Lengkap sees only domain-normalized
        // findings for unresolved slots and performs no broker or result-store I/O.
        for slot in unresolved {
            let dep_id = payload.dependencies[slot.index()];
            // Classify first. ResultStore bytes alone do not prove completion: a
            // worker writes results before acking, and a stale ack can leave bytes
            // behind while the broker still considers the job live.
            match self.client.broker.classify(dep_id).await? {
                worklane_core::JobState::DeadLettered => {
                    findings.push(LocatedFinding::new(
                        slot,
                        Finding::Impossible(FanInImpossible::DeadLettered(dep_id)),
                    ));
                    break;
                }
                worklane_core::JobState::Live => {}
                worklane_core::JobState::CompletedOrUnknown => {
                    if let Some(bytes) = self.result_store.get(&dep_id).await? {
                        findings.push(LocatedFinding::new(slot, Finding::Produced(bytes)));
                    } else {
                        findings.push(LocatedFinding::new(
                            slot,
                            Finding::Impossible(FanInImpossible::MissingResult(dep_id)),
                        ));
                        break;
                    }
                }
            }
        }

        let decision =
            assembly
                .adjudicate(findings)
                .map_err(|err| -> worklane_core::HandlerError {
                    format!(
                        "fan-in {} internal adjudication inconsistency: {err}",
                        payload.fanin_id
                    )
                    .into()
                })?;

        let results = match decision {
            Decision::Pending(assembly) => {
                // Some dependency has never been observed complete. Check the bound.
                if payload.generation >= payload.max_generations {
                    return Err(format!(
                        "Fan-in {} exceeded max generations ({})",
                        payload.fanin_id, payload.max_generations
                    )
                    .into());
                }

                // Reschedule self to poll again later. The assembly is exported
                // into Worklane's existing serialized checkpoint shape.
                let mut next_payload = payload.clone();
                next_payload.generation = payload.generation.saturating_add(1);
                next_payload.collected = checkpoint_fan_in_assembly(
                    &payload.dependencies,
                    &payload.collected,
                    assembly,
                )?;
                let next_gen = next_payload.generation;
                let key = format!("fiw:{}:{}", payload.fanin_id, next_gen);

                self.client
                    .enqueue_inner::<FanInWatcherJob>(
                        ctx.lane.clone(),
                        std::time::Duration::from_secs(payload.poll_delay_secs),
                        Some(key),
                        next_payload,
                    )
                    .await?;

                return Ok(());
            }
            Decision::Ready(results) => results,
            Decision::Impossible { cause, .. } => match cause {
                FanInImpossible::DeadLettered(dep_id) => {
                    return Err(format!(
                        "Fan-in {} cannot complete: dependency {} was dead-lettered",
                        payload.fanin_id, dep_id
                    )
                    .into());
                }
                FanInImpossible::MissingResult(dep_id) => {
                    return Err(format!(
                        "Fan-in {} cannot aggregate: dependency {} completed but its \
                         result was evicted before capture (increase the result TTL)",
                        payload.fanin_id, dep_id
                    )
                    .into());
                }
            },
        };

        // Every dependency's value is captured. Lengkap returns outputs in
        // dependency-slot order for delivery to FanInResults<C>.
        if results.len() != payload.dependencies.len() {
            return Err(format!(
                "fan-in {} internal inconsistency: ready result count {} differs from \
                     dependency count {}",
                payload.fanin_id,
                results.len(),
                payload.dependencies.len()
            )
            .into());
        }

        // Splice the caller context (opaque bytes) and the captured result bytes
        // into the FanInResults<C> wire form. The watcher does not know C, so the
        // helper builds the JSON object via serde_json::Value and its unit test
        // proves `serde_json::from_slice::<FanInResults<C>>` reads it back.
        let callback_payload = fan_in_results_payload(&payload.callback_payload, results)?;

        // Enqueue the callback exactly once. Validate the callback lane against the
        // client's registry before enqueue, like every other enqueue path,
        // keeping the "every enqueue path rejects an unregistered lane"
        // invariant uniform.
        let callback_lane: worklane_core::Lane = payload.callback_lane.parse()?;
        self.client.check_lane(&callback_lane)?;
        // Offload the aggregated callback payload (Claim Check) before enqueue. This
        // is the payload most likely to be large — it splices together every
        // dependency's output — so without offload a wide or heavy fan-in would be
        // rejected by the envelope cap right at the finish line. No-op unless a
        // payload store is configured. (The dependency payloads and the watcher were
        // offloaded when the fan-in was submitted.)
        let callback_payload = self.client.maybe_offload(callback_payload).await?;
        let callback_job = NewJob::new(
            callback_lane,
            payload.callback_kind,
            callback_payload,
            payload.callback_max_attempts,
        )
        .with_unique_key(format!("fanin:{}:callback", payload.fanin_id))
        .with_priority(payload.callback_priority);
        let callback_id = callback_job.id;
        let callback_payload = callback_job.payload.clone();

        let batch_result = self.client.enqueue_batch(vec![callback_job]).await;
        if let Err(err) = batch_result {
            self.client
                .cleanup_offload(
                    callback_id,
                    &callback_payload,
                    "fan-in callback enqueue failed",
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
    fn fan_in_results_payload_round_trips_wire_shape() {
        let context = CallbackContext {
            label: "done".to_string(),
            generation: 3,
        };
        let context_payload = worklane_core::to_payload(&context).unwrap();
        let payload =
            fan_in_results_payload(&context_payload, vec![vec![1, 2], vec![3, 4]]).unwrap();
        let decoded: FanInResults<CallbackContext> = worklane_core::from_payload(&payload).unwrap();

        assert_eq!(decoded.context, context);
        assert_eq!(decoded.results, vec![vec![1, 2], vec![3, 4]]);
    }

    #[test]
    fn lengkap_checkpoint_round_trip_preserves_dependency_slots() {
        let dependencies = vec![JobId::new(), JobId::new(), JobId::new()];
        let assembly =
            restore_fan_in_assembly(&dependencies, &[(dependencies[2], vec![3])]).unwrap();

        let Decision::Pending(assembly) = assembly
            .adjudicate([LocatedFinding::<_, FanInImpossible>::new(
                lengkap::Slot::new(0),
                Finding::Produced(vec![1]),
            )])
            .unwrap()
        else {
            panic!("one unresolved dependency must remain pending");
        };

        let checkpoint =
            checkpoint_fan_in_assembly(&dependencies, &[(dependencies[2], vec![3])], assembly)
                .unwrap();
        assert_eq!(
            checkpoint,
            vec![(dependencies[2], vec![3]), (dependencies[0], vec![1])]
        );

        let restored = restore_fan_in_assembly(&dependencies, &checkpoint).unwrap();
        assert_eq!(restored.value(lengkap::Slot::new(0)), Some(&vec![1]));
        assert_eq!(restored.value(lengkap::Slot::new(1)), None);
        assert_eq!(restored.value(lengkap::Slot::new(2)), Some(&vec![3]));
        assert_eq!(
            restored.unresolved_slots().collect::<Vec<_>>(),
            vec![lengkap::Slot::new(1)]
        );
    }
}
