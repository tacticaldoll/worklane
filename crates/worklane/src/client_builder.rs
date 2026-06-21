use std::time::Duration;

use worklane_core::{JobId, Lane, NewJob, Result};

use crate::client::JobBuilder;

impl<'a> JobBuilder<'a> {
    /// Extract the underlying `NewJob` from this builder. Crate-internal: the
    /// chord path needs the raw core job; app code goes through the enqueue
    /// methods so lane-registry and offload invariants are not bypassed.
    pub(crate) fn into_inner(self) -> NewJob {
        self.job
    }

    /// Override the default lane for this job.
    #[must_use = "this value must be used"]
    pub fn with_lane(mut self, lane: Lane) -> Self {
        self.job.lane = lane;
        self
    }

    /// Set a delay before the job becomes visible to workers.
    #[must_use = "this value must be used"]
    pub fn with_delay(mut self, delay: Duration) -> Self {
        self.job = self.job.with_delay(delay);
        self
    }

    /// Set a unique key for deduplication.
    #[must_use = "this value must be used"]
    pub fn with_unique_key(mut self, key: impl Into<String>) -> Self {
        self.job = self.job.with_unique_key(key);
        self
    }

    /// Set the priority for this job. Higher values mean higher priority.
    #[must_use = "this value must be used"]
    pub fn with_priority(mut self, priority: u8) -> Self {
        self.job = self.job.with_priority(priority);
        self
    }

    /// Enqueue the job to the configured broker.
    pub async fn enqueue(mut self) -> Result<JobId> {
        self.client.check_lane(&self.job.lane)?;
        let payload = std::mem::take(&mut self.job.payload);
        self.job.payload = self.client.maybe_offload(payload).await?;
        let submitted = self.job.id;
        let payload = self.job.payload.clone();
        let returned = match self.client.broker.enqueue(self.job).await {
            Ok(returned) => returned,
            Err(err) => {
                self.client
                    .cleanup_offload(submitted, &payload, "enqueue failed")
                    .await;
                return Err(err);
            }
        };
        self.client
            .cleanup_deduped_offload(submitted, returned, &payload)
            .await;
        Ok(returned)
    }

    /// Fan out this job configuration across multiple lanes as an atomic batch.
    pub async fn enqueue_to_lanes(
        self,
        lanes: impl IntoIterator<Item = Lane>,
    ) -> Result<Vec<JobId>> {
        let mut jobs: Vec<NewJob> = lanes
            .into_iter()
            .map(|lane| {
                let mut job = self.job.clone();
                job.lane = lane;
                job.id = JobId::new();
                job
            })
            .collect();

        for job in &jobs {
            self.client.check_lane(&job.lane)?;
        }
        if self.job.unique_key.is_some() && jobs.len() > 1 {
            return Err(worklane_core::Error::Broker(
                "a unique_key cannot be combined with multi-lane fan-out: the shared key \
                 would deduplicate the per-lane jobs down to one; enqueue per lane instead"
                    .to_string(),
            ));
        }

        for job in &mut jobs {
            let payload = std::mem::take(&mut job.payload);
            job.payload = self.client.maybe_offload(payload).await?;
        }
        let submitted: Vec<(JobId, Vec<u8>)> = jobs
            .iter()
            .map(|job| (job.id, job.payload.clone()))
            .collect();
        let returned = match self.client.broker.enqueue_batch(jobs).await {
            Ok(returned) => returned,
            Err(err) => {
                self.client
                    .cleanup_offloads(
                        submitted
                            .iter()
                            .map(|(job_id, payload)| (*job_id, payload.as_slice())),
                        "batch enqueue failed",
                    )
                    .await;
                return Err(err);
            }
        };
        for ((submitted_id, payload), returned_id) in submitted.into_iter().zip(returned.iter()) {
            self.client
                .cleanup_deduped_offload(submitted_id, *returned_id, &payload)
                .await;
        }
        Ok(returned)
    }
}
