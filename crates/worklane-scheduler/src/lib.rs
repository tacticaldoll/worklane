//! Recurring (cron) schedules for `worklane`.
//!
//! Depend on this crate only when an application needs recurring job enqueue.
//! One-shot delayed enqueue is already available through the core client API.
//!
//! A [`Scheduler`] holds schedule definitions — each a cron expression paired
//! with a job template — and [`run`](Scheduler::run)s a daemon that enqueues the
//! templated job through the broker every time a schedule becomes due, on the
//! injected [`Clock`]. It is the recurring counterpart to the one-shot delayed
//! enqueue (`Client::enqueue_in`); it only enqueues, so it needs no `Broker`
//! trait change and works over every broker.
//!
//! This lives in its own crate so that consumers who do not schedule do not
//! compile the `cron`/`chrono` dependencies it requires — mirroring the
//! `worklane-pubsub` ecosystem crate. It builds on the public `worklane-core`
//! API only and adds nothing to the `Broker` contract.
//!
//! Time is interpreted as a duration since the Unix epoch, so the scheduler
//! requires an epoch-based clock — [`WallClock`] (the default). `SystemClock`
//! (time since process start) yields meaningless civil times and is unsupported
//! for scheduling. Cron expressions use the `cron` crate's format (seconds first:
//! `sec min hour day-of-month month day-of-week [year]`), evaluated in UTC.
//!
//! The scheduler supports High Availability (HA) deployments. Multiple instances
//! can run concurrently and coordinate via the atomic `ScheduledStore::enqueue_scheduled`
//! to ensure each schedule occurrence is handled at most once across the cluster
//! without race conditions or job loss.
//!
//! **HA invariant: every instance must define each schedule identically.** The
//! cluster-wide deduplication keys on `(schedule_id, occurrence)`, where the
//! occurrence instant is computed from the cron expression and the timezone. So
//! all instances sharing a `schedule_id` MUST use the **same cron expression and
//! the same timezone** for it. If they disagree they compute different occurrence
//! instants for the "same" schedule and the dedup no longer collides — the
//! occurrence double-fires (each instance enqueues its own), or, with a divergent
//! `schedule_id`, never coordinates at all. Treat a schedule's `(id, cron, tz)` as
//! one value deployed uniformly across the fleet and change it everywhere at once.
//! The scheduler cannot enforce this — it sees only its own instance — so it is
//! the operator's contract.
#![forbid(unsafe_code)]
#![warn(missing_docs)]

use std::future::Future;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use cron::Schedule as CronSchedule;
use worklane_core::ScheduledStore;
use worklane_core::{
    Broker, Clock, DEFAULT_MAX_ATTEMPTS, Error, Job, Lane, NewJob, Result, WallClock, to_payload,
};

/// A single recurring schedule: a parsed cron expression plus the job template
/// it enqueues each time it is due.
struct ScheduleEntry {
    id: String,
    cron: CronSchedule,
    /// The timezone the cron fields are interpreted in (DST-aware). Defaults to
    /// UTC; set per-scheduler with [`Scheduler::with_timezone`].
    tz: Tz,
    lane: Lane,
    kind: &'static str,
    payload: Vec<u8>,
    max_attempts: u32,
    dedup: bool,
}

/// A hook to mutate jobs right before they are enqueued.
pub type PreDispatchHook = Arc<dyn Fn(&mut NewJob) + Send + Sync>;

/// Enqueues templated jobs on recurring cron schedules.
pub struct Scheduler {
    store: Arc<dyn ScheduledStore>,
    clock: Arc<dyn Clock>,
    default_max_attempts: u32,
    lane: Lane,
    timezone: Tz,
    entries: Vec<ScheduleEntry>,
    resilient: bool,
    pre_dispatch: Option<PreDispatchHook>,
}

impl Scheduler {
    /// Create a scheduler over the given broker, enqueuing to the default lane
    /// with the default `max_attempts`, on a [`WallClock`].
    ///
    /// Fails with [`Error::Broker`] if the broker does not provide a
    /// [`ScheduledStore`] capability (its [`Broker::scheduled_store`] returns
    /// `None`).
    pub fn new(broker: Arc<dyn Broker>) -> Result<Self> {
        let store = broker.scheduled_store().ok_or_else(|| {
            Error::Broker("broker does not support scheduled enqueue".to_string())
        })?;
        Ok(Self::with_scheduled_store(store))
    }

    /// Create a scheduler directly from a [`ScheduledStore`], for when you
    /// already hold the store handle (for example in tests). [`new`](Self::new)
    /// is the usual entry point and obtains the store from a broker.
    pub fn with_scheduled_store(store: Arc<dyn ScheduledStore>) -> Self {
        Scheduler {
            store,
            clock: Arc::new(WallClock::new()),
            default_max_attempts: DEFAULT_MAX_ATTEMPTS,
            lane: Lane::default(),
            timezone: Tz::UTC,
            entries: Vec::new(),
            resilient: false,
            pre_dispatch: None,
        }
    }

    /// Interpret cron expressions in `tz` (an IANA timezone, e.g.
    /// `chrono_tz::America::New_York`) instead of UTC (builder style). Applies to
    /// every schedule registered **after** this call, so set it before
    /// [`schedule`](Self::schedule). DST is handled by `chrono-tz`: a `0 30 9 * * *`
    /// schedule fires at 09:30 *local* time year-round, its UTC instant shifting
    /// with the offset. See [`run`](Self::run) for the spring-forward / fall-back
    /// edge behavior.
    #[must_use = "this value must be used"]
    pub fn with_timezone(mut self, tz: Tz) -> Self {
        self.timezone = tz;
        self
    }

    /// Set a hook to mutate jobs (e.g., to inject OTEL trace contexts or
    /// custom priorities) right before they are enqueued.
    #[must_use = "this value must be used"]
    pub fn with_pre_dispatch<F>(mut self, hook: F) -> Self
    where
        F: Fn(&mut NewJob) + Send + Sync + 'static,
    {
        self.pre_dispatch = Some(Arc::new(hook));
        self
    }

    /// Enable **resilient mode** for [`run`](Self::run) (builder style).
    ///
    /// By default `run` fails fast: a broker error while firing a due schedule
    /// propagates and ends the daemon. In resilient mode the error is logged and
    /// the loop continues past that occurrence, so a transient broker fault does
    /// not permanently stop all scheduling. A `false` claim from
    /// `enqueue_scheduled` (another instance won the occurrence) is the normal HA
    /// outcome and is never treated as an error in either mode.
    #[must_use = "this value must be used"]
    pub fn with_resilient(mut self, resilient: bool) -> Self {
        self.resilient = resilient;
        self
    }

    /// Use a specific [`Clock`] (builder style). Must be epoch-based (the default
    /// [`WallClock`]); a process-relative clock is not meaningful for cron.
    #[must_use = "this value must be used"]
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// Set the default `max_attempts` for enqueued jobs (builder style).
    #[must_use = "this value must be used"]
    pub fn with_max_attempts(mut self, max_attempts: u32) -> Self {
        self.default_max_attempts = max_attempts;
        self
    }

    /// Set the default lane schedules enqueue to (builder style).
    #[must_use = "this value must be used"]
    pub fn with_lane(mut self, lane: Lane) -> Self {
        self.lane = lane;
        self
    }

    /// Register a recurring schedule for job kind `J` on the scheduler's default
    /// lane. `cron_expr` is the `cron` crate's format (seconds first), evaluated in
    /// the scheduler's timezone (UTC unless [`with_timezone`](Self::with_timezone)
    /// was set). Rejects an unparseable expression.
    pub fn schedule<J: Job>(
        &mut self,
        id: impl Into<String>,
        cron_expr: &str,
        payload: J::Payload,
    ) -> Result<&mut Self> {
        let lane = self.lane.clone();
        self.add::<J>(id, cron_expr, lane, false, payload)
    }

    /// As [`schedule`](Self::schedule), but enqueue to an explicit `lane`.
    pub fn schedule_to<J: Job>(
        &mut self,
        id: impl Into<String>,
        cron_expr: &str,
        lane: Lane,
        payload: J::Payload,
    ) -> Result<&mut Self> {
        self.add::<J>(id, cron_expr, lane, false, payload)
    }

    /// As [`schedule`](Self::schedule), but each fire enqueues with a `unique_key`
    /// of `"{id}:{fire_unix_secs}"`, so the broker's unique-key handling makes the
    /// fire idempotent.
    pub fn schedule_unique<J: Job>(
        &mut self,
        id: impl Into<String>,
        cron_expr: &str,
        payload: J::Payload,
    ) -> Result<&mut Self> {
        let lane = self.lane.clone();
        self.add::<J>(id, cron_expr, lane, true, payload)
    }

    fn add<J: Job>(
        &mut self,
        id: impl Into<String>,
        cron_expr: &str,
        lane: Lane,
        dedup: bool,
        payload: J::Payload,
    ) -> Result<&mut Self> {
        let id = id.into();
        // The id is the cluster-wide occurrence key: it keys both the
        // `enqueue_scheduled` watermark and the dedup `unique_key`. Two entries
        // sharing an id would have one silently swallow the other's fires (the
        // first to claim an occurrence wins it for all), so reject a duplicate at
        // registration rather than fail quietly at runtime.
        if self.entries.iter().any(|e| e.id == id) {
            return Err(Error::Registration(format!(
                "a schedule with id {id:?} is already registered"
            )));
        }
        let cron = CronSchedule::from_str(cron_expr).map_err(|e| {
            Error::Registration(format!("invalid cron expression {cron_expr:?}: {e}"))
        })?;
        let payload = to_payload(&payload)?;
        let tz = self.timezone;
        self.entries.push(ScheduleEntry {
            id,
            cron,
            tz,
            lane,
            kind: J::KIND,
            payload,
            max_attempts: self.default_max_attempts,
            dedup,
        });
        Ok(self)
    }

    /// Run as a long-lived daemon: enqueue each schedule's templated job every
    /// time it is due, on the injected clock, until `shutdown` resolves.
    ///
    /// Cursors are seeded to each schedule's next occurrence at/after the current
    /// time, so occurrences that fell while the scheduler was not running are
    /// **not** backfilled. Each due schedule fires once per occurrence; after a
    /// fire its cursor advances past the current time, skipping any missed slots.
    /// A shutdown signal interrupts the wait so `run` returns promptly.
    ///
    /// **Clock monotonicity.** Cursor advancement and re-fire avoidance rely on the
    /// clock never going backwards. The default [`WallClock`] guarantees this — it
    /// is monotonic non-decreasing for its lifetime, clamping a backward wall-clock
    /// step (e.g. an NTP correction) to the previous reading. So a backward NTP
    /// step cannot regress a cursor or re-fire an occurrence; only a *forward* jump
    /// moves time ahead, which merely skips missed occurrences (never backfills).
    /// A custom [`Clock`] used here must uphold the same non-decreasing property.
    ///
    /// **Timezone / DST.** Cron fields are interpreted in the scheduler's timezone
    /// ([`with_timezone`](Self::with_timezone), default UTC). Across a DST
    /// transition the local wall-clock time is preserved (a 09:30-local schedule
    /// stays 09:30 local). On spring-forward, a fire time inside the skipped hour
    /// advances to the next valid instant; on fall-back, an ambiguous time fires
    /// once. UTC schedules have no DST edges.
    pub async fn run(&self, shutdown: impl Future<Output = ()>) -> Result<()> {
        tokio::pin!(shutdown);

        if self.entries.is_empty() {
            shutdown.await;
            return Ok(());
        }

        // Seed each cursor to the next occurrence strictly after "now".
        let now = self.now_utc()?;
        let mut cursors: Vec<DateTime<Utc>> = Vec::with_capacity(self.entries.len());
        for e in &self.entries {
            cursors.push(Self::next_after(e, &now)?);
        }

        loop {
            // `cursors` is built 1:1 from the (guarded non-empty, never-shrinking)
            // entry list, so `min()` is always `Some`. Handle the impossible empty
            // case as a graceful shutdown rather than a panic on the daemon's hot
            // path, so a future refactor can never turn this into a crash.
            let Some(&earliest) = cursors.iter().min() else {
                shutdown.await;
                return Ok(());
            };
            let now = self.now_utc()?;
            let wait = (earliest - now)
                .to_std()
                .unwrap_or(Duration::ZERO)
                .max(Duration::from_millis(1));

            tokio::select! {
                biased;
                _ = &mut shutdown => return Ok(()),
                _ = tokio::time::sleep(wait) => {}
            }

            // Fire every schedule now due, then advance its cursor past "now" so
            // a clock jump skips missed occurrences instead of backfilling them.
            let now = self.now_utc()?;
            for (i, e) in self.entries.iter().enumerate() {
                if cursors[i] <= now {
                    // In resilient mode a transient broker error is logged and the
                    // occurrence is skipped (the cursor still advances) so the
                    // daemon does not die or hot-loop; fail-fast propagates it.
                    if let Err(err) = self.fire(e, cursors[i]).await {
                        if self.resilient {
                            tracing::warn!(
                                schedule = %e.id,
                                occurrence = cursors[i].timestamp(),
                                error = %err,
                                "scheduler fire failed; continuing in resilient mode"
                            );
                        } else {
                            return Err(err);
                        }
                    }
                    // Advance the cursor to the first occurrence strictly after
                    // the time observed *now that the fire has completed* — not the
                    // pre-fire `now` — so an occurrence that became due during a
                    // slow `fire` is skipped, not backfilled on the next loop
                    // iteration (honoring "missed occurrences are not backfilled").
                    // Computing it directly from this instant in one step (rather
                    // than looping from the cursor one occurrence at a time) skips
                    // any missed occurrences without spinning per missed tick — a
                    // large clock jump on a sub-minute schedule would otherwise
                    // burn one `cron::after` call per skipped occurrence. Read on
                    // both the fail-fast and resilient paths.
                    let after = self.now_utc()?;
                    cursors[i] = Self::next_after(e, &after)?;
                }
            }
        }
    }

    fn now_utc(&self) -> Result<DateTime<Utc>> {
        let since_epoch = self.clock.now();
        // Convert checked rather than `as i64`: a silent wrap to a negative or
        // absurd second count, once fed to `enqueue_scheduled` as an occurrence,
        // would advance that schedule's strictly-greater watermark past every
        // legitimate future occurrence and silently stop the schedule. Surface
        // an out-of-range clock as an error instead.
        //
        // This error ends `run` even in resilient mode (the in-loop `now_utc()?`
        // calls propagate it). That is deliberate, not an oversight: it fires only
        // for a clock outside the i64-seconds range (before 1970 or past ~2554) —
        // a fundamentally broken clock, not the transient broker/enqueue fault that
        // resilient mode is meant to ride out, and one a retry cannot fix. With the
        // default `WallClock` it is unreachable in practice.
        let secs = i64::try_from(since_epoch.as_secs())
            .map_err(|_| Error::Broker("clock time is out of range for scheduling".into()))?;
        DateTime::from_timestamp(secs, since_epoch.subsec_nanos())
            .ok_or_else(|| Error::Broker("clock time is out of range for scheduling".into()))
    }

    fn next_after(e: &ScheduleEntry, after: &DateTime<Utc>) -> Result<DateTime<Utc>> {
        // Interpret the cron fields in the entry's timezone, then map the result
        // back to a UTC instant for the cursor/watermark. `cron` iterates a
        // `DateTime<Tz>` through chrono, so DST is handled by the tz: a
        // spring-forward gap advances to the next valid instant and a fall-back
        // ambiguity resolves deterministically — never a panic or a skipped
        // schedule. With the default `Tz::UTC` this is identical to plain UTC
        // iteration.
        let after_tz = after.with_timezone(&e.tz);
        e.cron
            .after_owned(after_tz)
            .next()
            .map(|dt| dt.with_timezone(&Utc))
            .ok_or_else(|| Error::Broker(format!("schedule {:?} has no further occurrence", e.id)))
    }

    async fn fire(&self, e: &ScheduleEntry, occurrence: DateTime<Utc>) -> Result<()> {
        let timestamp = occurrence.timestamp();

        let mut job = NewJob::new(e.lane.clone(), e.kind, e.payload.clone(), e.max_attempts);
        if e.dedup {
            job = job.with_unique_key(format!("{}:{}", e.id, timestamp));
        }

        if let Some(hook) = &self.pre_dispatch {
            hook(&mut job);
        }

        if !self.store.enqueue_scheduled(&e.id, timestamp, job).await? {
            tracing::debug!(schedule = %e.id, occurrence = timestamp, "schedule occurrence already claimed by another instance");
            return Ok(());
        }

        tracing::debug!(schedule = %e.id, lane = %e.lane, kind = %e.kind, "enqueued scheduled job");
        Ok(())
    }
}

#[cfg(test)]
mod tz_tests {
    use super::*;
    use chrono::TimeZone;
    use std::str::FromStr;

    fn entry(cron_expr: &str, tz: Tz) -> ScheduleEntry {
        ScheduleEntry {
            id: "t".to_string(),
            cron: CronSchedule::from_str(cron_expr).unwrap(),
            tz,
            lane: Lane::default(),
            kind: "k",
            payload: Vec::new(),
            max_attempts: 1,
            dedup: false,
        }
    }

    #[test]
    fn utc_is_the_default_and_unchanged() {
        // A noon-UTC daily schedule from midnight UTC fires at 12:00 UTC.
        let e = entry("0 0 12 * * *", Tz::UTC);
        let after = Utc.with_ymd_and_hms(2024, 1, 15, 0, 0, 0).unwrap();
        let next = Scheduler::next_after(&e, &after).unwrap();
        assert_eq!(next, Utc.with_ymd_and_hms(2024, 1, 15, 12, 0, 0).unwrap());
    }

    #[test]
    fn fires_at_local_wall_clock_time_in_winter() {
        // Noon America/New_York in January is EST (UTC-5) → 17:00 UTC.
        let e = entry("0 0 12 * * *", chrono_tz::America::New_York);
        let after = Utc.with_ymd_and_hms(2024, 1, 15, 0, 0, 0).unwrap();
        let next = Scheduler::next_after(&e, &after).unwrap();
        assert_eq!(next, Utc.with_ymd_and_hms(2024, 1, 15, 17, 0, 0).unwrap());
    }

    #[test]
    fn local_wall_clock_time_is_preserved_across_dst() {
        // Same noon-local schedule in July is EDT (UTC-4) → 16:00 UTC. The wall-
        // clock time (noon local) is preserved; only its UTC instant shifts.
        let e = entry("0 0 12 * * *", chrono_tz::America::New_York);
        let after = Utc.with_ymd_and_hms(2024, 7, 15, 0, 0, 0).unwrap();
        let next = Scheduler::next_after(&e, &after).unwrap();
        assert_eq!(next, Utc.with_ymd_and_hms(2024, 7, 15, 16, 0, 0).unwrap());
    }

    #[test]
    fn spring_forward_gap_does_not_stall_or_panic() {
        // 02:30 America/New_York does not exist on 2024-03-10 (clocks jump
        // 02:00→03:00). The schedule must still advance to a valid future instant,
        // not panic or silently stop.
        let e = entry("0 30 2 * * *", chrono_tz::America::New_York);
        // Just after 01:00 EST that morning (06:00 UTC), before the gap.
        let after = Utc.with_ymd_and_hms(2024, 3, 10, 6, 0, 0).unwrap();
        let next = Scheduler::next_after(&e, &after).unwrap();
        assert!(
            next > after,
            "the gap must not stall the schedule: {next} !> {after}"
        );
    }

    #[test]
    fn fall_back_ambiguous_hour_fires_once() {
        // 01:30 America/New_York occurs twice on 2024-11-03 (clocks fall 02:00→
        // 01:00). The schedule must resolve to a single, valid future instant.
        let e = entry("0 30 1 * * *", chrono_tz::America::New_York);
        let after = Utc.with_ymd_and_hms(2024, 11, 3, 4, 0, 0).unwrap();
        let next = Scheduler::next_after(&e, &after).unwrap();
        assert!(
            next > after,
            "the ambiguous hour must resolve to one instant"
        );
        // Advancing strictly past it yields the next day's occurrence, not a
        // second fire of the same wall-clock time.
        let after2 = next;
        let next2 = Scheduler::next_after(&e, &after2).unwrap();
        assert!(next2 > next && (next2 - next) >= chrono::Duration::hours(23));
    }
}
