use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const MIN_FORWARD_JUMP_THRESHOLD: Duration = Duration::from_millis(1);

/// A monotonic time source, abstracted so brokers derive time-based decisions
/// (visibility, lease expiry, retry scheduling) from an injectable clock rather
/// than reading wall-clock time directly.
///
/// `now` returns the elapsed time since an arbitrary epoch; only differences are
/// meaningful.
pub trait Clock: Send + Sync {
    /// The current time as a duration since the clock's epoch.
    fn now(&self) -> Duration;
}

/// A real clock backed by [`Instant`].
pub struct SystemClock {
    start: Instant,
}

impl SystemClock {
    /// Create a system clock anchored at the current instant.
    pub fn new() -> Self {
        SystemClock {
            start: Instant::now(),
        }
    }
}

impl Default for SystemClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for SystemClock {
    fn now(&self) -> Duration {
        self.start.elapsed()
    }
}

/// A wall-clock time source anchored at the Unix epoch, guarded to be
/// **monotonic non-decreasing** for the lifetime of the instance.
///
/// Unlike [`SystemClock`], whose epoch is the moment the process constructed it,
/// `WallClock` measures time since `UNIX_EPOCH`. Its values are therefore stable
/// across process restarts, which is what a durable broker needs so persisted
/// visibility and lease times stay meaningful after a restart.
///
/// Raw wall-clock time follows adjustments (e.g. NTP) and can step **backward**.
/// A backward step is a correctness hazard for a broker: it reorders the
/// `available_at`/`leased_until` keys derived from it (so visibility ordering
/// breaks) and re-hides in-flight work. To remove that hazard as a code
/// guarantee — rather than relying on a slewing time daemon being installed —
/// this clock never returns a value below the highest it has already returned:
/// a backward step is clamped to the previous reading until real time catches
/// up. The floor is per-instance (a broker holds one `WallClock`), and resets on
/// restart, where `SystemTime` is re-anchored anyway.
///
/// This monotonicity is the *clock's* guarantee, not the broker's: a broker only
/// compares `now()` against stored absolute deadlines, so the defense against a
/// backward step belongs here, at the time source — no backend re-implements it.
///
/// Residual: a large **forward** step still moves the clock ahead (we must follow
/// real time forward for durability), which can expire an in-flight lease early
/// and redeliver its job. That is duplicate delivery, which the at-least-once
/// contract already permits; only the more dangerous backward direction is
/// eliminated here.
///
/// Because that forward residual is the mechanism behind clock-jump duplicate
/// execution, `WallClock` can optionally *observe* it: configure a forward-jump
/// threshold with [`WallClock::with_jump_threshold`] and a single time reading
/// that advances past the previous reading by more than the threshold logs a
/// `tracing` warning and increments [`WallClock::forward_jumps`], so an operator
/// can correlate duplicate execution with a clock event. Detection is opt-in and
/// never changes the value `now` returns or any lease/visibility math. The
/// counter is best-effort observability: under concurrent reads, several callers
/// may observe the same underlying clock movement, so it must not be treated as
/// an exact event log.
pub struct WallClock {
    /// Highest nanoseconds-since-epoch value returned so far (the floor).
    floor_nanos: AtomicU64,
    /// Forward-jump warning threshold in nanoseconds; `None` disables detection.
    jump_threshold_nanos: Option<u64>,
    /// Count of observed forward jumps beyond the threshold.
    forward_jumps: AtomicU64,
}

impl WallClock {
    /// Create a wall-clock time source.
    pub fn new() -> Self {
        WallClock {
            floor_nanos: AtomicU64::new(0),
            jump_threshold_nanos: None,
            forward_jumps: AtomicU64::new(0),
        }
    }

    /// Enable opt-in forward-jump detection (builder style): a reading that
    /// advances past the previous reading by more than `threshold` logs a warning
    /// and increments [`forward_jumps`](WallClock::forward_jumps). Choose a
    /// threshold comfortably larger than how often the clock is read (each read
    /// is the sampling window), so ordinary elapsed time is not mistaken for a
    /// jump. Values below 1ms are raised to 1ms to prevent warning floods from a
    /// zero or near-zero threshold. Detection does not affect the time returned
    /// by `now`.
    pub fn with_jump_threshold(mut self, threshold: Duration) -> Self {
        let threshold = threshold.max(MIN_FORWARD_JUMP_THRESHOLD);
        self.jump_threshold_nanos = Some(u64::try_from(threshold.as_nanos()).unwrap_or(u64::MAX));
        self
    }

    /// The number of forward jumps observed beyond the configured threshold.
    /// Always `0` when no threshold is configured. This is an advisory,
    /// best-effort counter under concurrent reads, not an exact clock-event log.
    pub fn forward_jumps(&self) -> u64 {
        self.forward_jumps.load(Ordering::Relaxed)
    }

    /// The raw, unguarded time since the Unix epoch. A system clock set before
    /// 1970 is degenerate; clamp to zero rather than panic.
    fn raw_nanos() -> u64 {
        let since_epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO);
        // u64 nanoseconds since the epoch overflow only past the year ~2554;
        // saturate rather than wrap at that horizon.
        u64::try_from(since_epoch.as_nanos()).unwrap_or(u64::MAX)
    }
}

impl Default for WallClock {
    fn default() -> Self {
        Self::new()
    }
}

impl WallClock {
    /// Warn and count a forward jump from `prev` to `raw` when a threshold is
    /// configured and the advance exceeds it. A `prev` of `0` is the
    /// uninitialised floor (the very first reading), which is not a jump and is
    /// skipped. Does not affect the returned time.
    fn observe_forward_jump(&self, prev: u64, raw: u64) {
        let Some(threshold) = self.jump_threshold_nanos else {
            return;
        };
        if prev == 0 || raw <= prev {
            return;
        }
        let delta = raw - prev;
        if delta > threshold {
            self.forward_jumps.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(
                forward_jump = ?Duration::from_nanos(delta),
                threshold = ?Duration::from_nanos(threshold),
                "wall clock jumped forward beyond threshold; in-flight leases may \
                 expire early, widening the at-least-once duplicate-execution window"
            );
        }
    }
}

impl Clock for WallClock {
    fn now(&self) -> Duration {
        // Raise the floor to the current reading and return whichever is larger,
        // so a backward wall-clock step is clamped to the previous value.
        let raw = Self::raw_nanos();
        let prev = self.floor_nanos.fetch_max(raw, Ordering::Relaxed);
        self.observe_forward_jump(prev, raw);
        Duration::from_nanos(raw.max(prev))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wall_clock_is_non_decreasing() {
        let clock = WallClock::new();
        let a = clock.now();
        let b = clock.now();
        assert!(b >= a, "wall clock must not go backward between reads");
    }

    #[test]
    fn forward_jump_beyond_threshold_warns_and_counts() {
        let clock = WallClock::new().with_jump_threshold(Duration::from_secs(10));
        // prev is a real (non-zero) prior reading; raw advances 60s past it.
        let prev = Duration::from_secs(1_000).as_nanos() as u64;
        let raw = prev + Duration::from_secs(60).as_nanos() as u64;
        clock.observe_forward_jump(prev, raw);
        assert_eq!(clock.forward_jumps(), 1);
    }

    #[test]
    fn movement_within_threshold_is_silent() {
        let clock = WallClock::new().with_jump_threshold(Duration::from_secs(10));
        let prev = Duration::from_secs(1_000).as_nanos() as u64;
        let raw = prev + Duration::from_secs(1).as_nanos() as u64; // under threshold
        clock.observe_forward_jump(prev, raw);
        assert_eq!(clock.forward_jumps(), 0);
    }

    #[test]
    fn zero_jump_threshold_is_clamped() {
        let clock = WallClock::new().with_jump_threshold(Duration::ZERO);
        let prev = Duration::from_secs(1_000).as_nanos() as u64;
        let raw = prev + 1;
        clock.observe_forward_jump(prev, raw);
        assert_eq!(clock.forward_jumps(), 0);
    }

    #[test]
    fn first_reading_is_not_a_jump() {
        // The uninitialised floor (0) must not look like a huge forward jump.
        let clock = WallClock::new().with_jump_threshold(Duration::from_secs(10));
        let raw = Duration::from_secs(1_700_000_000).as_nanos() as u64; // ~now
        clock.observe_forward_jump(0, raw);
        assert_eq!(clock.forward_jumps(), 0);
    }

    #[test]
    fn no_threshold_never_counts() {
        let clock = WallClock::new(); // detection disabled
        let prev = Duration::from_secs(1_000).as_nanos() as u64;
        let raw = prev + Duration::from_secs(10_000).as_nanos() as u64;
        clock.observe_forward_jump(prev, raw);
        assert_eq!(clock.forward_jumps(), 0);
    }

    #[test]
    fn wall_clock_clamps_a_backward_step_to_the_floor() {
        // Simulate a previously-observed far-future reading (as if the wall clock
        // had been ahead, then stepped back). `now` must not drop below it.
        let clock = WallClock::new();
        let far_future = Duration::from_secs(4_000_000_000); // ~year 2096
        clock
            .floor_nanos
            .store(far_future.as_nanos() as u64, Ordering::Relaxed);
        let observed = clock.now();
        assert!(
            observed >= far_future,
            "a backward step must be clamped to the previous (floor) value: \
             observed {observed:?} < floor {far_future:?}"
        );
    }
}
