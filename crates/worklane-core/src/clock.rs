use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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

/// A wall-clock time source anchored at the Unix epoch.
///
/// Unlike [`SystemClock`], whose epoch is the moment the process constructed it,
/// `WallClock` measures time since `UNIX_EPOCH`. Its values are therefore stable
/// across process restarts, which is what a durable broker needs so persisted
/// visibility and lease times stay meaningful after a restart. The trade-off is
/// that it is **not monotonic**: it follows wall-clock adjustments (e.g. NTP).
pub struct WallClock;

impl WallClock {
    /// Create a wall-clock time source.
    pub fn new() -> Self {
        WallClock
    }
}

impl Default for WallClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for WallClock {
    fn now(&self) -> Duration {
        // A system clock set before 1970 is degenerate; clamp rather than panic.
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
    }
}
