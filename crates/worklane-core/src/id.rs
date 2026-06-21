use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A unique identifier for an enqueued job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct JobId(Uuid);

impl JobId {
    /// Generate a new random (v4) job id.
    pub fn new() -> Self {
        JobId(Uuid::new_v4())
    }
}

impl Default for JobId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for JobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// The error returned when [`JobId`] fails to parse from a string.
///
/// Wraps the underlying parse failure as text so the `uuid` crate does not leak
/// into worklane's public API (a `uuid` major bump would otherwise be a breaking
/// change here).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobIdParseError(String);

impl fmt::Display for JobIdParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid job id: {}", self.0)
    }
}

impl std::error::Error for JobIdParseError {}

impl FromStr for JobId {
    type Err = JobIdParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(s)
            .map(JobId)
            .map_err(|e| JobIdParseError(e.to_string()))
    }
}
