use std::collections::HashSet;
use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// The maximum length, in bytes, of a lane name.
const MAX_LEN: usize = 256;

/// The name of the default lane.
pub const DEFAULT_LANE: &str = "default";

/// A validated lane identifier.
///
/// A `Lane` is a distinct type rather than a bare `String`, so a value of
/// another kind (for example a job `kind`) cannot be passed where a lane is
/// expected. It is constructed only through the fallible conversions
/// ([`TryFrom`]/[`FromStr`]), which enforce a *portable* invariant — the
/// strictest set every broker can honour: non-empty, at most 256 bytes, no
/// control characters, and no leading or trailing whitespace.
///
/// Backend-specific constraints (such as a broker's key-delimiter charset) are
/// deliberately *not* enforced here; encoding a lane safely for a particular
/// store is that broker's responsibility.
///
/// `Lane` serializes transparently as its bare string name, so persisted job
/// envelopes are unchanged by the introduction of the type. Deserialization
/// does not re-validate: a lane that was persisted deserializes back into an
/// equal `Lane` even if its name would not pass current construction
/// validation.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Lane(String);

impl Lane {
    /// Build a lane from a name known to be valid, skipping validation. Used for
    /// the default lane literal; never exposed to untrusted input.
    fn from_static_unchecked(name: &'static str) -> Self {
        Lane(name.to_string())
    }

    /// Validate `name` against the portable invariant and build a `Lane`.
    fn validated(name: String) -> Result<Self, LaneError> {
        if name.is_empty() {
            return Err(LaneError::Empty);
        }
        if name.len() > MAX_LEN {
            return Err(LaneError::TooLong);
        }
        if name.chars().any(|c| c.is_control()) {
            return Err(LaneError::InvalidChar);
        }
        if name.trim() != name {
            return Err(LaneError::InvalidChar);
        }
        Ok(Lane(name))
    }

    /// The lane name as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for Lane {
    fn default() -> Self {
        Lane::from_static_unchecked(DEFAULT_LANE)
    }
}

impl fmt::Display for Lane {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for Lane {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl PartialEq<str> for Lane {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl PartialEq<&str> for Lane {
    fn eq(&self, other: &&str) -> bool {
        self.0 == *other
    }
}

impl TryFrom<String> for Lane {
    type Error = LaneError;

    fn try_from(name: String) -> Result<Self, Self::Error> {
        Lane::validated(name)
    }
}

impl TryFrom<&str> for Lane {
    type Error = LaneError;

    fn try_from(name: &str) -> Result<Self, Self::Error> {
        Lane::validated(name.to_string())
    }
}

impl FromStr for Lane {
    type Err = LaneError;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        Lane::validated(name.to_string())
    }
}

/// An opt-in set of known lanes, checked at enqueue time to catch a typo'd lane
/// name before a job is stored on a lane no worker reserves.
///
/// A [`Lane`] guarantees only that a name is *well-formed*; `"emial"` is as valid
/// as `"email"`. A `LaneRegistry` adds the missing *value-level* check: the
/// application declares the lanes it considers real, and the enqueue side rejects
/// anything else. Membership is exact `Lane` equality — no fuzzy or suggestion
/// matching. The registry is a client-side guard only; brokers and workers gain
/// no knowledge of the full set of lanes from it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LaneRegistry {
    lanes: HashSet<Lane>,
}

impl LaneRegistry {
    /// Build a registry from the given lanes.
    pub fn new(lanes: impl IntoIterator<Item = Lane>) -> Self {
        LaneRegistry {
            lanes: lanes.into_iter().collect(),
        }
    }

    /// Add the default lane to the set of known lanes (builder style).
    pub fn with_default_lane(mut self) -> Self {
        self.lanes.insert(Lane::default());
        self
    }

    /// Whether `lane` is a member of this registry.
    pub fn contains(&self, lane: &Lane) -> bool {
        self.lanes.contains(lane)
    }

    /// The number of known lanes.
    pub fn len(&self) -> usize {
        self.lanes.len()
    }

    /// Whether the registry holds no lanes.
    pub fn is_empty(&self) -> bool {
        self.lanes.is_empty()
    }
}

impl FromIterator<Lane> for LaneRegistry {
    fn from_iter<I: IntoIterator<Item = Lane>>(iter: I) -> Self {
        LaneRegistry {
            lanes: iter.into_iter().collect(),
        }
    }
}

/// The reason a lane name failed validation.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum LaneError {
    /// The name was empty.
    #[error("lane name must not be empty")]
    Empty,
    /// The name exceeded the maximum length.
    #[error("lane name must not exceed 256 bytes")]
    TooLong,
    /// The name contained a control character or leading/trailing whitespace.
    #[error("lane name must not contain control characters or leading/trailing whitespace")]
    InvalidChar,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn well_formed_name_produces_lane() {
        let lane = Lane::try_from("critical").unwrap();
        assert_eq!(lane.as_str(), "critical");
    }

    #[test]
    fn empty_name_is_rejected() {
        assert_eq!(Lane::try_from(""), Err(LaneError::Empty));
    }

    #[test]
    fn over_length_name_is_rejected() {
        let long = "a".repeat(MAX_LEN + 1);
        assert_eq!(Lane::try_from(long), Err(LaneError::TooLong));
    }

    #[test]
    fn max_length_name_is_accepted() {
        let at_limit = "a".repeat(MAX_LEN);
        assert!(Lane::try_from(at_limit).is_ok());
    }

    #[test]
    fn control_character_is_rejected() {
        assert_eq!(Lane::try_from("a\nb"), Err(LaneError::InvalidChar));
    }

    #[test]
    fn surrounding_whitespace_is_rejected() {
        assert_eq!(Lane::try_from(" critical "), Err(LaneError::InvalidChar));
    }

    #[test]
    fn backend_specific_character_is_accepted() {
        // `:` is unsafe in the redis key scheme, but that is not a portable
        // constraint, so `Lane` accepts it.
        assert!(Lane::try_from("a:b").is_ok());
    }

    #[test]
    fn from_str_parses() {
        let lane: Lane = "critical".parse().unwrap();
        assert_eq!(lane.as_str(), "critical");
    }

    #[test]
    fn default_lane_name() {
        assert_eq!(Lane::default().as_str(), DEFAULT_LANE);
        assert_eq!(Lane::default(), Lane::try_from("default").unwrap());
    }

    #[test]
    fn transparent_string_serialization() {
        let lane = Lane::try_from("critical").unwrap();
        let json = serde_json::to_string(&lane).unwrap();
        assert_eq!(json, "\"critical\"");
    }

    #[test]
    fn registry_membership_is_exact() {
        let reg = LaneRegistry::new([
            Lane::try_from("email").unwrap(),
            Lane::try_from("reports").unwrap(),
        ]);
        assert!(reg.contains(&Lane::try_from("email").unwrap()));
        // A typo is a well-formed but unregistered lane: not a member.
        assert!(!reg.contains(&Lane::try_from("emial").unwrap()));
    }

    #[test]
    fn empty_registry_contains_nothing() {
        let reg = LaneRegistry::default();
        assert!(reg.is_empty());
        assert!(!reg.contains(&Lane::try_from("email").unwrap()));
        assert!(!reg.contains(&Lane::default()));
    }

    #[test]
    fn registry_with_default_lane_includes_it() {
        let reg = LaneRegistry::new([Lane::try_from("email").unwrap()]).with_default_lane();
        assert_eq!(reg.len(), 2);
        assert!(reg.contains(&Lane::default()));
    }

    #[test]
    fn stored_lane_deserializes_without_validation() {
        // A name current validation would reject (surrounding whitespace) must
        // still round-trip from stored form, because deserialization does not
        // re-validate.
        let invalid = " legacy ";
        assert!(Lane::try_from(invalid).is_err());

        let json = format!("\"{invalid}\"");
        let lane: Lane = serde_json::from_str(&json).unwrap();
        assert_eq!(lane.as_str(), invalid);
    }
}
