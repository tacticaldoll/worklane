use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::error::{Error, Result};

/// Serialize a typed payload to bytes (JSON), mapping failures to
/// [`Error::Serialization`].
pub fn to_payload<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(|e| Error::Serialization(e.to_string()))
}

/// Deserialize a typed payload from bytes (JSON), mapping failures to
/// [`Error::Serialization`].
pub fn from_payload<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    serde_json::from_slice(bytes).map_err(|e| Error::Serialization(e.to_string()))
}
