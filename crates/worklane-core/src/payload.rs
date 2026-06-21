use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::error::{Error, Result};

fn serde_err(e: serde_json::Error) -> Error {
    const MAX_LEN: usize = 512;
    let msg = crate::redact::redact_and_truncate(&e.to_string(), MAX_LEN);
    Error::Serialization(msg)
}

/// Serialize a typed payload to bytes (JSON), mapping failures to
/// [`Error::Serialization`].
pub fn to_payload<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(serde_err)
}

/// Deserialize a typed payload from bytes (JSON), mapping failures to
/// [`Error::Serialization`].
pub fn from_payload<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    serde_json::from_slice(bytes).map_err(serde_err)
}
