use thiserror::Error;

/// Errors produced by worklane operations.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// A payload could not be serialized or deserialized.
    #[error("serialization error: {0}")]
    Serialization(String),

    /// A reserved job had a kind with no registered handler.
    #[error("unknown job kind: {0}")]
    UnknownKind(String),

    /// A job handler returned an error.
    #[error("handler error: {0}")]
    Handler(String),

    /// A broker operation failed.
    #[error("broker error: {0}")]
    Broker(String),

    /// A reservation receipt is expired, superseded, or unknown.
    #[error("stale reservation: {0}")]
    StaleReservation(String),

    /// A worker registration was rejected (e.g. a duplicate kind).
    #[error("registration error: {0}")]
    Registration(String),
}

/// A `Result` specialized to the worklane [`Error`] type.
pub type Result<T> = std::result::Result<T, Error>;
