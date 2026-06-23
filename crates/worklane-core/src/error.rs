use thiserror::Error;

/// Errors produced by worklane operations.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// A payload could not be serialized or deserialized. A job carrying this
    /// error is unrecoverable — its payload will never decode — so the worker
    /// dead-letters it immediately rather than retrying.
    #[error("serialization error: {0}")]
    Serialization(String),

    /// A handler's output value failed to encode after the handler itself
    /// succeeded. Unlike [`Serialization`](Error::Serialization) (an
    /// undecodable input payload), this can be transient, so the worker routes
    /// it through the normal retry/dead-letter failure path.
    #[error("output encode error: {0}")]
    OutputEncode(String),

    /// A reserved job had a kind with no registered handler.
    #[error("unknown job kind: {0}")]
    UnknownKind(String),

    /// A job handler returned an error.
    #[error("handler error: {0}")]
    Handler(String),

    /// A broker operation failed.
    #[error("broker error: {0}")]
    Broker(String),

    /// A dead-lettered job could not be requeued because another live job already
    /// holds the same `JobId`. The dead-lettered job and the live holder are left
    /// untouched; the operator must resolve the conflict before requeuing.
    #[error("live job id conflict: {0}")]
    LiveJobIdConflict(String),

    /// A reservation receipt is expired, superseded, or unknown.
    #[error("stale reservation: {0}")]
    StaleReservation(String),

    /// A `requeue` could not restore the dead-lettered job's `unique_key` because
    /// the key is currently held by another live job. The dead-lettered job is
    /// left untouched; the operator must resolve the conflict (e.g. purge or let
    /// the holder finish) before requeuing. The key was released when the job was
    /// dead-lettered, so a new job may legitimately hold it in the meantime.
    #[error("unique key held: {0}")]
    UniqueKeyHeld(String),

    /// A worker registration was rejected (e.g. a duplicate kind).
    #[error("registration error: {0}")]
    Registration(String),

    /// An enqueue targeted a lane that is not in the client's configured
    /// [`LaneRegistry`](crate::LaneRegistry). The job was not submitted. This is
    /// most often a typo'd lane name; add the lane to the registry if it is
    /// intended. Only produced when a registry is configured — an unconfigured
    /// client accepts any well-formed lane.
    #[error("unknown lane: {0}")]
    UnknownLane(String),

    /// A result store operation failed, or no store is configured.
    #[error("result store error: {0}")]
    ResultStore(String),

    /// An optional broker capability was requested from a broker that does not
    /// implement it (its capability accessor returned `None`). The operation was
    /// not performed. The string names the missing capability.
    #[error("unsupported broker capability: {0}")]
    UnsupportedCapability(String),
}

/// A `Result` specialized to the worklane [`enum@Error`] type.
pub type Result<T> = std::result::Result<T, Error>;
