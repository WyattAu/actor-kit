//! Error types for the actor runtime.

use std::borrow::Cow;

/// Convenience alias used throughout the runtime.
pub type Result<T> = std::result::Result<T, Error>;

/// The error type for the actor runtime.
///
/// Deliberately small: the runtime only needs to express actor-lifecycle
/// failures, resource exhaustion (mailbox full, quota rejected), serialization
/// failures, and internal bugs.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    /// An actor lifecycle or messaging error (unknown actor, stopped actor,
    /// duplicate registration, supervision failure, ...).
    #[error("actor error: {0}")]
    Actor(String),
    /// A resource limit was hit: mailbox full, spawn rejected by the
    /// [`crate::ResourcePolicy`], message rate exceeded.
    #[error("resource exhausted: {0}")]
    ResourceExhausted(String),
    /// Serialization or deserialization failure.
    #[error("serialization failed: {0}")]
    Serialization(String),
    /// An internal invariant was violated (bug).
    #[error("internal error: {0}")]
    Internal(String),
}

impl Error {
    /// An actor lifecycle or messaging error.
    pub fn actor(message: impl Into<Cow<'static, str>>) -> Self {
        Self::Actor(message.into().into_owned())
    }

    /// A resource exhaustion error (mailbox full, quota rejected).
    pub fn resource_exhausted(message: impl Into<Cow<'static, str>>) -> Self {
        Self::ResourceExhausted(message.into().into_owned())
    }

    /// A serialization/deserialization error.
    pub fn serialization(message: impl Into<Cow<'static, str>>) -> Self {
        Self::Serialization(message.into().into_owned())
    }

    /// An internal error (bug or unimplemented feature).
    pub fn internal(message: impl Into<Cow<'static, str>>) -> Self {
        Self::Internal(message.into().into_owned())
    }
}
