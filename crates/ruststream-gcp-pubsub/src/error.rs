//! The crate-level error type.

use std::error::Error as StdError;

/// Errors returned by the Google Cloud Pub/Sub broker.
///
/// One enum for the whole crate, variants by source, per the `RustStream` broker conventions.
/// The wrapped sources are boxed `std` errors so the public API does not leak the client's
/// error types.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PubSubError {
    /// Building a client (authentication or connection setup) failed.
    #[error("pubsub client error: {0}")]
    Connect(#[source] Box<dyn StdError + Send + Sync>),

    /// A topic or subscription admin call failed.
    #[error("pubsub admin error for '{name}': {source}")]
    Admin {
        /// The resource the call was about.
        name: String,
        /// The client's failure.
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },

    /// The streaming pull failed permanently (transient failures are retried by the client).
    #[error("pubsub receive error on '{subscription}': {source}")]
    Receive {
        /// The subscription the stream was pulling from.
        subscription: String,
        /// The client's failure.
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },

    /// Publishing to a topic failed.
    #[error("pubsub publish error to '{topic}': {source}")]
    Publish {
        /// The topic the message was published to.
        topic: String,
        /// The client's failure.
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },

    /// The handle is used before `connect` filled the shared connection, or after `shutdown`.
    #[error("pubsub broker is not connected")]
    NotConnected,

    /// A subscription descriptor is invalid.
    #[error("invalid pubsub subscription descriptor: {0}")]
    InvalidDescriptor(String),
}

/// Boxes a client error into the crate's `Box<dyn StdError>` source form.
pub(crate) fn box_err<E>(err: E) -> Box<dyn StdError + Send + Sync>
where
    E: StdError + Send + Sync + 'static,
{
    Box::new(err)
}
