//! Google Cloud Pub/Sub broker implementation for `RustStream`.
//!
//! Handlers, routers, codecs, and middleware come from the framework; this crate supplies the
//! transport over the official
//! [`google-cloud-pubsub`](https://docs.rs/google-cloud-pubsub) client.
//!
//! - A streaming pull subscription is the framework's message stream; ack and nack are native
//!   per message, and the client extends ack deadlines in the background while a handler runs.
//! - Dead-letter topics and delivery-attempt counts are subscription settings, surfaced on
//!   received messages, not crate machinery.
//! - Ordering keys map onto the partition key; message attributes carry headers directly, so
//!   no envelope format is invented. A publish names its key with
//!   [`PubSubOrdering::with_ordering_key`], the crate's step on the framework's publish builder.
//! - The Pub/Sub emulator is a supported target ([`PubSubBroker::emulator`]) for local
//!   development and tests.
//!
//! A service imports [`prelude`]: one glob covering the framework's own prelude and this crate's
//! user-facing surface.

#![forbid(unsafe_code)]

mod broker;
mod error;
mod message;
pub mod prelude;
mod publisher;
mod subscriber;
mod subscription;
#[cfg(feature = "testing")]
pub mod testing;

pub use broker::{ConnectedPubSubBroker, PubSubBroker};
pub use error::PubSubError;
pub use message::{DELIVERY_ATTEMPT_HEADER, PARTITION_KEY_HEADER, PubSubMessage};
pub use publisher::{OrderedPublisher, PubSubOrdering, PubSubPublish, PubSubPublisher};
pub use subscriber::PubSubSubscriber;
pub use subscription::PubSubSubscription;
