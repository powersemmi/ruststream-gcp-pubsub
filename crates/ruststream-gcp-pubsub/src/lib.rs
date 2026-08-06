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
//!   no envelope format is invented.
//! - The Pub/Sub emulator is a supported target ([`PubSubBroker::emulator`]) for local
//!   development and tests.

#![forbid(unsafe_code)]

mod broker;
mod error;
mod message;
mod publisher;
mod subscriber;
mod subscription;
#[cfg(feature = "testing")]
pub mod testing;

pub use broker::{ConnectedPubSubBroker, PubSubBroker};
pub use error::PubSubError;
pub use message::{DELIVERY_ATTEMPT_HEADER, PARTITION_KEY_HEADER, PubSubMessage};
pub use publisher::{PubSubPublish, PubSubPublisher};
pub use subscriber::PubSubSubscriber;
pub use subscription::PubSubSubscription;
