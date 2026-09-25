#![doc = include_str!("README.md")]
#![forbid(unsafe_code)]

mod broker;
mod error;
#[cfg(feature = "testing")]
mod in_process;
mod message;
pub mod prelude;
mod publisher;
mod runtime_slot;
mod subscriber;
mod subscription;

pub use broker::{ConnectedPubSubBroker, PubSubBroker};
pub use error::PubSubError;
pub use message::{DELIVERY_ATTEMPT_HEADER, PARTITION_KEY_HEADER, PubSubMessage};
pub use publisher::{PubSubOrdering, PubSubPublish, PubSubPublishOptions, PubSubPublisher};
pub use subscriber::PubSubSubscriber;
pub use subscription::GooglePubSub;
