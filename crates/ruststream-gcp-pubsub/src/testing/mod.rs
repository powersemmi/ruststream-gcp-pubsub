//! In-process test support, behind the `testing` feature.
//!
//! [`PubSubTestBroker`] is a handler-stub transport that reproduces the crate's core routing in
//! memory - no server, no network - and implements
//! [`TestableBroker`](ruststream::testing::TestableBroker) on its connected form, so
//! application handlers can be unit-tested with the
//! [`TestApp`](ruststream::testing::TestApp) harness. It routes by exact address match and does
//! not simulate broker-specific semantics (dead-letter policies, credit, redelivery timing);
//! those are verified end to end against a real broker.
//!
//! A service mounts on it with the wiring it ships:
//! [`PubSubSubscription`](crate::PubSubSubscription) opens a subscription here as it opens a
//! streaming pull, and the real [`PubSubPublish`](crate::PubSubPublish) policy pairs against this
//! broker too, so neither side of a routes file needs a test-only variant.

mod broker;
mod router;
mod subscriber;

pub use broker::{ConnectedPubSubTestBroker, PubSubTestBroker, PubSubTestPublisher};
pub use subscriber::{PubSubTestMessage, PubSubTestSubscriber};
