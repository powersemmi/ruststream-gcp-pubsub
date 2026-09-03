//! The imports a service on Pub/Sub writes every time, in one glob.
//!
//! `use ruststream_gcp_pubsub::prelude::*;` carries the framework's own prelude, this crate's
//! broker and subscription descriptor, its publish policy, the live publisher, and the
//! [`PubSubOrdering`] step.
//!
//! A file that speaks to two brokers at once glob-imports the framework's prelude and names each
//! broker's types, which is what the prefixed names are for.
//!
//! # Examples
//!
//! ```
//! use ruststream_gcp_pubsub::prelude::*;
//! use serde::Deserialize;
//!
//! #[derive(Debug, Deserialize)]
//! struct Order {
//!     id: u64,
//! }
//!
//! #[subscriber(PubSubSubscription::new("orders-workers"))]
//! async fn handle(order: &Order, ctx: &mut Context<'_>) -> HandlerOutcome {
//!     let _ = (order.id, ctx.name());
//!     HandlerOutcome::ack()
//! }
//!
//! fn key_of(delivery: &impl IncomingMessage) -> Option<&[u8]> {
//!     delivery.partition_key()
//! }
//! ```

pub use crate::{PubSubBroker, PubSubOrdering, PubSubPublish, PubSubPublisher, PubSubSubscription};
pub use ruststream::prelude::*;

// The publish policy keeps its prefixed name rather than a broker-agnostic `Publish` alias: the
// framework's prelude carries `Publish` as its slot capability trait, and an explicit re-export
// wins over the glob below, so an alias of that name shadows the trait silently and no service on
// this broker can name it at all. `tests/prelude_pubsub.rs` pins that.

// `Partitioned` stays out on purpose: the core surfaces `partition_key` as a defaulted method on
// `IncomingMessage`, which this glob already carries, and this crate's deliveries override it, so
// a glob holding both traits makes `message.partition_key()` ambiguous (E0034) on a concrete
// delivery. A generic `&impl Partitioned` bound still compiles, which is how the trap hides.
