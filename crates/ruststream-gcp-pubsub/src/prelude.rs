//! The imports a service on Pub/Sub writes every time, in one glob.
//!
//! `use ruststream_gcp_pubsub::prelude::*;` carries the framework's own prelude, this crate's
//! broker and subscription descriptor, its publish policy under the name [`Publish`], the live
//! publisher, and the [`PubSubOrdering`] step.
//!
//! A file that speaks to two brokers at once names the prefixed originals from the crate root
//! instead. [`Publish`] is the publish policy a mount site attaches, not the framework's
//! `runtime::Publish` builder returned by `message(..)` and `raw(..)`; a file naming both imports
//! the builder explicitly.
//!
//! # Examples
//!
//! ```
//! use ruststream_gcp_pubsub::prelude::*;
//!
//! #[subscriber(PubSubSubscription::new("orders-workers"))]
//! async fn handle(order: &[u8], ctx: &mut Context<'_>) -> HandlerResult {
//!     let _ = (order.len(), ctx.name());
//!     HandlerResult::Ack
//! }
//!
//! fn key_of(delivery: &impl IncomingMessage) -> Option<&[u8]> {
//!     delivery.partition_key()
//! }
//! ```

pub use crate::PubSubPublish as Publish;
pub use crate::{PubSubBroker, PubSubOrdering, PubSubPublisher, PubSubSubscription};
pub use ruststream::prelude::*;

// `Partitioned` stays out on purpose: the core surfaces `partition_key` as a defaulted method on
// `IncomingMessage`, which this glob already carries, and this crate's deliveries override it, so
// a glob holding both traits makes `message.partition_key()` ambiguous (E0034) on a concrete
// delivery. A generic `&impl Partitioned` bound still compiles, which is how the trap hides.
