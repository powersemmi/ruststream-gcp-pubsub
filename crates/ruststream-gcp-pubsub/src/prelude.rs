//! The imports a routes file on Pub/Sub writes every time, in one glob.
//!
//! `use ruststream_gcp_pubsub::prelude::*;` carries the framework's own prelude, this crate's
//! broker and subscription descriptor, its publish policy under the mount-site name [`Publish`],
//! the live publisher, and the [`PubSubOrdering`] step.
//!
//! The two sides of a service import different things, and that is what keeps the two
//! vocabularies apart. A file of handler bodies imports the framework's prelude alone and bounds
//! a slot with a capability trait ([`Publisher`], [`PubSubOrdering`] for the ordering step), so it
//! names no broker at all. A routes file imports this glob and attaches policies under their
//! uniform mount-site names, so moving a service to another broker changes the one import rather
//! than every mount site. The prefixed originals stay at the crate root for a file that speaks to
//! two brokers at once and has to say which one it means.
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
//!
//! // What a mount site attaches, under the name every broker's prelude gives its publish policy.
//! let policy: Publish = Publish;
//! # let _ = policy;
//! ```

pub use crate::PubSubPublish as Publish;
pub use crate::{PubSubBroker, PubSubOrdering, PubSubPublisher, PubSubSubscription};
pub use ruststream::prelude::*;

// `Publish` is a policy name, and the framework's prelude exports nothing under it, so this
// re-export names one thing and collides with nothing: a mount site reads the same whichever
// broker it is on, and swapping brokers swaps the glob. The capability vocabulary a handler body
// bounds its slot with comes from the framework's prelude, which such a file imports on its own.
// `tests/prelude_pubsub.rs` pins both halves.
//
// Never alias a policy to a framework trait name (`Publisher`, `TransactionalPublisher`,
// `OwnedTransactions`, `RequestReply`): a body that globs both preludes has to keep resolving
// those to the framework's traits.

// `Partitioned` stays out on purpose: the core surfaces `partition_key` as a defaulted method on
// `IncomingMessage`, which this glob already carries, and this crate's deliveries override it, so
// a glob holding both traits makes `message.partition_key()` ambiguous (E0034) on a concrete
// delivery. A generic `&impl Partitioned` bound still compiles, which is how the trap hides.
