//! The imports a service on Pub/Sub writes every time, in one glob.
//!
//! `use ruststream_gcp_pubsub::prelude::*;` brings in the framework's own prelude plus this
//! crate's user-facing surface: the broker, the subscription descriptor a mount site names, the
//! publish policy and its live publisher, and the ordering-key step. A service file needs no
//! other import to write a handler, mount it and publish from it.
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
//! ```

// The framework's prelude documents brokers as explicit imports, because which broker a service
// runs on is the one thing every service states for itself. Importing this prelude is that
// statement: the broker lives in the crate path being imported, so the framework's glob rides
// along and one import serves the file.
pub use ruststream::prelude::*;

// Everything a service on this crate names by hand today: the broker, the descriptor its
// subscribers mount on, the publish policy the runtime pairs at startup, the live publisher a
// service keeps in state, and the ordering-key step (an extension trait, so the glob is how a
// call site reaches `with_ordering_key`).
pub use crate::{PubSubBroker, PubSubOrdering, PubSubPublish, PubSubPublisher, PubSubSubscription};

// Deliberately absent:
//
// - The `testing` module: broker-author tooling behind a feature, imported explicitly by the
//   tests that use it, never by the service under test.
// - `PARTITION_KEY_HEADER`, `DELIVERY_ATTEMPT_HEADER` and the message types they belong to: a
//   service publishes through the builder, which assembles the message itself, and reads its
//   deliveries through the framework's handler surface. Code that works at the message level
//   names those types explicitly, and says by that import which layer it is working at.
// - `PubSubError`: a service names the error where it handles one, not in every file.
