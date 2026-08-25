//! The imports a service on Pub/Sub writes every time, in one glob.
//!
//! `use ruststream_gcp_pubsub::prelude::*;` brings in the framework's own prelude plus this
//! crate's user-facing surface: the broker, the subscription descriptor a mount site names, the
//! publish policy and its live publisher, and the ordering-key step. A service file needs no
//! other import to write a handler, mount it and publish from it.
//!
//! It is also this broker's capability manifest: the framework capability traits a service
//! writes for itself - in a bound, or as a method call on a value a handler is handed, which
//! needs the trait in scope - and that this crate implements. For Pub/Sub that is exactly one,
//! [`Partitioned`], because transactions, request-reply, batch subscription and seeking are not
//! things this broker does; the [capability
//! table](https://powersemmi.github.io/ruststream-gcp-pubsub/pubsub/#capabilities) sets out the
//! whole picture. The traits the runtime consumes on a service's behalf are not part of the
//! manifest, implemented or not: nobody writes their names.
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
//! // A capability this broker has, reached through the same glob: a delivery reports the
//! // ordering key it arrived under.
//! fn key_of(delivery: &impl Partitioned) -> Option<&[u8]> {
//!     delivery.partition_key()
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

// The capability manifest: the framework capability traits a service writes itself and this
// crate implements. `Partitioned` is the whole list - a handler calls `partition_key()` on the
// delivery it is handed, and a method call needs its trait in scope. It is the framework's own
// item, so a service on two brokers globs both preludes and the compiler unifies them on the
// same trait rather than reporting a clash.
pub use ruststream::Partitioned;

// Deliberately absent:
//
// - The `testing` module: broker-author tooling behind a feature, imported explicitly by the
//   tests that use it, never by the service under test.
// - `PARTITION_KEY_HEADER`, `DELIVERY_ATTEMPT_HEADER` and the message types they belong to: a
//   service publishes through the builder, which assembles the message itself, and reads its
//   deliveries through the framework's handler surface. Code that works at the message level
//   names those types explicitly, and says by that import which layer it is working at.
// - `PubSubError`: a service names the error where it handles one, not in every file.
// - `Subscribe` and `DescribeServer`, both implemented here: contract machinery consumed by the
//   runtime and by AsyncAPI generation, not by the service. The runtime is what calls `subscribe`
//   at include time, and a service that mounts a subscriber never writes either name.
// - `BatchSubscriber` and `Seekable` would be the same case on the subscriber side, and
//   `TransactionalPublisher`, `OwnedTransactions`, `RequestReply`, `Seeker` and `Positioned` are
//   traits a service does write - in a bound, or as a method call on a value it holds - but
//   Pub/Sub implements none of these, and a manifest that promised them would say this broker can
//   do what it cannot.
