//! The imports a service on Pub/Sub writes every time, in one glob.
//!
//! `use ruststream_gcp_pubsub::prelude::*;` brings in the framework's own prelude plus this
//! crate's user-facing surface: the broker, the subscription descriptor a mount site names, the
//! publish policy and its live publisher, and the ordering-key step. A service file needs no
//! other import to write a handler, mount it and publish from it.
//!
//! Policies arrive under their concept name, not this broker's: [`PubSubPublish`](crate) is
//! [`Publish`] here, so a mount site writes `.publisher(Publish)` or `.out(M, Publish)` whichever
//! broker it runs on, and a service changes brokers by changing one import rather than every
//! mount site. Every policy this broker supports has such a name, and a concept name that is
//! missing means the broker has no policy of that kind - the manifest principle, on the policy
//! layer. The prefixed originals stay at the crate root, for a file that speaks to two brokers at
//! once. One caveat on [`Publish`]: it is the publish *policy*, the declaration a mount site
//! attaches, not the framework's `runtime::Publish` builder that a call site gets back from
//! `message(..)` or `raw(..)`. A file that names both imports that one explicitly.
//!
//! It is also this broker's capability manifest: the framework capability traits a service
//! writes for itself - in a bound, or as a method call on a value a handler is handed, which
//! needs the trait in scope - and that this crate implements. For Pub/Sub that set is empty, and
//! the comment below records why each candidate is out. Nothing is lost by that: the partition
//! key, the one capability of this broker a handler reads per delivery, arrives on
//! [`IncomingMessage`], which the framework's prelude already carries. The [capability
//! table](https://powersemmi.github.io/ruststream-gcp-pubsub/pubsub/#capabilities) sets out what
//! this broker does and does not implement.
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
//! // The ordering key a delivery arrived under, read off the framework's own delivery surface -
//! // no broker-specific trait needed at the call site.
//! fn key_of(delivery: &impl IncomingMessage) -> Option<&[u8]> {
//!     delivery.partition_key()
//! }
//! ```

// The framework's prelude documents brokers as explicit imports, because which broker a service
// runs on is the one thing every service states for itself. Importing this prelude is that
// statement: the broker lives in the crate path being imported, so the framework's glob rides
// along and one import serves the file.
pub use ruststream::prelude::*;

// Everything a service on this crate names by hand today: the broker, the descriptor its
// subscribers mount on, the live publisher a service keeps in state, and the ordering-key step
// (an extension trait, so the glob is how a call site reaches `with_ordering_key`). The live
// publisher keeps its prefix: it is a type a service names on rare occasion, not a word it writes
// at a mount site, and only the policy vocabulary below goes uniform.
pub use crate::{PubSubBroker, PubSubOrdering, PubSubPublisher, PubSubSubscription};

// The policy vocabulary, under concept names rather than this broker's own. A mount site writes
// `.publisher(Publish)` on every broker, so the word a service reads there says what the policy
// is for, not which crate it came from, and moving a service between brokers is a change of
// import rather than a rewrite of every mount site. A concept name missing from this list means
// the broker has no policy of that kind - the manifest principle, applied to the policy layer.
// The prefixed originals stay at the crate root for a file that speaks to two brokers at once.
pub use crate::PubSubPublish as Publish;

// The capability manifest is deliberately empty: no framework capability trait is re-exported
// here, and the exclusions below say why each candidate is out. Where a broker does have one to
// offer, it is a framework item, so a service on two brokers globs both preludes and the compiler
// unifies them on the same trait rather than reporting a clash.

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
// - `Partitioned`, the exception worth spelling out: it is implemented, and a handler does read
//   the key per delivery, yet re-exporting it breaks the natural call. The core surfaces
//   `partition_key` as a defaulted method on `IncomingMessage`, which is already in the glob, and
//   this crate's deliveries override it by delegating to `Partitioned`; with both traits in scope
//   `message.partition_key()` on a concrete delivery is ambiguous (E0034). A generic
//   `&impl Partitioned` bound would still compile, which is exactly why the trap is easy to ship:
//   it springs on the caller who names the type.
