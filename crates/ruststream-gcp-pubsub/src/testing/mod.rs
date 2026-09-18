//! Unit-testing a Pub/Sub service in process, behind the `testing` feature.
//!
//! [`PubSubTestBroker`] is a handler-stub transport that reproduces this crate's routing in
//! memory - no server, no network - and implements
//! [`TestableBroker`](ruststream::testing::TestableBroker) on its connected form, so a service
//! runs under the framework's
//! [`TestApp`](https://docs.rs/ruststream/latest/ruststream/testing/index.html) harness through
//! the dispatch path production uses.
//!
//! A routes file needs no editing to be mounted here. The same
//! [`GooglePubSub`](crate::GooglePubSub) that opens a streaming pull opens an in-process
//! subscription, and the same [`PubSubPublish`](crate::PubSubPublish) policy pairs against this
//! broker, so neither side of the file has a test-only spelling to swap in. Swapping the broker
//! at the mount site is the whole change.
//!
//! # Examples
//!
//! ```
//! # mod demo {
//! use ruststream::testing::TestApp;
//! use ruststream_gcp_pubsub::prelude::*;
//! use ruststream_gcp_pubsub::testing::PubSubTestBroker;
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
//! struct Order {
//!     id: u64,
//! }
//!
//! #[subscriber(GooglePubSub::new("orders-workers"))]
//! async fn confirm(order: &Order, Out(out): Out<impl Publisher>) -> HandlerOutcome {
//!     if out
//!         .message(order)
//!         .to("confirmations")
//!         .publish()
//!         .await
//!         .is_err()
//!     {
//!         return HandlerOutcome::retry();
//!     }
//!     HandlerOutcome::ack()
//! }
//!
//! pub async fn a_confirmation_reaches_the_slot() -> Result<(), Box<dyn std::error::Error>> {
//!     let app = RustStream::new(AppInfo::new("orders", "0.1.0"))
//!         .with_broker(PubSubTestBroker::new(), |b| {
//!             b.include(confirm).out(DefaultSlot, Publish::default()).build();
//!         });
//!     let tb = TestApp::start(app).await?;
//!
//!     // An external producer injects to the subscription; the stand-in holds no topics.
//!     tb.broker::<PubSubTestBroker>()
//!         .message(&Order { id: 42 })
//!         .to("orders-workers")
//!         .publish()
//!         .await?;
//!     tb.settle().await?;
//!
//!     tb.out::<DefaultSlot>()
//!         .assert_called_once()
//!         .decoded_as::<Order>()
//!         .with(&Order { id: 42 });
//!
//!     tb.shutdown().await?;
//!     Ok(())
//! }
//! # }
//! # fn main() {}
//! ```
//!
//! # What the stand-in reproduces
//!
//! It routes by exact address match, and the address is the subscription name - the name the
//! descriptor reports, the one a test injects to and asserts on. A test therefore publishes to
//! the subscription, which no producer does against Pub/Sub: the topic-to-subscription binding is
//! the product's, and it is checked against the emulator instead.
//!
//! * [`batch_wait`](crate::GooglePubSub::batch_wait) is honoured. Batching is on the client
//!   either way, over the framework's own buffer, so a `&[T]` body is unit-testable here.
//! * The registration's dead-letter policy is honoured. The stand-in counts the deliveries of
//!   each message, reports the count the way a Pub/Sub delivery does, and publishes a spent one
//!   to the declared topic, so a test drives the cap the service ships.
//! * Ordering keys are honoured on the way out: a published message reports its key under
//!   [`PARTITION_KEY_HEADER`](crate::PARTITION_KEY_HEADER), and `with_options(..)` on a slot view
//!   reads back what a call asked for. `assert_options_default()` states that a publish named
//!   none and took the mount site's.
//! * A publisher that outlived `shutdown` reports `NotConnected` here as it does against
//!   Pub/Sub, so a test cannot go green on a publish the product would refuse.
//!
//! # What it does not
//!
//! [`create_with_topic`](crate::GooglePubSub::create_with_topic) has nothing to create;
//! [`max_outstanding`](crate::GooglePubSub::max_outstanding) and
//! [`ack_extension`](crate::GooglePubSub::ack_extension) name streaming-pull machinery that is
//! not there. All three are ignored. Nothing leases a message in process, so deadline extension,
//! redelivery timing, ordered delivery and the subscription's own dead-lettering are not
//! modelled - those are checked end to end against the emulator, where the integration suite and
//! the framework's conformance lifecycle run (`just test-brokers`).
//!
//! The conformance harness itself is a broker author's tool, not a user-facing one: a service
//! tests with `TestApp`.

mod broker;
mod router;
mod subscriber;

pub use broker::{ConnectedPubSubTestBroker, PubSubTestBroker, PubSubTestPublisher};
pub use subscriber::{PubSubTestMessage, PubSubTestSubscriber};
