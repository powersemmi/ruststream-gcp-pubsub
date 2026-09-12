//! [`GooglePubSub`]: the subscription descriptor.
//!
//! Pub/Sub separates the topic from the subscription, and the descriptor keeps both explicit:
//! by default it names an existing subscription; `create_with_topic` opts into creating the
//! subscription (and its topic) on subscribe, which is what local development against the
//! emulator wants.

use std::borrow::Cow;
// Only the stand-in's source answers without asking the transport anything.
#[cfg(feature = "testing")]
use std::future::{Future, ready};
use std::time::Duration;

use ruststream::{FromName, RedeliveryAddress, SubscriptionSource};

use crate::broker::ConnectedPubSubBroker;
use crate::error::PubSubError;
use crate::subscriber::PubSubSubscriber;

/// How long a partial batch waits for more deliveries before it goes out. One streaming-pull
/// burst crosses the network in tens of milliseconds, so a deadline much shorter than this
/// would cut most batches down to the first delivery that arrives.
const DEFAULT_BATCH_WAIT: Duration = Duration::from_millis(50);

/// A subscription descriptor for one Pub/Sub subscription.
///
/// Implements [`SubscriptionSource`] for the real broker and for the in-process stand-in behind
/// the `testing` feature, so it sits inline in the `#[subscriber(..)]` decorator and the
/// declaration a service ships is the one its tests mount:
///
/// ```
/// use std::time::Duration;
/// use ruststream_gcp_pubsub::GooglePubSub;
///
/// let source = GooglePubSub::new("orders-workers")
///     .max_outstanding(1_000)
///     .ack_extension(Duration::from_secs(60));
/// # let _ = source;
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub struct GooglePubSub {
    name: String,
    create_with_topic: Option<String>,
    max_outstanding: Option<i64>,
    ack_extension: Option<Duration>,
    batch_wait: Duration,
}

impl GooglePubSub {
    /// Names an existing subscription (short name or full
    /// `projects/{p}/subscriptions/{s}` resource name).
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            create_with_topic: None,
            max_outstanding: None,
            ack_extension: None,
            batch_wait: DEFAULT_BATCH_WAIT,
        }
    }

    /// Creates the subscription bound to `topic` on subscribe when it does not exist yet (the
    /// topic is created too). Meant for local development and tests against the emulator;
    /// production subscriptions are usually managed as infrastructure.
    pub fn create_with_topic(mut self, topic: impl Into<String>) -> Self {
        self.create_with_topic = Some(topic.into());
        self
    }

    /// Flow control: how many received messages may be outstanding (unacked) at once. Defaults
    /// to the client's 1000.
    pub fn max_outstanding(mut self, messages: i64) -> Self {
        self.max_outstanding = Some(messages);
        self
    }

    /// How far each background ack-deadline extension reaches while a handler runs. The client
    /// clamps it to the protocol's 10s..=600s range; defaults to 60s.
    pub fn ack_extension(mut self, extension: Duration) -> Self {
        self.ack_extension = Some(extension);
        self
    }

    /// How long a partial batch waits for more deliveries after its first one, for a handler
    /// that takes a slice. Defaults to 50ms.
    ///
    /// How *large* a batch may be is not named here: that is the registration's
    /// `batch(n)`, which reaches the subscription on its own. This is the other half - how
    /// long the subscription is willing to wait for a batch that size, before handing over
    /// what it has.
    ///
    /// ```
    /// use std::time::Duration;
    /// use ruststream_gcp_pubsub::GooglePubSub;
    ///
    /// let source = GooglePubSub::new("orders-workers")
    ///     .batch_wait(Duration::from_millis(200));
    /// # let _ = source;
    /// ```
    pub fn batch_wait(mut self, wait: Duration) -> Self {
        self.batch_wait = wait;
        self
    }

    /// The subscription name this descriptor resolves.
    #[must_use]
    pub fn subscription(&self) -> &str {
        &self.name
    }

    /// The same name, taken out of a descriptor that has served its purpose.
    #[cfg(feature = "testing")]
    pub(crate) fn into_subscription(self) -> String {
        self.name
    }

    pub(crate) fn create_topic_ref(&self) -> Option<&str> {
        self.create_with_topic.as_deref()
    }

    pub(crate) fn max_outstanding_value(&self) -> Option<i64> {
        self.max_outstanding
    }

    pub(crate) fn ack_extension_value(&self) -> Option<Duration> {
        self.ack_extension
    }

    pub(crate) fn batch_wait_value(&self) -> Duration {
        self.batch_wait
    }

    /// Rejects descriptors that cannot form a subscription, before any I/O.
    pub(crate) fn validate(&self) -> Result<(), PubSubError> {
        if self.name.is_empty() {
            return Err(PubSubError::InvalidDescriptor(
                "subscription name must be non-empty".into(),
            ));
        }
        if self.create_with_topic.as_deref() == Some("") {
            return Err(PubSubError::InvalidDescriptor(
                "topic name must be non-empty".into(),
            ));
        }
        Ok(())
    }
}

/// A subscription is identified by its name and nothing else, every other setting having a
/// default, so the mount site may supply the name instead of the declaration:
/// `#[subscriber(GooglePubSub)]` on the handler and `.name("orders-workers")` where it is mounted.
///
/// # Examples
///
/// ```
/// use ruststream::FromName;
/// use ruststream_gcp_pubsub::GooglePubSub;
///
/// assert_eq!(
///     GooglePubSub::from_name("orders-workers"),
///     GooglePubSub::new("orders-workers"),
/// );
/// ```
impl FromName for GooglePubSub {
    fn from_name(name: impl Into<Cow<'static, str>>) -> Self {
        Self::new(name.into().into_owned())
    }
}

impl SubscriptionSource<ConnectedPubSubBroker> for GooglePubSub {
    type Subscriber = PubSubSubscriber;

    fn name(&self) -> &str {
        self.subscription()
    }

    async fn subscribe(
        self,
        connected: &ConnectedPubSubBroker,
    ) -> Result<PubSubSubscriber, PubSubError> {
        connected.subscribe_descriptor(self).await
    }

    /// The topic this subscription is bound to, which is where a publisher reaches it again.
    ///
    /// A subscription name is not an address on Pub/Sub: a publish goes to a topic, and the
    /// subscription behind it is what receives. The descriptor therefore answers with the topic -
    /// the one `create_with_topic` names, or the one the API reports for an existing subscription.
    /// The runtime asks once, at startup, and publishes the deferred copy of a `retry_after`
    /// delivery there.
    ///
    /// `None` when the subscription's topic has been deleted, because nothing reaches it then.
    ///
    /// # Errors
    ///
    /// Returns [`PubSubError::Admin`] when the subscription has to be looked up and the call
    /// fails.
    async fn redelivery_address(
        &self,
        connected: &ConnectedPubSubBroker,
    ) -> Result<Option<RedeliveryAddress>, PubSubError> {
        if let Some(topic) = self.create_topic_ref() {
            return Ok(Some(RedeliveryAddress::new(topic.to_owned())));
        }
        Ok(connected
            .topic_of(self.subscription())
            .await?
            .map(RedeliveryAddress::new))
    }
}

/// The same descriptor mounts on the in-process stand-in, so a service is unit-tested as it is
/// declared rather than through a bare subscription name.
///
/// The stand-in routes by one address, and that address is the subscription name - the name this
/// source reports, the one the harness injects to and asserts on. What the descriptor says about
/// the service carries over; what it says about the product cannot, because the product is not
/// there:
///
/// * [`batch_wait`](GooglePubSub::batch_wait) is honoured. Batching is on the client either
///   way, over the framework's own buffer, so the deadline means the same thing here.
/// * [`create_with_topic`](GooglePubSub::create_with_topic) is ignored. The stand-in holds
///   no topics and no subscriptions, only addresses, so it has nothing to create and no
///   topic-to-subscription binding to route through. A test therefore publishes to the
///   subscription name, which no producer does against Pub/Sub; that a message published to the
///   *topic* reaches this subscription is the binding's contract, and it is verified against the
///   emulator instead.
/// * [`max_outstanding`](GooglePubSub::max_outstanding) is ignored. It is the streaming
///   pull's flow control, and there is no pull here: the router hands a delivery straight to the
///   subscription's queue.
/// * [`ack_extension`](GooglePubSub::ack_extension) is ignored. Nothing leases a message in
///   process, so nothing expires and nothing needs extending; a handler that outruns its deadline
///   is a live-broker scenario.
///
/// # Examples
///
/// ```
/// use ruststream::runtime::{AppInfo, RustStream};
/// use ruststream_gcp_pubsub::prelude::*;
/// use ruststream_gcp_pubsub::testing::PubSubTestBroker;
/// use serde::Deserialize;
///
/// #[derive(Debug, Deserialize)]
/// struct Order {
///     id: u64,
/// }
///
/// #[subscriber(GooglePubSub::new("orders-workers").max_outstanding(1_000))]
/// async fn handle(order: &Order) -> HandlerOutcome {
///     let _ = order.id;
///     HandlerOutcome::ack()
/// }
///
/// // The production declaration, mounted on the stand-in a test starts.
/// let app = RustStream::new(AppInfo::new("orders", "0.1.0"))
///     .with_broker(PubSubTestBroker::new(), |b| {
///         b.include(handle);
///     });
/// # let _ = app;
/// ```
#[cfg(feature = "testing")]
impl SubscriptionSource<crate::testing::ConnectedPubSubTestBroker> for GooglePubSub {
    type Subscriber = crate::testing::PubSubTestSubscriber;

    fn name(&self) -> &str {
        self.subscription()
    }

    async fn subscribe(
        self,
        connected: &crate::testing::ConnectedPubSubTestBroker,
    ) -> Result<Self::Subscriber, PubSubError> {
        connected.subscribe_descriptor(self).await
    }

    /// The subscription's own name, because that is the one address the stand-in routes by. A
    /// deferred retry lands where the next delivery comes from, which is the promise the answer
    /// carries; the topic hop it takes against Pub/Sub is the product's, and is checked there.
    ///
    /// Nothing is asked of the transport, so the answer is ready before the future is polled.
    fn redelivery_address(
        &self,
        connected: &crate::testing::ConnectedPubSubTestBroker,
    ) -> impl Future<Output = Result<Option<RedeliveryAddress>, PubSubError>> {
        let _ = connected;
        ready(Ok(Some(RedeliveryAddress::new(self.name.clone()))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_subscription_name_is_rejected_before_io() {
        assert!(matches!(
            GooglePubSub::new("").validate(),
            Err(PubSubError::InvalidDescriptor(_))
        ));
    }

    #[test]
    fn empty_topic_name_is_rejected_before_io() {
        assert!(matches!(
            GooglePubSub::new("s").create_with_topic("").validate(),
            Err(PubSubError::InvalidDescriptor(_))
        ));
    }
}
