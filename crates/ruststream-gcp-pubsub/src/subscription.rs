//! [`PubSubSubscription`]: the subscription descriptor.
//!
//! Pub/Sub separates the topic from the subscription, and the descriptor keeps both explicit:
//! by default it names an existing subscription; `create_with_topic` opts into creating the
//! subscription (and its topic) on subscribe, which is what local development against the
//! emulator wants.

use std::time::Duration;

use ruststream::SubscriptionSource;

use crate::broker::ConnectedPubSubBroker;
use crate::error::PubSubError;
use crate::subscriber::PubSubSubscriber;

/// How long a partial batch waits for more deliveries before it goes out. One streaming-pull
/// burst crosses the network in tens of milliseconds, so a deadline much shorter than this
/// would cut most batches down to the first delivery that arrives.
const DEFAULT_BATCH_WAIT: Duration = Duration::from_millis(50);

/// A subscription descriptor for one Pub/Sub subscription.
///
/// Implements [`SubscriptionSource`], so it can sit inline in the `#[subscriber(..)]`
/// decorator:
///
/// ```
/// use std::time::Duration;
/// use ruststream_gcp_pubsub::PubSubSubscription;
///
/// let source = PubSubSubscription::new("orders-workers")
///     .max_outstanding(1_000)
///     .ack_extension(Duration::from_secs(60));
/// # let _ = source;
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub struct PubSubSubscription {
    name: String,
    create_with_topic: Option<String>,
    max_outstanding: Option<i64>,
    ack_extension: Option<Duration>,
    batch_wait: Duration,
}

impl PubSubSubscription {
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
    /// use ruststream_gcp_pubsub::PubSubSubscription;
    ///
    /// let source = PubSubSubscription::new("orders-workers")
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

impl SubscriptionSource<ConnectedPubSubBroker> for PubSubSubscription {
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_subscription_name_is_rejected_before_io() {
        assert!(matches!(
            PubSubSubscription::new("").validate(),
            Err(PubSubError::InvalidDescriptor(_))
        ));
    }

    #[test]
    fn empty_topic_name_is_rejected_before_io() {
        assert!(matches!(
            PubSubSubscription::new("s")
                .create_with_topic("")
                .validate(),
            Err(PubSubError::InvalidDescriptor(_))
        ));
    }
}
