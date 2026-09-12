//! [`PubSubPublisher`], its [`PubSubPublish`] policy, the [`PubSubPublishOptions`] a single
//! message may differ by, and the [`PubSubOrdering`] step that names one.

use std::borrow::Cow;
use std::fmt;
use std::future::{Future, ready};
use std::sync::Arc;

use google_cloud_pubsub::client::Publisher as GcpPublisher;
use ruststream::runtime::{PublishBuilder, PublishSink};
use ruststream::{OutgoingMessage, PairError, PublishPolicy, Publisher};

use crate::broker::{ConnectedPubSubBroker, Core, CoreCell};
use crate::error::{PubSubError, box_err};
use crate::message::{PARTITION_KEY_HEADER, to_gcp_message};

/// The settings one Pub/Sub message may differ from the next by.
///
/// Pub/Sub gives a message exactly one such field, its ordering key, so that is the whole type.
/// The field is optional, because a publish carries only what its call site adjusted: what a call
/// leaves unset keeps what the [`PubSubPublish`] policy fixed at the mount site.
///
/// A handler body that names the [`ordering_key`](PubSubOrdering::ordering_key) step bounds its
/// slot on this type, and a test reads the value back with
/// `tb.out::<Marker>().with_options(..)`.
///
/// # Examples
///
/// ```
/// use ruststream_gcp_pubsub::PubSubPublishOptions;
///
/// let options = PubSubPublishOptions {
///     ordering_key: Some("order-42".to_owned()),
/// };
/// assert_eq!(options.ordering_key.as_deref(), Some("order-42"));
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PubSubPublishOptions {
    /// The message's ordering key. Messages sharing a key are delivered to one subscriber in
    /// publish order; `None` leaves the publish unordered unless the policy fixed a key.
    pub ordering_key: Option<String>,
}

/// Publishes messages to Pub/Sub topics, one client publisher per topic, created lazily and
/// shared through the broker core (so `shutdown` can flush buffered batches).
///
/// The destination name is the topic id (short or full resource name). Message attributes carry
/// headers directly, and the ordering key reaches the client as the message's own field.
/// Buildable before `connect` and usable until `shutdown`; afterwards every publish reports
/// [`PubSubError::NotConnected`] instead of silently succeeding.
#[derive(Clone)]
pub struct PubSubPublisher {
    cell: CoreCell,
    /// The key the mount site fixed for this handle, applied to every publish that names none.
    default_ordering_key: Option<Arc<str>>,
}

impl fmt::Debug for PubSubPublisher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PubSubPublisher")
            .field("default_ordering_key", &self.default_ordering_key)
            .finish_non_exhaustive()
    }
}

impl PubSubPublisher {
    pub(crate) fn new(cell: CoreCell) -> Self {
        Self {
            cell,
            default_ordering_key: None,
        }
    }

    /// The handle a policy pairs into: the same connection cell, carrying the policy's key.
    pub(crate) fn with_default_ordering_key(mut self, key: Option<Arc<str>>) -> Self {
        self.default_ordering_key = key;
        self
    }

    fn core(&self) -> Result<&Core, PubSubError> {
        let core = self.cell.get().ok_or(PubSubError::NotConnected)?;
        core.ensure_open()?;
        Ok(core)
    }

    /// The per-topic client publisher, created on first use and cached on the core.
    async fn publisher_for(&self, core: &Core, topic: &str) -> GcpPublisher {
        let name = core.topic_name(topic);
        let mut publishers = core.publishers.lock().await;
        if let Some(publisher) = publishers.get(&name) {
            return publisher.clone();
        }
        // Sync and infallible off the connected BasePublisher; the network work happened in
        // connect.
        let publisher = core.base_publisher.publisher(name.clone()).build();
        publishers.insert(name, publisher.clone());
        publisher
    }
}

/// The ordering key one publish carries, resolved over the three places it can come from.
///
/// The call site wins in either spelling - the [`ordering_key`](PubSubOrdering::ordering_key)
/// step, or the broker-agnostic `partition-key` header a handler writes - and the key the policy
/// fixed applies when the call names neither.
pub(crate) fn resolve_ordering_key<'a>(
    msg: &'a OutgoingMessage<'_>,
    options: Option<&'a PubSubPublishOptions>,
    policy: Option<&'a str>,
) -> Option<Cow<'a, str>> {
    if let Some(key) = options.and_then(|options| options.ordering_key.as_deref()) {
        return Some(Cow::Borrowed(key));
    }
    if let Some(value) = msg.headers().get(PARTITION_KEY_HEADER) {
        return Some(String::from_utf8_lossy(value));
    }
    policy.map(Cow::Borrowed)
}

impl Publisher for PubSubPublisher {
    type Error = PubSubError;
    type Options = PubSubPublishOptions;

    async fn publish(
        &self,
        msg: OutgoingMessage<'_>,
        options: Option<&Self::Options>,
    ) -> Result<(), Self::Error> {
        let core = self.core()?;
        let publisher = self.publisher_for(core, msg.name()).await;
        let key = resolve_ordering_key(&msg, options, self.default_ordering_key.as_deref());
        let message = to_gcp_message(&msg, key.as_deref());
        match publisher.publish(message).await {
            Ok(_message_id) => Ok(()),
            Err(err) => {
                // An error on an ordered key pauses the key; resume so the pause cannot wedge
                // every later publish on this key, and let the caller see this failure.
                if let Some(key) = key {
                    publisher.resume_publish(key.into_owned());
                }
                Err(PubSubError::Publish {
                    topic: core.topic_name(msg.name()),
                    source: box_err(err),
                })
            }
        }
    }
}

/// Names the ordering key of one publish.
///
/// The step sits on the framework's publish builder, between `message(..)` and `publish()`, so
/// the publish it finishes is still the mount site's: the codec that entry named, its transforms
/// and its slot attribution all hold. The key reaches the client as the message's own ordering
/// key and never as an attribute.
///
/// It resolves only on a builder over a Pub/Sub publisher, which is what the bound on the sink's
/// options type buys: on any other broker's builder the method does not exist.
///
/// A handler body that names it imports this crate's prelude and bounds its slot
/// `Out<impl Publisher<Options = PubSubPublishOptions>, Marker>`. Where a whole slot orders under
/// one key, the mount site says so once instead - [`PubSubPublish::ordering_key`].
///
/// # Examples
///
/// ```
/// use ruststream::runtime::PublishExt;
/// use ruststream::{Outgoing, Serialized};
/// use ruststream_gcp_pubsub::{PubSubOrdering, PubSubPublisher};
///
/// #[derive(Outgoing, Serialized)]
/// struct OrderEvent(Vec<u8>);
///
/// async fn seed(publisher: &PubSubPublisher) -> Result<(), Box<dyn std::error::Error>> {
///     publisher
///         .message(&OrderEvent(b"created".to_vec()))
///         .to("orders")
///         .ordering_key("order-42")
///         .publish()
///         .await?;
///     Ok(())
/// }
/// ```
pub trait PubSubOrdering {
    /// Sends this one message under `key`, whatever the mount site's default is.
    ///
    /// Pass a `&str` or an owned `String`; the key is copied into the message's settings.
    #[must_use]
    fn ordering_key(self, key: impl Into<String>) -> Self;
}

impl<Sink, Body, Enc, Hdrs, Dest> PubSubOrdering for PublishBuilder<Sink, Body, Enc, Hdrs, Dest>
where
    Sink: PublishSink<Options = PubSubPublishOptions>,
{
    fn ordering_key(mut self, key: impl Into<String>) -> Self {
        self.options_mut()
            .get_or_insert_with(PubSubPublishOptions::default)
            .ordering_key = Some(key.into());
        self
    }
}

/// The publish policy for [`PubSubPublisher`]: pure declaration, constructible anywhere, paired
/// with the connected broker by the runtime after `connect`.
///
/// It carries the defaults of the per-message settings, which on Pub/Sub means one: the ordering
/// key every message through this mount site is sent under.
///
/// # Examples
///
/// ```
/// use ruststream_gcp_pubsub::PubSubPublish;
///
/// // Every message this mount site sends is ordered under one key.
/// let policy = PubSubPublish::default().ordering_key("order-42");
/// # let _ = policy;
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[must_use]
pub struct PubSubPublish {
    ordering_key: Option<String>,
}

impl PubSubPublish {
    /// Orders every message this mount site sends under `key`.
    ///
    /// Reach for it where a whole slot belongs to one entity - one order's events, one device's
    /// telemetry. A key that differs per message is the call's to name, with the
    /// [`ordering_key`](PubSubOrdering::ordering_key) step.
    pub fn ordering_key(mut self, key: impl Into<String>) -> Self {
        self.ordering_key = Some(key.into());
        self
    }

    /// The key this policy hands its live publisher, in the form the publisher keeps it.
    pub(crate) fn default_key(&self) -> Option<Arc<str>> {
        self.ordering_key.as_deref().map(Arc::from)
    }
}

impl PublishPolicy<ConnectedPubSubBroker> for PubSubPublish {
    type Live = PubSubPublisher;

    fn pair(
        self,
        connected: &ConnectedPubSubBroker,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        let key = self.default_key();
        ready(Ok(connected.publisher().with_default_ordering_key(key)))
    }
}

#[cfg(test)]
mod tests {
    use ruststream::HeaderMap;

    use super::*;

    fn keyed_header(value: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(PARTITION_KEY_HEADER, value.to_owned());
        headers
    }

    /// The step is the most specific thing a publish can say, so it wins over both the
    /// cross-broker header spelling and the mount site's own key.
    #[test]
    fn the_step_wins_over_the_header_and_the_policy() {
        let msg =
            OutgoingMessage::new("orders", b"{}".as_slice()).with_headers(keyed_header("header"));
        let options = PubSubPublishOptions {
            ordering_key: Some("step".to_owned()),
        };

        let key = resolve_ordering_key(&msg, Some(&options), Some("policy"));
        assert_eq!(key.as_deref(), Some("step"));
    }

    /// A handler that names no broker still orders its messages: the broker-agnostic header is
    /// this transport's other spelling of the same key, and it is a call site too.
    #[test]
    fn the_partition_key_header_orders_a_publish() {
        let msg =
            OutgoingMessage::new("orders", b"{}".as_slice()).with_headers(keyed_header("header"));

        let key = resolve_ordering_key(&msg, None, Some("policy"));
        assert_eq!(key.as_deref(), Some("header"));
    }

    /// What no call site named is what the mount site fixed.
    #[test]
    fn a_publish_that_names_nothing_takes_the_policys_key() {
        let msg = OutgoingMessage::new("orders", b"{}".as_slice());

        let key = resolve_ordering_key(&msg, None, Some("policy"));
        assert_eq!(key.as_deref(), Some("policy"));
    }

    /// Nothing anywhere means an unordered publish, which is Pub/Sub's own default.
    #[test]
    fn a_publish_with_no_key_anywhere_is_unordered() {
        let msg = OutgoingMessage::new("orders", b"{}".as_slice());

        assert!(resolve_ordering_key(&msg, None, None).is_none());
    }
}
