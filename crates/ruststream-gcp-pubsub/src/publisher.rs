//! [`PubSubPublisher`], its [`PubSubPublish`] policy, and the [`PubSubOrdering`] publish step.

use std::borrow::Cow;
use std::fmt;

use bytes::Bytes;
use google_cloud_pubsub::client::Publisher as GcpPublisher;
use ruststream::runtime::{OutSlot, SlotPublisher};
use ruststream::{Headers, OutgoingMessage, PairError, PublishPolicy, Publisher};

use crate::broker::{ConnectedPubSubBroker, Core, CoreCell};
use crate::error::{PubSubError, box_err};
use crate::message::{PARTITION_KEY_HEADER, to_gcp_message};

/// Publishes messages to Pub/Sub topics, one client publisher per topic, created lazily and
/// shared through the broker core (so `shutdown` can flush buffered batches).
///
/// The destination name is the topic id (short or full resource name). A `partition-key`
/// header becomes the message's ordering key; per-key FIFO is the client's ordered path.
/// Buildable before `connect` and usable until `shutdown`; afterwards every publish reports
/// [`PubSubError::NotConnected`] instead of silently succeeding.
#[derive(Clone)]
pub struct PubSubPublisher {
    cell: CoreCell,
}

impl fmt::Debug for PubSubPublisher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PubSubPublisher").finish_non_exhaustive()
    }
}

impl PubSubPublisher {
    pub(crate) fn new(cell: CoreCell) -> Self {
        Self { cell }
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

impl Publisher for PubSubPublisher {
    type Error = PubSubError;

    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        let core = self.core()?;
        let publisher = self.publisher_for(core, msg.name()).await;
        let (message, ordering_key) = to_gcp_message(&msg);
        match publisher.publish(message).await {
            Ok(_message_id) => Ok(()),
            Err(err) => {
                // An error on an ordered key pauses the key; resume so the pause cannot wedge
                // every later publish on this key, and let the caller see this failure.
                if !ordering_key.is_empty() {
                    publisher.resume_publish(ordering_key);
                }
                Err(PubSubError::Publish {
                    topic: core.topic_name(msg.name()),
                    source: box_err(err),
                })
            }
        }
    }
}

/// Names the ordering key of a publish, the one Pub/Sub argument that belongs to the message
/// rather than to the connection.
///
/// The framework's publish builder ends in a [`Publisher`], so a broker step is a publisher
/// adapter in front of it: `with_ordering_key` returns an [`OrderedPublisher`] that offers the
/// key as its [base headers](Publisher::base_headers), and the builder's own entry points
/// (`message`, `raw`) and the rest of the chain (codec, headers, destination) follow unchanged.
/// The builder writes the publish's own headers over the base key by key, so a message declaring
/// a header contract publishes with an ordering key too.
///
/// Implemented for the live publisher, for the in-process test publisher, and for the `Out` slot
/// wrapper, so the same call works in a handler, in a startup hook and under the test harness.
///
/// # Examples
///
/// ```
/// use ruststream::runtime::PublishExt;
/// use ruststream_gcp_pubsub::{PubSubOrdering, PubSubPublisher};
///
/// async fn seed(publisher: &PubSubPublisher) -> Result<(), Box<dyn std::error::Error>> {
///     publisher
///         .with_ordering_key("order-42")
///         .raw(b"created")
///         .to("orders")
///         .publish()
///         .await?;
///     Ok(())
/// }
/// ```
pub trait PubSubOrdering: Publisher {
    /// Adapts this publisher so every message it sends carries `key` as its ordering key.
    ///
    /// Pass a `&str` (the borrowed case) or a `String` (a computed key). The adapter borrows
    /// the publisher, so one key can serve a run of publishes.
    #[must_use]
    fn with_ordering_key<'a>(&'a self, key: impl Into<Cow<'a, str>>) -> OrderedPublisher<'a, Self> {
        OrderedPublisher::new(self, key.into())
    }
}

impl PubSubOrdering for PubSubPublisher {}

// The adapter wraps the slot rather than reaching through `inner`, so publishes made with an
// ordering key stay attributed to the slot under the test harness; the wrapper forwards the base
// headers of whatever it holds, so wrapping it either way keeps the key on the message.
impl<P: PubSubOrdering, M: OutSlot> PubSubOrdering for SlotPublisher<P, M> {}

/// A publisher that offers one ordering key to every publish built on it, returned by
/// [`PubSubOrdering::with_ordering_key`].
///
/// The key travels as the `partition-key` header the crate already maps onto the message's
/// ordering key, so an ordered publish stays portable (the same service running against another
/// broker keeps its partition key) and a delivered message reports the key back through
/// [`Partitioned`](ruststream::Partitioned).
///
/// It is offered as the adapter's base headers, not stamped into the message, which is what puts
/// it under the publish's own headers rather than beside them: a `partition-key` named at the
/// call wins over the adapter's key, and any other header the call names travels with it. The
/// base reaches the message through the publish builder; a message handed to
/// [`Publisher::publish`] directly is sent as it was built.
pub struct OrderedPublisher<'a, P: ?Sized> {
    inner: &'a P,
    base: Headers,
}

impl<'a, P: Publisher + ?Sized> OrderedPublisher<'a, P> {
    fn new(inner: &'a P, key: Cow<'a, str>) -> Self {
        // The key is converted once per adapter, not once per publish; an owned key moves into
        // the buffer instead of being copied.
        let key = match key {
            Cow::Borrowed(key) => Bytes::copy_from_slice(key.as_bytes()),
            Cow::Owned(key) => Bytes::from(key),
        };
        // Whatever the wrapped handle already contributes stays contributed: the adapter adds a
        // key on top of it rather than replacing the handle's base, and the key wins over an
        // entry of the same name because the adapter is the more specific level.
        let mut base = inner.base_headers().cloned().unwrap_or_default();
        base.insert(PARTITION_KEY_HEADER, key);
        Self { inner, base }
    }
}

impl<P: ?Sized> fmt::Debug for OrderedPublisher<'_, P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OrderedPublisher")
            .field("base_headers", &self.base)
            .finish_non_exhaustive()
    }
}

impl<P: Publisher + ?Sized> Publisher for OrderedPublisher<'_, P> {
    type Error = P::Error;

    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        self.inner.publish(msg).await
    }

    fn base_headers(&self) -> Option<&Headers> {
        Some(&self.base)
    }
}

/// The publish policy for [`PubSubPublisher`]: pure declaration, constructible anywhere,
/// paired with the connected broker by the runtime after `connect`.
///
/// # Examples
///
/// ```
/// use ruststream_gcp_pubsub::PubSubPublish;
///
/// let policy = PubSubPublish::default();
/// # let _ = policy;
/// ```
#[derive(Debug, Clone, Copy, Default)]
#[must_use]
pub struct PubSubPublish;

impl PublishPolicy<ConnectedPubSubBroker> for PubSubPublish {
    type Live = PubSubPublisher;

    async fn pair(self, connected: &ConnectedPubSubBroker) -> Result<Self::Live, PairError> {
        Ok(connected.publisher())
    }
}

#[cfg(test)]
mod tests {
    use std::future::{Future, ready};
    use std::sync::Mutex;

    use ruststream::runtime::PublishExt;

    use super::*;

    /// A publisher that keeps what it was handed, so the adapter's effect on the message is
    /// observable without a connection. `base` stands for a handle that already contributes
    /// headers of its own.
    #[derive(Debug, Default)]
    struct Recorder {
        sent: Mutex<Vec<(String, Vec<u8>, Headers)>>,
        base: Option<Headers>,
    }

    impl Recorder {
        fn tagged(name: &str, value: &str) -> Self {
            let mut base = Headers::new();
            base.insert(name, value.to_owned());
            Self {
                sent: Mutex::default(),
                base: Some(base),
            }
        }

        fn last(&self) -> (String, Vec<u8>, Headers) {
            self.sent.lock().expect("no panic held the lock")[0].clone()
        }
    }

    impl Publisher for Recorder {
        type Error = PubSubError;

        fn publish(
            &self,
            msg: OutgoingMessage<'_>,
        ) -> impl Future<Output = Result<(), Self::Error>> + Send {
            self.sent.lock().expect("no panic held the lock").push((
                msg.name().to_owned(),
                msg.payload().to_vec(),
                msg.headers().clone(),
            ));
            ready(Ok(()))
        }

        fn base_headers(&self) -> Option<&Headers> {
            self.base.as_ref()
        }
    }

    impl PubSubOrdering for Recorder {}

    #[tokio::test]
    async fn the_key_rides_the_partition_key_header() {
        let recorder = Recorder::default();
        recorder
            .with_ordering_key("order-42")
            .raw(b"created")
            .to("orders")
            .publish()
            .await
            .expect("the recorder accepts the message");

        let (name, payload, headers) = recorder.last();
        assert_eq!(name, "orders");
        assert_eq!(payload, b"created");
        assert_eq!(headers.get_str(PARTITION_KEY_HEADER), Some("order-42"));
    }

    #[tokio::test]
    async fn other_headers_survive_the_step() {
        let recorder = Recorder::default();
        let mut headers = Headers::new();
        headers.insert("x-tenant", "acme");
        recorder
            .with_ordering_key(format!("order-{}", 7))
            .raw(b"created")
            .to("orders")
            .with_headers(headers)
            .publish()
            .await
            .expect("the recorder accepts the message");

        let (.., headers) = recorder.last();
        assert_eq!(headers.get_str("x-tenant"), Some("acme"));
        assert_eq!(headers.get_str(PARTITION_KEY_HEADER), Some("order-7"));
    }

    #[tokio::test]
    async fn a_key_named_at_the_call_wins_over_the_adapter() {
        let recorder = Recorder::default();
        let mut headers = Headers::new();
        headers.insert(PARTITION_KEY_HEADER, "order-9");
        recorder
            .with_ordering_key("order-42")
            .raw(b"created")
            .to("orders")
            .with_headers(headers)
            .publish()
            .await
            .expect("the recorder accepts the message");

        // The adapter serves a run of publishes and the call names one message, so the call has
        // the last word - the framework's precedence for every position of the builder.
        let (.., headers) = recorder.last();
        assert_eq!(headers.get_str(PARTITION_KEY_HEADER), Some("order-9"));
    }

    #[tokio::test]
    async fn the_wrapped_handles_own_base_survives() {
        let recorder = Recorder::tagged("x-tenant", "acme");
        recorder
            .with_ordering_key("order-42")
            .raw(b"created")
            .to("orders")
            .publish()
            .await
            .expect("the recorder accepts the message");

        let (.., headers) = recorder.last();
        assert_eq!(headers.get_str("x-tenant"), Some("acme"));
        assert_eq!(headers.get_str(PARTITION_KEY_HEADER), Some("order-42"));
    }

    #[tokio::test]
    async fn a_message_published_directly_carries_no_base() {
        let recorder = Recorder::default();
        let ordered = recorder.with_ordering_key("order-42");
        // Not the builder path: base headers reach the message when the builder assembles it, so
        // an already-built message travels as its author wrote it.
        ordered
            .publish(OutgoingMessage::new("orders", b"created".as_slice()))
            .await
            .expect("the recorder accepts the message");

        let (.., headers) = recorder.last();
        assert_eq!(headers.get_str(PARTITION_KEY_HEADER), None);
    }
}
