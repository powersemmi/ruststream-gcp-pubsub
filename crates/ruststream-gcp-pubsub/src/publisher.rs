//! [`PubSubPublisher`], its [`PubSubPublish`] policy, and the [`PubSubOrdering`] publish step.

use std::borrow::Cow;
use std::fmt;

use bytes::Bytes;
use google_cloud_pubsub::client::Publisher as GcpPublisher;
use ruststream::runtime::{OutSlot, SlotPublisher};
use ruststream::{OutgoingMessage, PairError, PublishPolicy, Publisher};

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
/// adapter in front of it: `with_ordering_key` returns an [`OrderedPublisher`] that carries the
/// key and is itself a publisher, and the builder's own entry points (`message`, `raw`) and the
/// rest of the chain (codec, headers, destination) follow unchanged.
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

// The slot wrapper delegates rather than reaching through `inner`, so publishes made with an
// ordering key stay attributed to the slot under the test harness.
impl<P: PubSubOrdering, M: OutSlot> PubSubOrdering for SlotPublisher<P, M> {}

/// A publisher that stamps one ordering key onto every message it sends, returned by
/// [`PubSubOrdering::with_ordering_key`].
///
/// The key is written as the `partition-key` header the crate already maps onto the message's
/// ordering key, so an ordered publish stays portable (the same service running against another
/// broker keeps its partition key) and a delivered message reports the key back through
/// [`Partitioned`](ruststream::Partitioned). A key named here replaces one set in the headers:
/// the call site is the more specific level.
pub struct OrderedPublisher<'a, P: ?Sized> {
    inner: &'a P,
    key: Bytes,
}

impl<'a, P: ?Sized> OrderedPublisher<'a, P> {
    fn new(inner: &'a P, key: Cow<'a, str>) -> Self {
        // The key is converted once per adapter, not once per publish; an owned key moves into
        // the buffer instead of being copied.
        let key = match key {
            Cow::Borrowed(key) => Bytes::copy_from_slice(key.as_bytes()),
            Cow::Owned(key) => Bytes::from(key),
        };
        Self { inner, key }
    }
}

impl<P: ?Sized> fmt::Debug for OrderedPublisher<'_, P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OrderedPublisher")
            .field("ordering_key", &String::from_utf8_lossy(&self.key))
            .finish_non_exhaustive()
    }
}

impl<P: Publisher + ?Sized> Publisher for OrderedPublisher<'_, P> {
    type Error = P::Error;

    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        let mut headers = msg.headers().clone();
        headers.insert(PARTITION_KEY_HEADER, self.key.clone());
        self.inner
            .publish(OutgoingMessage::new(msg.name(), msg.payload()).with_headers(headers))
            .await
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

    use ruststream::Headers;
    use ruststream::runtime::PublishExt;

    use super::*;

    /// A publisher that keeps what it was handed, so the adapter's effect on the message is
    /// observable without a connection.
    #[derive(Debug, Default)]
    struct Recorder(Mutex<Vec<(String, Vec<u8>, Headers)>>);

    impl Recorder {
        fn last(&self) -> (String, Vec<u8>, Headers) {
            self.0.lock().expect("no panic held the lock")[0].clone()
        }
    }

    impl Publisher for Recorder {
        type Error = PubSubError;

        fn publish(
            &self,
            msg: OutgoingMessage<'_>,
        ) -> impl Future<Output = Result<(), Self::Error>> + Send {
            self.0.lock().expect("no panic held the lock").push((
                msg.name().to_owned(),
                msg.payload().to_vec(),
                msg.headers().clone(),
            ));
            ready(Ok(()))
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
    async fn a_named_key_replaces_one_set_in_the_headers() {
        let recorder = Recorder::default();
        let mut headers = Headers::new();
        headers.insert(PARTITION_KEY_HEADER, "stale");
        recorder
            .with_ordering_key("order-42")
            .raw(b"created")
            .to("orders")
            .with_headers(headers)
            .publish()
            .await
            .expect("the recorder accepts the message");

        let (.., headers) = recorder.last();
        assert_eq!(headers.get_str(PARTITION_KEY_HEADER), Some("order-42"));
    }
}
