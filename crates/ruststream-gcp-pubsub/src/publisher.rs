//! [`PubSubPublisher`] and its [`PubSubPublish`] policy.

use google_cloud_pubsub::client::Publisher as GcpPublisher;
use ruststream::{OutgoingMessage, PairError, PublishPolicy, Publisher};

use crate::broker::{ConnectedPubSubBroker, Core, CoreCell};
use crate::error::{PubSubError, box_err};
use crate::message::to_gcp_message;

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

impl std::fmt::Debug for PubSubPublisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
