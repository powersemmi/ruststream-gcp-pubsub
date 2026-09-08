//! [`PubSubTestBroker`]: the in-process transport and its connected form.

use std::future::{Future, ready};
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use ruststream::testing::{Coordinator, TestableBroker};
use ruststream::{
    Broker, ConnectedBroker, DefaultPublish, OutgoingMessage, PairError, PublishPolicy, Publisher,
    RawMessage, Subscribe,
};

use crate::error::PubSubError;
use crate::publisher::{PubSubOrdering, PubSubPublish};
use crate::subscription::PubSubSubscription;
use crate::testing::router::AddressRouter;
use crate::testing::subscriber::PubSubTestSubscriber;

/// Shared state of one in-process broker: the router plus the harness coordinator.
#[derive(Debug, Default)]
pub(crate) struct TestState {
    pub(crate) router: AddressRouter,
    coordinator: OnceLock<Coordinator>,
}

impl TestState {
    fn coordinator(&self) -> Option<&Coordinator> {
        self.coordinator.get()
    }

    pub(crate) fn publish(&self, name: &str, payload: Bytes, headers: ruststream::HeaderMap) {
        self.router
            .publish(name, payload, headers, self.coordinator());
    }
}

/// An in-process stand-in for [`PubSubBroker`](crate::PubSubBroker): same core routing, no server.
///
/// # Examples
///
/// ```
/// use ruststream_gcp_pubsub::testing::PubSubTestBroker;
///
/// let broker = PubSubTestBroker::new();
/// # let _ = broker;
/// ```
#[derive(Debug, Clone, Default)]
#[must_use]
pub struct PubSubTestBroker {
    state: Arc<TestState>,
}

impl PubSubTestBroker {
    /// Creates an empty in-process broker. Synchronous and I/O-free, like the real `new`.
    pub fn new() -> Self {
        Self::default()
    }

    /// A publisher usable before `connect`, mirroring the real broker's early-publisher path.
    #[must_use]
    pub fn publisher(&self) -> PubSubTestPublisher {
        PubSubTestPublisher {
            state: Arc::clone(&self.state),
        }
    }
}

impl Broker for PubSubTestBroker {
    type Error = PubSubError;
    type Connected = ConnectedPubSubTestBroker;

    fn connect(self) -> impl Future<Output = Result<Self::Connected, Self::Error>> {
        ready(Ok(ConnectedPubSubTestBroker { state: self.state }))
    }
}

/// The connected form of [`PubSubTestBroker`]; implements
/// [`TestableBroker`](ruststream::testing::TestableBroker) for the harness and the conformance
/// suite.
#[derive(Debug, Clone)]
pub struct ConnectedPubSubTestBroker {
    state: Arc<TestState>,
}

impl ConnectedPubSubTestBroker {
    /// A publisher from the connected form.
    #[must_use]
    pub fn publisher(&self) -> PubSubTestPublisher {
        PubSubTestPublisher {
            state: Arc::clone(&self.state),
        }
    }

    /// Opens the subscription described by `descriptor`, so a service mounts the descriptor it
    /// runs in production. Mirrors the real broker's
    /// [`subscribe_descriptor`](crate::ConnectedPubSubBroker::subscribe_descriptor).
    ///
    /// The stand-in routes by one address, and that address is the subscription name: it holds
    /// no topics, so it has no topic-to-subscription binding to route through. What the
    /// descriptor says about the service - the batch deadline - carries over; what it says
    /// about the product does not. [`PubSubSubscription`] carries the full ledger, on its
    /// `SubscriptionSource` impl for this broker.
    ///
    /// # Errors
    ///
    /// Returns [`PubSubError::InvalidDescriptor`] when the descriptor names no subscription, on
    /// the same check the real broker runs before any I/O.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::Broker;
    /// use ruststream_gcp_pubsub::PubSubSubscription;
    /// use ruststream_gcp_pubsub::testing::PubSubTestBroker;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), ruststream_gcp_pubsub::PubSubError> {
    /// let broker = PubSubTestBroker::new().connect().await?;
    /// let subscriber = broker
    ///     .subscribe_descriptor(PubSubSubscription::new("orders-workers"))
    ///     .await?;
    /// # let _ = subscriber;
    /// # Ok(())
    /// # }
    /// ```
    pub fn subscribe_descriptor(
        &self,
        descriptor: PubSubSubscription,
    ) -> impl Future<Output = Result<PubSubTestSubscriber, PubSubError>> {
        ready(self.open(descriptor))
    }

    /// The synchronous body of [`Self::subscribe_descriptor`]: nothing here awaits, and the
    /// future above exists for call-site parity with the real broker.
    fn open(&self, descriptor: PubSubSubscription) -> Result<PubSubTestSubscriber, PubSubError> {
        descriptor.validate()?;
        let batch_wait = descriptor.batch_wait_value();
        let (id, requeue, rx) = self.state.router.subscribe(descriptor.into_subscription());
        Ok(PubSubTestSubscriber::new(
            Arc::clone(&self.state),
            id,
            rx,
            requeue,
            self.state.coordinator().cloned(),
            batch_wait,
        ))
    }
}

impl ConnectedBroker for ConnectedPubSubTestBroker {
    type Error = PubSubError;
    type Closed = ();

    fn shutdown(self) -> impl Future<Output = Result<(), Self::Error>> {
        self.state.router.clear();
        ready(Ok(()))
    }
}

impl Subscribe for ConnectedPubSubTestBroker {
    type Subscriber = PubSubTestSubscriber;

    /// A name alone is the descriptor's own default form, so the two entry points open the same
    /// subscription here exactly as they do on the real broker.
    fn subscribe(&self, name: &str) -> impl Future<Output = Result<Self::Subscriber, Self::Error>> {
        self.subscribe_descriptor(PubSubSubscription::new(name))
    }
}

impl TestableBroker for ConnectedPubSubTestBroker {
    fn install_coordinator(&self, coordinator: Coordinator) {
        let _ = self.state.coordinator.set(coordinator);
    }

    fn inject(&self, message: OutgoingMessage<'_>) {
        self.state.publish(
            message.name(),
            Bytes::copy_from_slice(message.payload()),
            message.headers().clone(),
        );
    }

    fn published(&self, name: &str) -> Vec<RawMessage> {
        self.state.router.published(name)
    }
}

ruststream::register_testable_broker!(ConnectedPubSubTestBroker);

/// Publisher for the in-process broker.
#[derive(Debug, Clone)]
pub struct PubSubTestPublisher {
    state: Arc<TestState>,
}

impl Publisher for PubSubTestPublisher {
    type Error = PubSubError;

    fn publish(&self, msg: OutgoingMessage<'_>) -> impl Future<Output = Result<(), Self::Error>> {
        self.state.publish(
            msg.name(),
            Bytes::copy_from_slice(msg.payload()),
            msg.headers().clone(),
        );
        ready(Ok(()))
    }
}

// Keeps `with_ordering_key` callable in a test exactly as against the real broker.
impl PubSubOrdering for PubSubTestPublisher {}

/// The stand-in pairs the real [`PubSubPublish`] policy, so a routes file mounts on it with the
/// spelling it ships: `.out(Reply, Publish)` reads the same either way, and there is no test-only
/// policy to swap in. The policy holds no settings for the stand-in to honour - the destination
/// travels on the message and the ordering key on its header - so the pairing loses nothing.
impl PublishPolicy<ConnectedPubSubTestBroker> for PubSubPublish {
    type Live = PubSubTestPublisher;

    fn pair(
        self,
        connected: &ConnectedPubSubTestBroker,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(connected.publisher()))
    }
}

impl DefaultPublish for ConnectedPubSubTestBroker {
    type Policy = PubSubPublish;
}
