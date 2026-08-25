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

    pub(crate) fn publish(&self, name: &str, payload: Bytes, headers: ruststream::Headers) {
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

    fn subscribe(&self, name: &str) -> impl Future<Output = Result<Self::Subscriber, Self::Error>> {
        let (id, requeue, rx) = self.state.router.subscribe(name.to_owned());
        ready(Ok(PubSubTestSubscriber::new(
            Arc::clone(&self.state),
            id,
            rx,
            requeue,
            self.state.coordinator().cloned(),
        )))
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

/// The publish policy for [`PubSubTestPublisher`], mirroring
/// [`PubSubPublish`](crate::PubSubPublish) on the real broker.
///
/// # Examples
///
/// ```
/// use ruststream_gcp_pubsub::testing::PubSubTestPublish;
///
/// let policy = PubSubTestPublish::default();
/// # let _ = policy;
/// ```
#[derive(Debug, Clone, Copy, Default)]
#[must_use]
pub struct PubSubTestPublish;

impl PublishPolicy<ConnectedPubSubTestBroker> for PubSubTestPublish {
    type Live = PubSubTestPublisher;

    fn pair(
        self,
        connected: &ConnectedPubSubTestBroker,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(connected.publisher()))
    }
}

impl DefaultPublish for ConnectedPubSubTestBroker {
    type Policy = PubSubTestPublish;
}
