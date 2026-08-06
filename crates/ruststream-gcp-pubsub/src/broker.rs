//! The broker ladder: [`PubSubBroker`] -> [`ConnectedPubSubBroker`].
//!
//! Construction is synchronous and I/O-free; authentication and connection setup happen in the
//! consuming [`Broker::connect`], and the connected form holds the live clients directly. One
//! shared cell remains so publishers can be handed out while the application is still being
//! assembled, before `connect` runs.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use google_cloud_auth::credentials::{Credentials, anonymous};
use google_cloud_pubsub::client::{
    BasePublisher, Publisher, Subscriber, SubscriptionAdmin, TopicAdmin,
};
use ruststream::{Broker, ConnectedBroker, DefaultPublish, DescribeServer, ServerSpec, Subscribe};
use tokio::sync::OnceCell;

use crate::error::{PubSubError, box_err};
use crate::publisher::{PubSubPublish, PubSubPublisher};
use crate::subscriber::PubSubSubscriber;
use crate::subscription::PubSubSubscription;

/// The live client state shared by the connected form and every handle derived from it.
///
/// Why runtime checks exist here at all: publishers may be handed out before `connect` and may
/// outlive `shutdown` (aliasing), so the dead-connection path must be a runtime error - the
/// typed ladder covers only the owner's handle.
pub(crate) struct Core {
    pub(crate) subscriber: Subscriber,
    pub(crate) base_publisher: BasePublisher,
    pub(crate) topic_admin: TopicAdmin,
    pub(crate) subscription_admin: SubscriptionAdmin,
    pub(crate) project: String,
    pub(crate) closed: AtomicBool,
    /// Per-topic publisher handles, shared by every publisher handle so shutdown can flush
    /// them all.
    pub(crate) publishers: tokio::sync::Mutex<std::collections::HashMap<String, Publisher>>,
}

impl Core {
    pub(crate) fn ensure_open(&self) -> Result<(), PubSubError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(PubSubError::NotConnected);
        }
        Ok(())
    }

    /// Resolves a short topic id to a full resource name; full names pass through.
    pub(crate) fn topic_name(&self, topic: &str) -> String {
        if topic.starts_with("projects/") {
            topic.to_owned()
        } else {
            format!("projects/{}/topics/{topic}", self.project)
        }
    }

    /// Resolves a short subscription id to a full resource name; full names pass through.
    pub(crate) fn subscription_name(&self, subscription: &str) -> String {
        if subscription.starts_with("projects/") {
            subscription.to_owned()
        } else {
            format!("projects/{}/subscriptions/{subscription}", self.project)
        }
    }
}

impl std::fmt::Debug for Core {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Core")
            .field("project", &self.project)
            .field("closed", &self.closed.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

pub(crate) type CoreCell = Arc<OnceCell<Arc<Core>>>;

/// A Google Cloud Pub/Sub broker for the `RustStream` messaging framework.
///
/// `new` is synchronous and records only configuration; the runtime authenticates and connects
/// once at startup via the consuming [`Broker::connect`]. That is what lets a service compose
/// with the synchronous `#[ruststream::app]` builder.
///
/// # Examples
///
/// ```
/// use ruststream_gcp_pubsub::PubSubBroker;
///
/// let broker = PubSubBroker::new("my-project"); // Application Default Credentials
/// let local = PubSubBroker::new("my-project").emulator("localhost:8085");
/// # let _ = (broker, local);
/// ```
#[derive(Clone)]
#[must_use]
pub struct PubSubBroker {
    project: String,
    credentials: Option<Credentials>,
    endpoint: Option<String>,
    emulator: Option<String>,
    // Shared with publishers handed out before connect; the consuming connect fills it.
    cell: CoreCell,
}

impl std::fmt::Debug for PubSubBroker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PubSubBroker")
            .field("project", &self.project)
            .field("emulator", &self.emulator)
            .finish_non_exhaustive()
    }
}

impl PubSubBroker {
    /// Records the project id; Application Default Credentials by default. No I/O.
    pub fn new(project: impl Into<String>) -> Self {
        Self {
            project: project.into(),
            credentials: None,
            endpoint: None,
            emulator: None,
            cell: Arc::new(OnceCell::new()),
        }
    }

    /// Uses explicit credentials instead of Application Default Credentials.
    pub fn credentials(mut self, credentials: Credentials) -> Self {
        self.credentials = Some(credentials);
        self
    }

    /// Overrides the service endpoint (for example a regional endpoint, which is what keeps
    /// ordering keys ordered across publishers in one region).
    pub fn endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    /// Targets a local Pub/Sub emulator at `host:port`: plaintext transport and anonymous
    /// credentials. The client does not honour `PUBSUB_EMULATOR_HOST` on its own, so the host
    /// is explicit here.
    pub fn emulator(mut self, host: impl Into<String>) -> Self {
        self.emulator = Some(host.into());
        self
    }

    /// A publisher sharing this broker's connection cell; buildable before `connect`.
    #[must_use]
    pub fn publisher(&self) -> PubSubPublisher {
        PubSubPublisher::new(Arc::clone(&self.cell))
    }
}

macro_rules! build_client {
    ($builder:expr, $endpoint:expr, $credentials:expr) => {{
        let mut b = $builder;
        if let Some(endpoint) = $endpoint {
            b = b.with_endpoint(endpoint.clone());
        }
        if let Some(credentials) = $credentials {
            b = b.with_credentials(credentials.clone());
        }
        b.build()
            .await
            .map_err(|e| PubSubError::Connect(box_err(e)))
    }};
}

impl Broker for PubSubBroker {
    type Error = PubSubError;
    type Connected = ConnectedPubSubBroker;

    async fn connect(self) -> Result<Self::Connected, Self::Error> {
        let core = self
            .cell
            .get_or_try_init(async || {
                let (endpoint, credentials) = if let Some(host) = &self.emulator {
                    (
                        Some(format!("http://{host}")),
                        Some(anonymous::Builder::new().build()),
                    )
                } else {
                    (self.endpoint.clone(), self.credentials.clone())
                };

                let subscriber: Subscriber =
                    build_client!(Subscriber::builder(), &endpoint, &credentials)?;
                let base_publisher: BasePublisher =
                    build_client!(BasePublisher::builder(), &endpoint, &credentials)?;
                let topic_admin: TopicAdmin =
                    build_client!(TopicAdmin::builder(), &endpoint, &credentials)?;
                let subscription_admin: SubscriptionAdmin =
                    build_client!(SubscriptionAdmin::builder(), &endpoint, &credentials)?;

                Ok::<_, PubSubError>(Arc::new(Core {
                    subscriber,
                    base_publisher,
                    topic_admin,
                    subscription_admin,
                    project: self.project.clone(),
                    closed: AtomicBool::new(false),
                    publishers: tokio::sync::Mutex::new(std::collections::HashMap::new()),
                }))
            })
            .await?
            .clone();
        Ok(ConnectedPubSubBroker {
            core,
            cell: self.cell,
        })
    }
}

impl DescribeServer for PubSubBroker {
    fn describe_server(&self) -> ServerSpec {
        let host = self
            .emulator
            .clone()
            .or_else(|| self.endpoint.clone())
            .unwrap_or_else(|| "pubsub.googleapis.com".to_owned());
        ServerSpec::new(host, "googlepubsub")
    }
}

/// The typed witness that `connect` succeeded: holds the live clients directly.
#[derive(Debug)]
pub struct ConnectedPubSubBroker {
    pub(crate) core: Arc<Core>,
    // Keeps the cell of publishers handed out before connect alive and filled.
    cell: CoreCell,
}

impl ConnectedPubSubBroker {
    /// A publisher from the connected form. It rides the same cell-backed publisher type as
    /// the early path; by now `connect` has filled the cell, so it resolves immediately.
    #[must_use]
    pub fn publisher(&self) -> PubSubPublisher {
        PubSubPublisher::new(Arc::clone(&self.cell))
    }

    /// Opens the subscription described by `descriptor`.
    ///
    /// # Errors
    ///
    /// Returns [`PubSubError`] when the descriptor is invalid, resource creation (when opted
    /// in) fails, or the broker is shut down.
    pub async fn subscribe_descriptor(
        &self,
        descriptor: PubSubSubscription,
    ) -> Result<PubSubSubscriber, PubSubError> {
        descriptor.validate()?;
        self.core.ensure_open()?;

        if let Some(topic) = descriptor.create_topic_ref() {
            self.ensure_topic(topic).await?;
            self.ensure_subscription(descriptor.subscription(), topic)
                .await?;
        }

        Ok(PubSubSubscriber::open(&self.core, &descriptor))
    }

    /// Creates `topic` when it does not exist. Get-then-create: a lost race means the create
    /// fails on an existing resource, which the re-get resolves.
    async fn ensure_topic(&self, topic: &str) -> Result<(), PubSubError> {
        let name = self.core.topic_name(topic);
        let admin = &self.core.topic_admin;
        if admin
            .get_topic()
            .set_topic(name.clone())
            .send()
            .await
            .is_ok()
        {
            return Ok(());
        }
        match admin.create_topic().set_name(name.clone()).send().await {
            Ok(_) => Ok(()),
            Err(create_err) => {
                if admin
                    .get_topic()
                    .set_topic(name.clone())
                    .send()
                    .await
                    .is_ok()
                {
                    Ok(())
                } else {
                    Err(PubSubError::Admin {
                        name,
                        source: box_err(create_err),
                    })
                }
            }
        }
    }

    async fn ensure_subscription(
        &self,
        subscription: &str,
        topic: &str,
    ) -> Result<(), PubSubError> {
        let name = self.core.subscription_name(subscription);
        let topic_name = self.core.topic_name(topic);
        let admin = &self.core.subscription_admin;
        if admin
            .get_subscription()
            .set_subscription(name.clone())
            .send()
            .await
            .is_ok()
        {
            return Ok(());
        }
        match admin
            .create_subscription()
            .set_name(name.clone())
            .set_topic(topic_name)
            .send()
            .await
        {
            Ok(_) => Ok(()),
            Err(create_err) => {
                if admin
                    .get_subscription()
                    .set_subscription(name.clone())
                    .send()
                    .await
                    .is_ok()
                {
                    Ok(())
                } else {
                    Err(PubSubError::Admin {
                        name,
                        source: box_err(create_err),
                    })
                }
            }
        }
    }
}

impl ConnectedBroker for ConnectedPubSubBroker {
    type Error = PubSubError;
    type Closed = ();

    async fn shutdown(self) -> Result<(), Self::Error> {
        self.core.closed.store(true, Ordering::Release);
        // Flush every cached per-topic publisher so buffered batches reach the service; the
        // client has no explicit close beyond dropping the handles.
        let publishers: Vec<_> = self
            .core
            .publishers
            .lock()
            .await
            .values()
            .cloned()
            .collect();
        for publisher in publishers {
            publisher.flush().await;
        }
        Ok(())
    }
}

impl Subscribe for ConnectedPubSubBroker {
    type Subscriber = PubSubSubscriber;

    async fn subscribe(&self, name: &str) -> Result<Self::Subscriber, Self::Error> {
        self.subscribe_descriptor(PubSubSubscription::new(name))
            .await
    }
}

impl DefaultPublish for ConnectedPubSubBroker {
    type Policy = PubSubPublish;
}
