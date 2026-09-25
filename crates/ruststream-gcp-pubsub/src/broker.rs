//! The broker ladder: [`PubSubBroker`] -> [`ConnectedPubSubBroker`].
//!
//! Construction is synchronous and I/O-free; authentication and connection setup happen in the
//! consuming [`Broker::connect`], and the connected form holds the live clients directly. One
//! shared cell remains so publishers can be handed out while the application is still being
//! assembled, before `connect` runs.

// Without the `testing` feature a link has one variant, so a `match` on it has a single arm; the
// matches stay so that the in-process arm has its place when the feature is on.
#![cfg_attr(
    not(feature = "testing"),
    allow(clippy::infallible_destructuring_match)
)]

use std::collections::HashMap;
#[cfg(feature = "testing")]
use std::future::{Future, ready};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(feature = "testing")]
use std::sync::{Mutex, PoisonError};

#[cfg(feature = "testing")]
use bytes::BytesMut;
use google_cloud_auth::credentials::{Credentials, anonymous};
use google_cloud_pubsub::client::{
    BasePublisher, Publisher, Subscriber, SubscriptionAdmin, TopicAdmin,
};
use google_cloud_pubsub::model::{DeadLetterPolicy, Subscription};
use google_cloud_wkt::FieldMask;
#[cfg(feature = "testing")]
use ruststream::testing::{Coordinator, InProcess, TestableBroker};
use ruststream::{
    Broker, BrokerMoves, ConnectedBroker, DeclareRetryError, DefaultPublish, DescribeServer,
    RetryDeclaration, ServerSpec, Subscribe,
};
#[cfg(feature = "testing")]
use ruststream::{OutgoingMessage, RawMessage};
use tokio::runtime::Handle;
use tokio::sync::OnceCell;

use crate::error::{PubSubError, box_err};
#[cfg(feature = "testing")]
use crate::in_process::{self, Project};
#[cfg(feature = "testing")]
use crate::message::to_gcp_message;
#[cfg(feature = "testing")]
use crate::publisher::resolve_ordering_key;
use crate::publisher::{PubSubPublish, PubSubPublisher};
use crate::runtime_slot::{RuntimeRegistration, RuntimeSlot};
use crate::subscriber::PubSubSubscriber;
use crate::subscription::{DeclaredRetries, GooglePubSub};

/// Whether a get-then-create found the resource or made it, which is what decides between
/// carrying a dead-letter policy into the create and writing it as an update afterwards.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Resource {
    Created,
    Existing,
}

/// The full resource name of the topic `topic` names in `project`: a short id is placed in the
/// project, a full name passes through.
pub(crate) fn topic_path(project: &str, topic: &str) -> String {
    if topic.starts_with("projects/") {
        topic.to_owned()
    } else {
        format!("projects/{project}/topics/{topic}")
    }
}

/// The full resource name of the subscription `subscription` names in `project`, resolved the
/// way [`topic_path`] resolves a topic.
pub(crate) fn subscription_path(project: &str, subscription: &str) -> String {
    if subscription.starts_with("projects/") {
        subscription.to_owned()
    } else {
        format!("projects/{project}/subscriptions/{subscription}")
    }
}

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
    pub(crate) publishers: tokio::sync::Mutex<HashMap<String, Publisher>>,
    /// What the registrations mounted by a bare subscription name declared about their retries,
    /// taken at startup and applied when each subscription opens.
    pub(crate) declared_retries: DeclaredRetries,
    /// Where a delivery of this connection finds the runtime `connect` ran on.
    runtime: RuntimeRegistration,
    /// The topic each opened subscription is attached to, by full resource name, which is what
    /// a live test harness asks to learn which subscriptions a publish reaches.
    #[cfg(feature = "testing")]
    attached: Mutex<HashMap<String, String>>,
}

impl Core {
    pub(crate) fn ensure_open(&self) -> Result<(), PubSubError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(PubSubError::NotConnected);
        }
        Ok(())
    }

    /// The slot every delivery of this connection carries, which finds the runtime `connect`
    /// ran on.
    pub(crate) const fn runtime_slot(&self) -> RuntimeSlot {
        self.runtime.slot()
    }

    /// Resolves a short topic id to a full resource name; full names pass through.
    pub(crate) fn topic_name(&self, topic: &str) -> String {
        topic_path(&self.project, topic)
    }

    /// Resolves a short subscription id to a full resource name; full names pass through.
    pub(crate) fn subscription_name(&self, subscription: &str) -> String {
        subscription_path(&self.project, subscription)
    }

    /// Opens the subscription `descriptor` describes against the service, creating its topology
    /// first where the descriptor opts in. The subscription's own tasks run on `runtime`.
    async fn subscribe(
        &self,
        descriptor: GooglePubSub,
        runtime: &Handle,
    ) -> Result<PubSubSubscriber, PubSubError> {
        self.ensure_open()?;

        let policy = descriptor.dead_letter_policy();
        let state = match descriptor.create_topic_ref() {
            Some(topic) => {
                self.ensure_topic(topic).await?;
                if let Some((dead_letter, _)) = policy {
                    // The API refuses a dead-letter policy naming a topic that is not there, and
                    // a descriptor creating its own topology owns this one too.
                    self.ensure_topic(dead_letter).await?;
                }
                self.ensure_subscription(descriptor.subscription(), topic, policy)
                    .await?
            }
            None => Resource::Existing,
        };
        // A subscription managed as infrastructure already exists, so the declaration reaches it
        // as an update; the create above carried it and needs no second call.
        if let (Some((dead_letter, attempts)), Resource::Existing) = (policy, state) {
            self.set_dead_letter_policy(descriptor.subscription(), dead_letter, attempts)
                .await?;
        }
        #[cfg(feature = "testing")]
        self.learn_attachment(&descriptor).await?;

        Ok(PubSubSubscriber::open(self, &descriptor, runtime))
    }

    /// Records the topic the subscription `descriptor` opens is attached to, as the service
    /// reports it.
    ///
    /// Only a test build asks: a live test harness waits on every subscription a publish reaches,
    /// and a subscription managed as infrastructure says which topic that is only to the service.
    /// Where the service does not answer, the descriptor's own topic stands in.
    ///
    /// # Errors
    ///
    /// Returns [`PubSubError::Admin`] when the service refuses the lookup (a subscription it does
    /// not know aside) and the descriptor names no topic: the harness would not wait for this
    /// subscription, and a test would settle before its handler ran.
    #[cfg(feature = "testing")]
    async fn learn_attachment(&self, descriptor: &GooglePubSub) -> Result<(), PubSubError> {
        let name = self.subscription_name(descriptor.subscription());
        let reported = self
            .subscription_admin
            .get_subscription()
            .set_subscription(name.clone())
            .send()
            .await
            .map(|subscription| subscription.topic);
        let topic = match reported {
            Ok(topic) if !topic.is_empty() => topic,
            // A subscription that does not exist receives nothing, and its stream reports it.
            Err(err) if err.http_status_code() == Some(404) => {
                return Ok(());
            }
            reported => match descriptor.create_topic_ref() {
                Some(topic) => self.topic_name(topic),
                None => {
                    return Err(PubSubError::Admin {
                        name,
                        source: reported.err().map_or_else(
                            || Box::from("the service reports no topic for the subscription"),
                            box_err,
                        ),
                    });
                }
            },
        };
        self.attached
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(name, topic);
        Ok(())
    }

    /// The topic the subscription `subscription` is attached to, where a live test learned it.
    #[cfg(feature = "testing")]
    fn attached_topic(&self, subscription: &str) -> Option<String> {
        self.attached
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&self.subscription_name(subscription))
            .cloned()
    }

    /// Writes the registration's dead-letter policy onto a subscription that already exists.
    ///
    /// Pub/Sub then stops redelivering a message once it has had `attempts` deliveries and
    /// publishes it to `dead_letter` instead, which is the whole of what the declaration buys on
    /// this broker.
    async fn set_dead_letter_policy(
        &self,
        subscription: &str,
        dead_letter: &str,
        attempts: i32,
    ) -> Result<(), PubSubError> {
        let name = self.subscription_name(subscription);
        let policy = DeadLetterPolicy::new()
            .set_dead_letter_topic(self.topic_name(dead_letter))
            .set_max_delivery_attempts(attempts);
        self.subscription_admin
            .update_subscription()
            .set_subscription(
                Subscription::new()
                    .set_name(name.clone())
                    .set_dead_letter_policy(policy),
            )
            .set_update_mask(FieldMask::default().set_paths(["dead_letter_policy"]))
            .send()
            .await
            .map_err(|err| PubSubError::Admin {
                name,
                source: box_err(err),
            })?;
        Ok(())
    }

    /// Creates `topic` when it does not exist. Get-then-create: a lost race means the create
    /// fails on an existing resource, which the re-get resolves.
    async fn ensure_topic(&self, topic: &str) -> Result<(), PubSubError> {
        let name = self.topic_name(topic);
        let admin = &self.topic_admin;
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
        dead_letter: Option<(&str, i32)>,
    ) -> Result<Resource, PubSubError> {
        let name = self.subscription_name(subscription);
        let topic_name = self.topic_name(topic);
        let admin = &self.subscription_admin;
        if admin
            .get_subscription()
            .set_subscription(name.clone())
            .send()
            .await
            .is_ok()
        {
            return Ok(Resource::Existing);
        }
        let mut create = admin
            .create_subscription()
            .set_name(name.clone())
            .set_topic(topic_name)
            // An ordering key orders deliveries only where the subscription says so, and this
            // is the only subscription the crate owns. Without the flag a service would order
            // its messages against the infrastructure it ships and not against the topology it
            // creates for a test, which is the difference a test is there to catch.
            .set_enable_message_ordering(true);
        if let Some((dead_letter, attempts)) = dead_letter {
            create = create.set_dead_letter_policy(
                DeadLetterPolicy::new()
                    .set_dead_letter_topic(self.topic_name(dead_letter))
                    .set_max_delivery_attempts(attempts),
            );
        }
        match create.send().await {
            Ok(_) => Ok(Resource::Created),
            Err(create_err) => {
                if admin
                    .get_subscription()
                    .set_subscription(name.clone())
                    .send()
                    .await
                    .is_ok()
                {
                    Ok(Resource::Existing)
                } else {
                    Err(PubSubError::Admin {
                        name,
                        source: box_err(create_err),
                    })
                }
            }
        }
    }

    /// Marks the connection closed and flushes every cached per-topic publisher, so buffered
    /// batches reach the service; the client has no explicit close beyond dropping the handles.
    async fn shutdown(&self) {
        self.closed.store(true, Ordering::Release);
        let publishers: Vec<_> = self.publishers.lock().await.values().cloned().collect();
        for publisher in publishers {
            publisher.flush().await;
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

/// What a connected broker and every handle paired off it speak over: the service's clients, or,
/// under the `testing` feature, the in-process transport the test harness connected instead.
///
/// Without the feature there is one variant, so the type is the client handle itself and every
/// `match` on it is irrefutable: a production build carries no second transport and no branch to
/// it.
#[derive(Debug, Clone)]
pub(crate) enum Link {
    Service(Arc<Core>),
    #[cfg(feature = "testing")]
    InProcess(Arc<Project>),
}

// The zero-cost promise of the in-process mode, held by the compiler: a build without it gives the
// link exactly the size of the client handle it wraps.
#[cfg(not(feature = "testing"))]
const _: () = assert!(size_of::<Link>() == size_of::<Arc<Core>>());

impl Link {
    /// What the bare-name registrations of this connection declared about their retries.
    fn declared_retries(&self) -> &DeclaredRetries {
        match self {
            Self::Service(core) => &core.declared_retries,
            #[cfg(feature = "testing")]
            Self::InProcess(project) => project.declared_retries(),
        }
    }
}

pub(crate) type CoreCell = Arc<OnceCell<Link>>;

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
        let link = self
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

                Ok::<_, PubSubError>(Link::Service(Arc::new(Core {
                    subscriber,
                    base_publisher,
                    topic_admin,
                    subscription_admin,
                    project: self.project.clone(),
                    closed: AtomicBool::new(false),
                    publishers: tokio::sync::Mutex::new(HashMap::new()),
                    declared_retries: DeclaredRetries::default(),
                    runtime: RuntimeRegistration::new(Handle::current()),
                    #[cfg(feature = "testing")]
                    attached: Mutex::new(HashMap::new()),
                })))
            })
            .await?
            .clone();
        Ok(ConnectedPubSubBroker {
            link,
            cell: self.cell,
            runtime: Handle::current(),
        })
    }
}

/// The in-process mode: the connected form a test runs the production app against, carrying the
/// in-process transport in place of the service's clients and this broker's project.
///
/// Nothing is dialled and no credentials are read, so a test runs where the service could not
/// authenticate. A publisher handed out before the transition shares the connection cell, so it
/// publishes in process too.
#[cfg(feature = "testing")]
impl InProcess for PubSubBroker {
    fn connect_in_process(
        self,
    ) -> impl Future<Output = Result<Self::Connected, Self::Error>> + Send {
        // A clone of this broker that already connected in process shares the cell, and that
        // transport is the one every handle of the broker speaks over. A clone connected to the
        // service filled it with a connection the harness cannot drive, and a test must not
        // publish to the service.
        let link = self.cell.get().cloned().unwrap_or_else(|| {
            let project = Link::InProcess(Project::new(self.project.clone(), Handle::current()));
            let _ = self.cell.set(project.clone());
            self.cell.get().cloned().unwrap_or(project)
        });
        if let Link::Service(_) = link {
            return ready(Err(PubSubError::Connect(Box::from(
                "a clone of this broker is connected to the service already, so it cannot \
                 connect in process",
            ))));
        }
        ready(Ok(ConnectedPubSubBroker {
            link,
            cell: self.cell,
            // Taken when the transition is called, which is on the runtime that awaits it.
            runtime: Handle::current(),
        }))
    }
}

#[cfg(feature = "testing")]
ruststream::register_testable_broker!(PubSubBroker);

impl DescribeServer for PubSubBroker {
    /// The address clients connect to, and nothing else. An operator writes whatever the client
    /// accepts, so an endpoint may carry a scheme, a path or credentials; the description goes
    /// into a document teams share, and `ServerSpec::host_from_url` is what keeps the rest of it
    /// out.
    fn describe_server(&self) -> ServerSpec {
        let host = self
            .emulator
            .as_deref()
            .or(self.endpoint.as_deref())
            .map_or_else(
                || "pubsub.googleapis.com".to_owned(),
                ServerSpec::host_from_url,
            );
        ServerSpec::new(host, "googlepubsub")
    }
}

/// The typed witness that `connect` succeeded: holds the live clients directly.
#[derive(Debug)]
pub struct ConnectedPubSubBroker {
    link: Link,
    // Keeps the cell of publishers handed out before connect alive and filled.
    cell: CoreCell,
    /// The runtime `connect` ran on. Every task the broker starts runs here: a subscription's
    /// pump and a delayed rejection, whichever thread opens the subscription or settles the
    /// delivery.
    runtime: Handle,
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
        descriptor: GooglePubSub,
    ) -> Result<PubSubSubscriber, PubSubError> {
        descriptor.validate()?;
        let core = match &self.link {
            Link::Service(core) => core,
            #[cfg(feature = "testing")]
            Link::InProcess(project) => {
                return in_process::subscribe(project, &descriptor);
            }
        };
        core.subscribe(descriptor, &self.runtime).await
    }
}

impl ConnectedBroker for ConnectedPubSubBroker {
    type Error = PubSubError;
    type Closed = ();

    async fn shutdown(self) -> Result<(), Self::Error> {
        match &self.link {
            Link::Service(core) => core.shutdown().await,
            #[cfg(feature = "testing")]
            Link::InProcess(project) => project.close(),
        }
        Ok(())
    }
}

impl Subscribe for ConnectedPubSubBroker {
    type Subscriber = PubSubSubscriber;
    // A bare name opens the descriptor's own default subscription, and a Pub/Sub subscription
    // moves a spent delivery itself.
    type Copies = BrokerMoves;

    async fn subscribe(&self, name: &str) -> Result<Self::Subscriber, Self::Error> {
        self.subscribe_descriptor(self.link.declared_retries().source(name))
            .await
    }

    /// A bare name has no descriptor to declare on, so the broker takes the declaration for the
    /// subscription this name opens: `dead_letter(topic)` becomes its `deadLetterTopic` and
    /// `max_attempts(n)` its `maxDeliveryAttempts`, written when
    /// [`subscribe`](Self::subscribe) opens the subscription.
    fn declare_retry(
        &self,
        name: &str,
        declaration: &RetryDeclaration,
    ) -> Result<(), DeclareRetryError> {
        self.link.declared_retries().take(name, declaration)
    }
}

impl DefaultPublish for ConnectedPubSubBroker {
    type Policy = PubSubPublish;
}

/// The harness's view of the connected broker: what it injects, what it reads back, the
/// coordinator it counts in-flight deliveries with, and which subscriptions a publish reaches.
///
/// Pub/Sub routes by attachment: a message published to a topic reaches every subscription
/// attached to that topic, each once, and no other. [`routes`](TestableBroker::routes) answers
/// that rule over the topic each subscription is attached to, in both modes: in process from the
/// transport's own record, live from what the service reported when the subscription opened.
///
/// # Panics
///
/// `inject` and `published` panic on a broker connected with `connect`: the harness drives only
/// the connection `connect_in_process` produced, and a live connection has no log to read and no
/// synchronous way to take a message. `inject` also panics on a message the service refuses, which
/// is a test that could not have run against Pub/Sub.
#[cfg(feature = "testing")]
impl TestableBroker for ConnectedPubSubBroker {
    fn install_coordinator(&self, coordinator: Coordinator) {
        if let Link::InProcess(project) = &self.link {
            project.install(coordinator);
        }
    }

    fn inject(&self, message: OutgoingMessage<'_>) {
        let project = self.project("inject");
        // An external producer publishes the bytes with the headers as attributes, and names the
        // ordering key the way this crate's own publisher reads it.
        let key = resolve_ordering_key(message.headers(), None, None);
        let wire = to_gcp_message(
            BytesMut::from(message.payload()),
            message.headers(),
            key.as_deref(),
        );
        if let Err(err) = project.publish(message.name(), &wire) {
            panic!(
                "the injected message to {:?} is not one Pub/Sub takes: {err}",
                message.name()
            );
        }
    }

    fn published(&self, name: &str) -> Vec<RawMessage> {
        self.project("published").published(name)
    }

    fn routes(&self, destination: &str, subscriptions: &[&str]) -> Vec<usize> {
        let (topic, attached): (String, Vec<Option<String>>) = match &self.link {
            Link::Service(core) => (
                core.topic_name(destination),
                subscriptions
                    .iter()
                    .map(|name| core.attached_topic(name))
                    .collect(),
            ),
            Link::InProcess(project) => (
                project.topic_path(destination),
                subscriptions
                    .iter()
                    .map(|name| project.attached_topic(name))
                    .collect(),
            ),
        };
        attached
            .iter()
            .enumerate()
            .filter(|(_, bound)| bound.as_deref() == Some(topic.as_str()))
            .map(|(position, _)| position)
            .collect()
    }
}

#[cfg(feature = "testing")]
impl ConnectedPubSubBroker {
    /// The in-process transport, which is all the harness drives.
    fn project(&self, what: &str) -> &Arc<Project> {
        match &self.link {
            Link::InProcess(project) => project,
            Link::Service(_) => panic!(
                "TestableBroker::{what} reached a broker connected with `connect`; the harness \
                 drives the connection `connect_in_process` produces"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn described(broker: &PubSubBroker) -> String {
        broker
            .describe_server()
            .host
            .expect("a Pub/Sub server always has an address")
    }

    #[test]
    fn the_default_server_is_the_public_endpoint() {
        assert_eq!(
            described(&PubSubBroker::new("p")),
            "pubsub.googleapis.com".to_owned()
        );
    }

    #[test]
    fn an_emulator_address_is_reported_as_written() {
        // The form the documentation puts in front of a reader.
        assert_eq!(
            described(&PubSubBroker::new("p").emulator("localhost:8085")),
            "localhost:8085".to_owned()
        );
    }

    /// Every endpoint form this crate accepts reduces to the host and port, and nothing an
    /// operator wrote around it reaches a document teams share.
    #[test]
    fn an_endpoint_is_reported_as_host_and_port() {
        for (written, expected) in [
            ("localhost:8085", "localhost:8085"),
            ("http://localhost:8085", "localhost:8085"),
            (
                "https://us-east1-pubsub.googleapis.com",
                "us-east1-pubsub.googleapis.com",
            ),
            ("https://pubsub.googleapis.com/v1", "pubsub.googleapis.com"),
            (
                "https://pubsub.googleapis.com/v1?alt=json",
                "pubsub.googleapis.com",
            ),
            ("http://user:pass@localhost:8085", "localhost:8085"),
            // The path holds the only `@`, so cutting on it before the path is removed would
            // report `b` as the host.
            ("https://pubsub.googleapis.com/a@b", "pubsub.googleapis.com"),
            // Credentials carrying an `@` of their own: the host follows the last one.
            ("http://user:p@ss@localhost:8085", "localhost:8085"),
            ("http://[::1]:8085", "[::1]:8085"),
        ] {
            let host = described(&PubSubBroker::new("p").endpoint(written));
            assert_eq!(host, expected.to_owned(), "endpoint {written:?}");
            assert!(
                !host.contains("://"),
                "scheme reached the description: {host}"
            );
            assert!(
                !host.contains('@'),
                "credentials reached the description: {host}"
            );
        }
    }

    /// The emulator takes the same path: it is an endpoint an operator writes too.
    #[test]
    fn an_emulator_endpoint_is_stripped_the_same_way() {
        let host = described(&PubSubBroker::new("p").emulator("http://user:pass@127.0.0.1:8085"));
        assert_eq!(host, "127.0.0.1:8085".to_owned());
    }
}
