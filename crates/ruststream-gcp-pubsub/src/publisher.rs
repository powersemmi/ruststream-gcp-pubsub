//! [`PubSubPublisher`], its [`PubSubPublish`] policy, the [`PubSubPublishOptions`] a single
//! message may differ by, and the [`PubSubOrdering`] step that names one.

// Without the `testing` feature a link has one variant, so a `match` on it has a single arm; the
// match stays so that the in-process arm has its place when the feature is on.
#![cfg_attr(
    not(feature = "testing"),
    allow(clippy::infallible_destructuring_match)
)]

use std::fmt;
use std::future::{Future, ready};
use std::sync::Arc;

use bytes::BytesMut;
use google_cloud_pubsub::client::Publisher as GcpPublisher;
#[cfg(feature = "asyncapi")]
use ruststream::asyncapi::{Binding, Bindings};
use ruststream::runtime::{PublishBuilder, PublishSink};
use ruststream::{HeaderMap, OutgoingMessage, PairError, PublishPolicy, Publisher, Take};
#[cfg(feature = "asyncapi")]
use serde::Serialize;
use tokio::runtime::Handle;

use crate::broker::{ConnectedPubSubBroker, Core, CoreCell, Link};
use crate::error::{PubSubError, box_err};
use crate::message::{PARTITION_KEY_HEADER, header_text, to_gcp_message};

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
/// # #[cfg(feature = "testing")]
/// # mod demo {
/// use std::error::Error;
///
/// use ruststream::testing::TestApp;
/// use ruststream_gcp_pubsub::prelude::*;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Debug, Deserialize, Serialize, Outgoing)]
/// struct Order {
///     id: u64,
/// }
///
/// #[subscriber("orders-workers")]
/// async fn confirm(
///     order: &Order,
///     Out(out): Out<impl Publisher<Options = PubSubPublishOptions>>,
/// ) -> HandlerOutcome {
///     let sent = out
///         .message(order)
///         .to("confirmations")
///         .ordering_key(format!("order-{}", order.id))
///         .publish()
///         .await;
///     if sent.is_err() {
///         return HandlerOutcome::retry();
///     }
///     HandlerOutcome::ack()
/// }
///
/// /// The app `main` runs, and the one the test hands the harness.
/// pub fn app() -> impl App {
///     RustStream::new(AppInfo::new("orders", "0.1.0"))
///         .with_broker(PubSubBroker::new("my-project"), |b| {
///             b.include(confirm).out(DefaultSlot, Publish::default()).build();
///         })
/// }
///
/// pub async fn each_confirmation_carries_its_orders_key() -> Result<(), Box<dyn Error>> {
///     let tb = TestApp::start(app()).await?;
///
///     // A subscription the service only names is attached to the topic of its own name.
///     tb.broker::<PubSubBroker>()
///         .message(&Order { id: 42 })
///         .to("orders-workers")
///         .publish()
///         .await?;
///     tb.settle().await?;
///
///     tb.out::<DefaultSlot>()
///         .assert_called(1)
///         .with_options(&PubSubPublishOptions {
///             ordering_key: Some("order-42".to_owned()),
///         });
///     tb.shutdown().await?;
///     Ok(())
/// }
/// # }
/// # #[cfg(feature = "testing")]
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// #     demo::each_confirmation_carries_its_orders_key().await
/// # }
/// # #[cfg(not(feature = "testing"))]
/// # fn main() {}
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PubSubPublishOptions {
    /// The message's ordering key. Messages sharing a key reach one subscriber in publish
    /// order, on a subscription that enables message ordering; `None` leaves the publish
    /// unordered unless the policy fixed a key.
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

    /// What this handle speaks over, once `connect` has filled the shared cell.
    fn link(&self) -> Result<&Link, PubSubError> {
        self.cell.get().ok_or(PubSubError::NotConnected)
    }

    /// The per-topic client publisher, created on first use and cached on the core.
    async fn publisher_for(&self, core: &Core, topic: &str) -> GcpPublisher {
        let name = core.topic_name(topic);
        let mut publishers = core.publishers.lock().await;
        if let Some(publisher) = publishers.get(&name) {
            return publisher.clone();
        }
        // Sync and infallible off the connected BasePublisher; the network work happened in
        // connect. The client starts the topic's batching worker on the runtime `build` runs in,
        // and the first publish may come from a runtime that stops right after (a handler on a
        // dedicated thread), so the worker is started on the runtime the broker connected on.
        let publisher = {
            let connect_runtime = core.runtime_slot().runtime();
            let _entered = connect_runtime.as_ref().map(Handle::enter);
            core.base_publisher.publisher(name.clone()).build()
        };
        publishers.insert(name, publisher.clone());
        publisher
    }
}

/// The ordering key one publish carries, resolved over the three places it can come from.
///
/// The native spelling wins: the message's own settings, written by the
/// [`ordering_key`](PubSubOrdering::ordering_key) step at a call site or by a publish transform on
/// the position the message leaves through. Next comes the framework's `partition-key` header,
/// which is the portable spelling of the same key and keeps a service that names no broker able to
/// order its messages. What neither named is what the policy fixed for the mount site.
///
/// # Errors
///
/// Returns the reason when the key comes from a header that is not UTF-8: an ordering key is text,
/// and a rewritten one would order the message under a key nobody named.
pub(crate) fn resolve_ordering_key<'a>(
    headers: &'a HeaderMap,
    options: Option<&'a PubSubPublishOptions>,
    policy: Option<&'a str>,
) -> Result<Option<&'a str>, String> {
    if let Some(key) = options.and_then(|options| options.ordering_key.as_deref()) {
        return Ok(Some(key));
    }
    if let Some(value) = headers.get(PARTITION_KEY_HEADER) {
        return header_text(PARTITION_KEY_HEADER, value).map(Some);
    }
    Ok(policy)
}

impl Publisher for PubSubPublisher {
    /// The client keeps the payload: a Pub/Sub message's data is a `Bytes`, so the buffer the
    /// framework wrote becomes it.
    type Payload = Take;

    type Error = PubSubError;
    type Options = PubSubPublishOptions;

    async fn publish(
        &self,
        msg: OutgoingMessage<'_, BytesMut>,
        options: Option<&Self::Options>,
    ) -> Result<(), Self::Error> {
        let core = match self.link()? {
            Link::Service(core) => core,
            #[cfg(feature = "testing")]
            Link::InProcess(project) => {
                let (name, payload, headers) = msg.into_parts();
                let refused = |reason: String| PubSubError::Publish {
                    topic: project.topic_path(name),
                    source: reason.into(),
                };
                let key =
                    resolve_ordering_key(&headers, options, self.default_ordering_key.as_deref())
                        .map_err(refused)?;
                let message = to_gcp_message(payload, &headers, key).map_err(refused)?;
                return project.publish(name, &message);
            }
        };
        core.ensure_open()?;
        let name = msg.name();
        let (_, payload, headers) = msg.into_parts();
        let refused = |reason: String| PubSubError::Publish {
            topic: core.topic_name(name),
            source: reason.into(),
        };
        let key = resolve_ordering_key(&headers, options, self.default_ordering_key.as_deref())
            .map_err(refused)?;
        let message = to_gcp_message(payload, &headers, key).map_err(refused)?;
        let publisher = self.publisher_for(core, name).await;
        match publisher.publish(message).await {
            Ok(_message_id) => Ok(()),
            Err(err) => {
                // An error on an ordered key pauses the key; resume so the pause cannot wedge
                // every later publish on this key, and let the caller see this failure.
                if let Some(key) = key {
                    publisher.resume_publish(key.to_owned());
                }
                Err(PubSubError::Publish {
                    topic: core.topic_name(name),
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
/// # mod demo {
/// use ruststream_gcp_pubsub::prelude::*;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Debug, Deserialize, Serialize, Outgoing)]
/// struct Shipment {
///     order_id: u64,
///     warehouse: String,
/// }
///
/// /// Every update of one order travels under that order's key, so a consumer of
/// /// `shipment-updates` sees them in the order they were sent.
/// #[subscriber("shipments-workers")]
/// async fn track(
///     shipment: &Shipment,
///     Out(out): Out<impl Publisher<Options = PubSubPublishOptions>>,
/// ) -> HandlerOutcome {
///     let sent = out
///         .message(shipment)
///         .to("shipment-updates")
///         .ordering_key(format!("order-{}", shipment.order_id))
///         .publish()
///         .await;
///     match sent {
///         Ok(_) => HandlerOutcome::ack(),
///         Err(_) => HandlerOutcome::retry(),
///     }
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("shipments", "0.1.0"))
///         .with_broker(PubSubBroker::new("my-project"), |b| {
///             b.include(track).out(DefaultSlot, Publish::default()).build();
///         })
/// }
/// # }
/// # fn main() {}
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
/// # mod demo {
/// use ruststream_gcp_pubsub::prelude::*;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Debug, Deserialize)]
/// struct Order {
///     id: u64,
/// }
///
/// #[derive(Serialize, Outgoing)]
/// #[outgoing(name = "receipts")]
/// struct Receipt {
///     order_id: u64,
/// }
///
/// #[subscriber("orders-receipts", publish)]
/// async fn receipt(order: &Order) -> Receipt {
///     Receipt { order_id: order.id }
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("orders", "0.1.0"))
///         .with_broker(PubSubBroker::new("my-project"), |b| {
///             // A reply has no call site, so every receipt is ordered under the policy's key.
///             b.include(receipt).out_reply(Publish::default().ordering_key("receipts"));
///         })
/// }
/// # }
/// # fn main() {}
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

    /// What a document can say about the messages this mount site sends.
    ///
    /// Only the ordering key, and only where the mount site fixed one: everything else the
    /// `googlepubsub` binding describes belongs to the topic resource (its labels, its
    /// retention, its storage policy, its schema), which no publish policy configures and no
    /// destination name tells. A key named per message is named at a call site, and a call site
    /// is not in the document.
    #[cfg(feature = "asyncapi")]
    pub(crate) fn message_binding(&self) -> Bindings {
        let Some(ordering_key) = self.ordering_key.as_deref() else {
            return Bindings::new();
        };
        let body = PubSubMessageBinding { ordering_key };
        // A binding that fails to build is a binding the document goes without: a broker never
        // holds up a service over a description of itself.
        Binding::new("googlepubsub", BINDING_VERSION, &body)
            .map_or_else(|_| Bindings::new(), |binding| Bindings::new().with(binding))
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

    /// The ordering key every message through this mount site is sent under, which is the one
    /// field of the `googlepubsub` binding this crate can state without a connection.
    ///
    /// The destination the runtime resolved is the topic this mount site publishes to, and the
    /// document already reports it as the channel's address; the channel half of the binding
    /// carries topic configuration alone, so naming the topic here adds nothing and the
    /// parameter goes unread.
    #[cfg(feature = "asyncapi")]
    fn message_bindings(&self, _channel: &str) -> Bindings {
        self.message_binding()
    }
}

/// The version of the `googlepubsub` binding this crate writes.
#[cfg(feature = "asyncapi")]
const BINDING_VERSION: &str = "0.2.0";

/// The message half of the `googlepubsub` binding: what a publish carries beyond its payload and
/// its attributes.
#[cfg(feature = "asyncapi")]
#[derive(Serialize)]
struct PubSubMessageBinding<'a> {
    #[serde(rename = "orderingKey")]
    ordering_key: &'a str,
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use ruststream::HeaderMap;

    use super::*;

    fn keyed_header(value: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(PARTITION_KEY_HEADER, value.to_owned());
        headers
    }

    /// The message's own settings are the native spelling, so they win over both the portable
    /// header and the key the mount site fixed for every message.
    #[test]
    fn the_messages_own_setting_wins_over_the_header_and_the_policy() {
        let options = PubSubPublishOptions {
            ordering_key: Some("step".to_owned()),
        };

        let headers = keyed_header("header");

        let key = resolve_ordering_key(&headers, Some(&options), Some("policy"));
        assert_eq!(key, Ok(Some("step")));
    }

    /// A handler that names no broker still orders its messages: the framework's header is the
    /// portable spelling of the same key, and it is a call site too.
    #[test]
    fn the_partition_key_header_orders_a_publish() {
        let headers = keyed_header("header");

        let key = resolve_ordering_key(&headers, None, Some("policy"));
        assert_eq!(key, Ok(Some("header")));
    }

    /// What no call site named is what the mount site fixed.
    #[test]
    fn a_publish_that_names_nothing_takes_the_policys_key() {
        let headers = HeaderMap::new();

        let key = resolve_ordering_key(&headers, None, Some("policy"));
        assert_eq!(key, Ok(Some("policy")));
    }

    /// Nothing anywhere means an unordered publish, which is Pub/Sub's own default.
    #[test]
    fn a_publish_with_no_key_anywhere_is_unordered() {
        let headers = HeaderMap::new();

        assert_eq!(resolve_ordering_key(&headers, None, None), Ok(None));
    }

    /// An ordering key is text, so a binary `partition-key` header is refused rather than
    /// ordering the message under a rewritten key.
    #[test]
    fn a_partition_key_header_that_is_not_text_is_refused() {
        let mut headers = HeaderMap::new();
        headers.insert(PARTITION_KEY_HEADER, Bytes::from_static(&[0x80, 0xff]));

        let refused = resolve_ordering_key(&headers, None, Some("policy"))
            .expect_err("a non-UTF-8 key has no ordering-key form");
        assert!(refused.contains(PARTITION_KEY_HEADER), "{refused}");
    }
}
