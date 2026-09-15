//! [`GooglePubSub`]: the subscription descriptor.
//!
//! Pub/Sub separates the topic from the subscription, and the descriptor keeps both explicit:
//! by default it names an existing subscription; `create_with_topic` opts into creating the
//! subscription (and its topic) on subscribe, which is what local development against the
//! emulator wants.

use std::borrow::Cow;
use std::collections::HashMap;
use std::num::NonZeroU32;
use std::ops::RangeInclusive;
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use ruststream::{BrokerMoves, DeclareRetryError, FromName, RetryDeclaration, SubscriptionSource};

use crate::broker::ConnectedPubSubBroker;
use crate::error::PubSubError;
use crate::subscriber::PubSubSubscriber;

/// How long a partial batch waits for more deliveries before it goes out. One streaming-pull
/// burst crosses the network in tens of milliseconds, so a deadline much shorter than this
/// would cut most batches down to the first delivery that arrives.
const DEFAULT_BATCH_WAIT: Duration = Duration::from_millis(50);

/// The range Pub/Sub accepts for a dead-letter policy's `maxDeliveryAttempts`. A cap outside it
/// is refused before the subscription opens, rather than at the API call that would reject it.
const DELIVERY_ATTEMPTS: RangeInclusive<i32> = 5..=100;

/// How long the client keeps extending the ack deadline of a delivery nothing has settled, which
/// is the client's own default and the budget a delayed retry spends. Past it the client stops
/// extending, the lease expires, and the subscription redelivers on its own.
const DEFAULT_MAX_LEASE: Duration = Duration::from_secs(60 * 60);

/// What one delivery may do before it has to be settled, read off the subscription that opened
/// it: how many deliveries the dead-letter policy allows, and how long the client will keep a
/// delivery leased.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DeliveryLimits {
    /// `None` where the registration declared no dead-letter policy, and Pub/Sub then counts
    /// nothing.
    pub(crate) max_delivery_attempts: Option<i32>,
    pub(crate) max_lease: Duration,
}

/// A subscription descriptor for one Pub/Sub subscription.
///
/// Implements [`SubscriptionSource`] for the real broker and for the in-process stand-in behind
/// the `testing` feature, so it sits inline in the `#[subscriber(..)]` decorator and the
/// declaration a service ships is the one its tests mount:
///
/// ```
/// use std::time::Duration;
/// use ruststream_gcp_pubsub::GooglePubSub;
///
/// let source = GooglePubSub::new("orders-workers")
///     .max_outstanding(1_000)
///     .ack_extension(Duration::from_secs(60));
/// # let _ = source;
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub struct GooglePubSub {
    name: String,
    create_with_topic: Option<String>,
    max_outstanding: Option<i64>,
    ack_extension: Option<Duration>,
    batch_wait: Duration,
    /// What the registration declared with `max_attempts(..)`, taken in by `declare_retry`.
    max_attempts: Option<NonZeroU32>,
    /// What it declared with `dead_letter(..)`: the topic spent deliveries are published to.
    dead_letter: Option<String>,
    max_lease: Duration,
}

impl GooglePubSub {
    /// Names an existing subscription (short name or full
    /// `projects/{p}/subscriptions/{s}` resource name).
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            create_with_topic: None,
            max_outstanding: None,
            ack_extension: None,
            batch_wait: DEFAULT_BATCH_WAIT,
            max_attempts: None,
            dead_letter: None,
            max_lease: DEFAULT_MAX_LEASE,
        }
    }

    /// Creates the subscription bound to `topic` on subscribe when it does not exist yet (the
    /// topic is created too). Meant for local development and tests against the emulator;
    /// production subscriptions are usually managed as infrastructure.
    ///
    /// The subscription it creates enables message ordering, so a run of messages sharing an
    /// ordering key reaches the handler in publish order. A subscription that already exists
    /// keeps whatever its own configuration says: ordered delivery is a field of the
    /// subscription resource, and the crate changes no resource it did not create.
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

    /// How long one delivery may stay unsettled before the client stops extending its ack
    /// deadline. Defaults to the client's own hour.
    ///
    /// This is the budget a delayed retry spends: `HandlerOutcome::retry_after(delay)` holds the
    /// delivery in the process for `delay`, and a delay longer than this is refused at the call,
    /// because the subscription would redeliver before it elapsed.
    ///
    /// ```
    /// use std::time::Duration;
    /// use ruststream_gcp_pubsub::GooglePubSub;
    ///
    /// // Handlers here may defer a delivery by up to two hours.
    /// let source = GooglePubSub::new("orders-workers").max_lease(Duration::from_secs(2 * 60 * 60));
    /// # let _ = source;
    /// ```
    pub fn max_lease(mut self, lease: Duration) -> Self {
        self.max_lease = lease;
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
    /// use ruststream_gcp_pubsub::GooglePubSub;
    ///
    /// let source = GooglePubSub::new("orders-workers")
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

    /// The same name, taken out of a descriptor that has served its purpose.
    #[cfg(feature = "testing")]
    pub(crate) fn into_subscription(self) -> String {
        self.name
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

    /// What one delivery of this subscription may do before it has to be settled.
    pub(crate) fn limits(&self) -> DeliveryLimits {
        DeliveryLimits {
            max_delivery_attempts: self.dead_letter_policy().map(|(_, attempts)| attempts),
            max_lease: self.max_lease,
        }
    }

    /// The declared cap in the API's own type. A count past `i32::MAX` saturates onto a value
    /// [`validate`](Self::validate) refuses, so a cap too large to express is rejected rather
    /// than quietly reduced.
    fn declared_attempts(&self) -> Option<i32> {
        self.max_attempts
            .map(|attempts| i32::try_from(attempts.get()).unwrap_or(i32::MAX))
    }

    /// The dead-letter policy this subscription opens with: the topic spent deliveries go to and
    /// how many deliveries one message gets. `None` where the registration declared neither.
    pub(crate) fn dead_letter_policy(&self) -> Option<(&str, i32)> {
        Some((self.dead_letter.as_deref()?, self.declared_attempts()?))
    }

    /// Takes in a registration's cap and dead-letter destination, which become the
    /// subscription's dead-letter policy when it opens. The two `SubscriptionSource` impls and
    /// the by-name ledger below all go through here, so a declaration means the same thing
    /// whichever way the registration named its subscription.
    fn take_declaration(mut self, declaration: &RetryDeclaration) -> Self {
        self.max_attempts = declaration.max_attempts();
        self.dead_letter = declaration.dead_letter().map(ToOwned::to_owned);
        self
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
        self.validate_declaration()
    }

    /// Holds the registration's declaration to what a Pub/Sub dead-letter policy can express.
    ///
    /// A dead-letter policy is one resource field carrying both halves, so a subscription cannot
    /// honour half a declaration: a cap alone would leave a spent delivery circulating, and a
    /// destination alone has no attempt count to fire on. Saying so at startup is the earliest
    /// the crate can - the mount site's declaration reaches the descriptor as data, so nothing
    /// here is a type the compiler could reject.
    fn validate_declaration(&self) -> Result<(), PubSubError> {
        match (self.declared_attempts(), self.dead_letter.as_deref()) {
            (Some(attempts), Some(topic)) => {
                if topic.is_empty() {
                    return Err(PubSubError::InvalidDescriptor(
                        "dead-letter topic must be non-empty".into(),
                    ));
                }
                if !DELIVERY_ATTEMPTS.contains(&attempts) {
                    return Err(PubSubError::InvalidDescriptor(format!(
                        "max_attempts({attempts}) is outside the {}..={} a Pub/Sub dead-letter \
                         policy accepts",
                        DELIVERY_ATTEMPTS.start(),
                        DELIVERY_ATTEMPTS.end(),
                    )));
                }
                Ok(())
            }
            (Some(_), None) => Err(PubSubError::InvalidDescriptor(format!(
                "subscription '{}' declares max_attempts without dead_letter; a Pub/Sub \
                 dead-letter policy needs the topic too",
                self.name,
            ))),
            (None, Some(_)) => Err(PubSubError::InvalidDescriptor(format!(
                "subscription '{}' declares dead_letter without max_attempts; a Pub/Sub \
                 dead-letter policy needs the attempt count too",
                self.name,
            ))),
            (None, None) => Ok(()),
        }
    }
}

/// A subscription is identified by its name and nothing else, every other setting having a
/// default, so the mount site may supply the name instead of the declaration:
/// `#[subscriber(GooglePubSub)]` on the handler and `.name("orders-workers")` where it is mounted.
///
/// # Examples
///
/// ```
/// use ruststream::FromName;
/// use ruststream_gcp_pubsub::GooglePubSub;
///
/// assert_eq!(
///     GooglePubSub::from_name("orders-workers"),
///     GooglePubSub::new("orders-workers"),
/// );
/// ```
impl FromName for GooglePubSub {
    fn from_name(name: impl Into<Cow<'static, str>>) -> Self {
        Self::new(name.into().into_owned())
    }
}

/// A Pub/Sub subscription moves a spent delivery itself, so the copy path is
/// [`BrokerMoves`]: the subscription's dead-letter policy is the mechanism, and nothing is
/// published from the service to retry a message. `.out_retry(..)` over this descriptor is
/// therefore a compile error, and the error names it.
impl SubscriptionSource<ConnectedPubSubBroker> for GooglePubSub {
    type Subscriber = PubSubSubscriber;
    type Copies = BrokerMoves;

    fn name(&self) -> &str {
        self.subscription()
    }

    async fn subscribe(
        self,
        connected: &ConnectedPubSubBroker,
    ) -> Result<PubSubSubscriber, PubSubError> {
        connected.subscribe_descriptor(self).await
    }

    /// Takes in the registration's cap and dead-letter destination, which become the
    /// subscription's dead-letter policy when [`subscribe`](Self::subscribe) opens it: the topic
    /// is the policy's `deadLetterTopic` and the cap its `maxDeliveryAttempts`.
    ///
    /// Only recorded here. The declaration turns into topology where the connection exists, and
    /// a half declaration is refused there rather than opening a subscription that honours
    /// neither half.
    fn declare_retry(self, declaration: &RetryDeclaration) -> Self {
        self.take_declaration(declaration)
    }
}

/// The same descriptor mounts on the in-process stand-in, so a service is unit-tested as it is
/// declared rather than through a bare subscription name.
///
/// The stand-in routes by one address, and that address is the subscription name - the name this
/// source reports, the one the harness injects to and asserts on. What the descriptor says about
/// the service carries over; what it says about the product cannot, because the product is not
/// there:
///
/// * [`batch_wait`](GooglePubSub::batch_wait) is honoured. Batching is on the client either
///   way, over the framework's own buffer, so the deadline means the same thing here.
/// * [`create_with_topic`](GooglePubSub::create_with_topic) is ignored. The stand-in holds
///   no topics and no subscriptions, only addresses, so it has nothing to create and no
///   topic-to-subscription binding to route through. A test therefore publishes to the
///   subscription name, which no producer does against Pub/Sub; that a message published to the
///   *topic* reaches this subscription is the binding's contract, and it is verified against the
///   emulator instead.
/// * [`max_outstanding`](GooglePubSub::max_outstanding) is ignored. It is the streaming
///   pull's flow control, and there is no pull here: the router hands a delivery straight to the
///   subscription's queue.
/// * [`ack_extension`](GooglePubSub::ack_extension) is ignored. Nothing leases a message in
///   process, so nothing expires and nothing needs extending; a handler that outruns its deadline
///   is a live-broker scenario.
/// * The registration's dead-letter policy is honoured. The stand-in counts the deliveries of
///   each message, reports the count the way a Pub/Sub delivery does, and publishes a spent one
///   to the declared topic, so a test drives the cap the service ships.
///
/// # Examples
///
/// ```
/// use ruststream::runtime::{AppInfo, RustStream};
/// use ruststream_gcp_pubsub::prelude::*;
/// use ruststream_gcp_pubsub::testing::PubSubTestBroker;
/// use serde::Deserialize;
///
/// #[derive(Debug, Deserialize)]
/// struct Order {
///     id: u64,
/// }
///
/// #[subscriber(GooglePubSub::new("orders-workers").max_outstanding(1_000))]
/// async fn handle(order: &Order) -> HandlerOutcome {
///     let _ = order.id;
///     HandlerOutcome::ack()
/// }
///
/// // The production declaration, mounted on the stand-in a test starts.
/// let app = RustStream::new(AppInfo::new("orders", "0.1.0"))
///     .with_broker(PubSubTestBroker::new(), |b| {
///         b.include(handle);
///     });
/// # let _ = app;
/// ```
#[cfg(feature = "testing")]
impl SubscriptionSource<crate::testing::ConnectedPubSubTestBroker> for GooglePubSub {
    type Subscriber = crate::testing::PubSubTestSubscriber;
    type Copies = BrokerMoves;

    fn name(&self) -> &str {
        self.subscription()
    }

    async fn subscribe(
        self,
        connected: &crate::testing::ConnectedPubSubTestBroker,
    ) -> Result<Self::Subscriber, PubSubError> {
        connected.subscribe_descriptor(self).await
    }

    /// The same declaration the product takes, so a test drives the cap and the dead-letter
    /// destination it ships: the stand-in counts deliveries per message and publishes a spent
    /// one to the declared topic, which is what the subscription's dead-letter policy does.
    fn declare_retry(self, declaration: &RetryDeclaration) -> Self {
        self.take_declaration(declaration)
    }
}

/// What the bare-name registrations of one connection declared about their retries.
///
/// A bare name carries no descriptor to declare on, so the broker takes the declaration through
/// [`Subscribe::declare_retry`](ruststream::Subscribe::declare_retry) and keeps it here until
/// the subscription of that name opens. Both the real broker and the in-process stand-in hold
/// one, so `#[subscriber("orders-workers")]` gets the dead-letter policy the mount site declared
/// either way.
#[derive(Debug, Default)]
pub(crate) struct DeclaredRetries(Mutex<HashMap<String, RetryDeclaration>>);

impl DeclaredRetries {
    /// Takes what a registration declared for the subscription `name` opens.
    ///
    /// Refuses what a Pub/Sub dead-letter policy cannot express - half a declaration, a cap
    /// outside the range the service accepts - on the same check the descriptor runs, and
    /// refuses a second registration on the same subscription that declares something else: one
    /// subscription carries one policy, and the last writer winning would silently drop what the
    /// other registration asked for.
    pub(crate) fn take(
        &self,
        name: &str,
        declaration: &RetryDeclaration,
    ) -> Result<(), DeclareRetryError> {
        GooglePubSub::new(name)
            .take_declaration(declaration)
            .validate()
            .map_err(refusal)?;
        let conflicts = {
            let mut taken = self.0.lock().unwrap_or_else(PoisonError::into_inner);
            match taken.get(name) {
                Some(already) if already != declaration => true,
                _ => {
                    taken.insert(name.to_owned(), declaration.clone());
                    false
                }
            }
        };
        if conflicts {
            return Err(refusal(PubSubError::InvalidDescriptor(format!(
                "subscription '{name}' is mounted twice with different retry \
                 declarations, and one subscription carries one dead-letter policy",
            ))));
        }
        Ok(())
    }

    /// The descriptor the bare name `name` opens: the subscription it names, carrying what the
    /// registration declared for it.
    pub(crate) fn source(&self, name: &str) -> GooglePubSub {
        let source = GooglePubSub::new(name);
        match self
            .0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(name)
        {
            Some(declaration) => source.take_declaration(declaration),
            None => source,
        }
    }
}

/// A startup refusal, with the crate's own error as its cause: the runtime prints the reason on
/// one line and fails the registration that declared it.
fn refusal(err: PubSubError) -> DeclareRetryError {
    DeclareRetryError::Broker(Box::new(err))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_subscription_name_is_rejected_before_io() {
        assert!(matches!(
            GooglePubSub::new("").validate(),
            Err(PubSubError::InvalidDescriptor(_))
        ));
    }

    #[test]
    fn empty_topic_name_is_rejected_before_io() {
        assert!(matches!(
            GooglePubSub::new("s").create_with_topic("").validate(),
            Err(PubSubError::InvalidDescriptor(_))
        ));
    }

    /// What a registration declaring `attempts` and `destination` states at its mount site.
    fn declaration(attempts: Option<u32>, destination: Option<&str>) -> RetryDeclaration {
        let mut declaration = RetryDeclaration::new();
        if let Some(attempts) = attempts {
            declaration =
                declaration.with_max_attempts(NonZeroU32::new(attempts).expect("non-zero"));
        }
        if let Some(destination) = destination {
            declaration = declaration.with_dead_letter(destination.to_owned());
        }
        declaration
    }

    /// Builds the descriptor a registration declaring `attempts` and `destination` produces.
    fn declared(attempts: Option<u32>, destination: Option<&str>) -> GooglePubSub {
        SubscriptionSource::<ConnectedPubSubBroker>::declare_retry(
            GooglePubSub::new("orders-workers"),
            &declaration(attempts, destination),
        )
    }

    /// Both halves are one resource field, so the descriptor carries them as one policy.
    #[test]
    fn a_full_declaration_becomes_the_subscriptions_dead_letter_policy() {
        let source = declared(Some(5), Some("orders-dead"));
        assert_eq!(source.dead_letter_policy(), Some(("orders-dead", 5)));
    }

    /// A registration that declared nothing leaves the subscription's own topology alone.
    #[test]
    fn an_empty_declaration_leaves_the_subscription_untouched() {
        let source = declared(None, None);
        assert_eq!(source.dead_letter_policy(), None);
        assert!(source.validate().is_ok());
    }

    /// Half a declaration cannot be honoured, and the subscription says so before it opens
    /// rather than running without the cap the registration asked for.
    #[test]
    fn half_a_declaration_is_refused_before_io() {
        for (attempts, destination) in [(Some(5), None), (None, Some("orders-dead"))] {
            let source = declared(attempts, destination);
            assert!(
                matches!(source.validate(), Err(PubSubError::InvalidDescriptor(_))),
                "declaring {attempts:?} / {destination:?} must be refused",
            );
        }
    }

    /// Pub/Sub bounds `maxDeliveryAttempts`, so a cap outside it is named here and not by the
    /// admin call that would reject it.
    #[test]
    fn a_cap_outside_the_services_range_is_refused_before_io() {
        for attempts in [1, 4, 101, 1_000] {
            let source = declared(Some(attempts), Some("orders-dead"));
            assert!(
                matches!(source.validate(), Err(PubSubError::InvalidDescriptor(_))),
                "max_attempts({attempts}) must be refused",
            );
        }
        for attempts in [5, 100] {
            let source = declared(Some(attempts), Some("orders-dead"));
            assert!(
                source.validate().is_ok(),
                "max_attempts({attempts}) must be accepted",
            );
        }
    }

    /// A cap too large for the wire type saturates onto a value the range check refuses, so it
    /// is rejected rather than quietly reduced to something the service accepts.
    #[test]
    fn a_cap_past_the_wire_type_is_refused_rather_than_reduced() {
        let source = declared(Some(u32::MAX), Some("orders-dead"));
        assert_eq!(source.declared_attempts(), Some(i32::MAX));
        assert!(matches!(
            source.validate(),
            Err(PubSubError::InvalidDescriptor(_))
        ));
    }

    /// A dead-letter topic is a resource name, so an empty one is refused with the rest.
    #[test]
    fn an_empty_dead_letter_topic_is_refused_before_io() {
        let source = declared(Some(5), Some(""));
        assert!(matches!(
            source.validate(),
            Err(PubSubError::InvalidDescriptor(_))
        ));
    }

    /// A bare name has no descriptor to hold the declaration, so the broker's ledger holds it
    /// and the subscription that name opens carries the policy.
    #[test]
    fn a_declaration_for_a_bare_name_reaches_the_subscription_it_opens() {
        let taken = DeclaredRetries::default();

        taken
            .take("orders-workers", &declaration(Some(5), Some("orders-dead")))
            .expect("a full declaration is one a dead-letter policy carries");

        assert_eq!(
            taken.source("orders-workers").dead_letter_policy(),
            Some(("orders-dead", 5)),
        );
    }

    /// A name nothing declared for opens the subscription as it stands.
    #[test]
    fn a_bare_name_with_no_declaration_opens_the_subscription_untouched() {
        let taken = DeclaredRetries::default();

        taken
            .take("orders-workers", &RetryDeclaration::new())
            .expect("a registration may declare nothing");

        assert_eq!(
            taken.source("orders-workers"),
            GooglePubSub::new("orders-workers")
        );
    }

    /// The ledger holds a bare name to the same policy the descriptor is held to, and the
    /// refusal carries the crate's own reason so the startup line says what to fix.
    #[test]
    fn a_bare_name_is_refused_what_a_dead_letter_policy_cannot_express() {
        let taken = DeclaredRetries::default();

        for (attempts, destination) in [
            (Some(5), None),
            (None, Some("orders-dead")),
            (Some(3), Some("orders-dead")),
        ] {
            let err = taken
                .take("orders-workers", &declaration(attempts, destination))
                .expect_err("the declaration must be refused");
            assert!(
                matches!(err, DeclareRetryError::Broker(_)),
                "declaring {attempts:?} / {destination:?} must refuse as a broker rejection",
            );
        }
    }

    /// One subscription carries one dead-letter policy, so a second registration declaring
    /// something else for the same name is refused rather than silently overwriting the first.
    #[test]
    fn one_subscription_declared_twice_differently_is_refused() {
        let taken = DeclaredRetries::default();
        taken
            .take("orders-workers", &declaration(Some(5), Some("orders-dead")))
            .expect("the first declaration is taken");

        let err = taken
            .take(
                "orders-workers",
                &declaration(Some(10), Some("orders-dead")),
            )
            .expect_err("a contradicting declaration must be refused");

        assert!(matches!(err, DeclareRetryError::Broker(_)));
        assert_eq!(
            taken.source("orders-workers").dead_letter_policy(),
            Some(("orders-dead", 5)),
            "the refusal leaves the declaration already taken alone",
        );
    }

    /// Two registrations that declare the same thing are no contradiction: the subscription
    /// opens with the policy both asked for.
    #[test]
    fn one_subscription_declared_twice_alike_is_taken_once() {
        let taken = DeclaredRetries::default();
        let declared = declaration(Some(5), Some("orders-dead"));

        taken.take("orders-workers", &declared).expect("the first");
        taken.take("orders-workers", &declared).expect("the second");

        assert_eq!(
            taken.source("orders-workers").dead_letter_policy(),
            Some(("orders-dead", 5)),
        );
    }
}
