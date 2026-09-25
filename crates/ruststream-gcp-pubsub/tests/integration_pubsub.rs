//! End-to-end checks against the Pub/Sub emulator, gated behind `PUBSUB_TEST_HOST`.
//!
//! Start it with `just brokers-up`, then:
//! `PUBSUB_TEST_HOST=127.0.0.1:8085 cargo test --all-features -- --test-threads=1`.

use std::pin::pin;
use std::thread;
use std::time::Duration;

use futures::StreamExt;
use ruststream::runtime::PublishExt;
use ruststream::{
    AckError, Broker, ConnectedBroker, HeaderMap, IncomingMessage, Outgoing, OutgoingMessage,
    PublishPolicy, Publisher, RetryDeclaration, Serialized, Subscribe, Subscriber,
    SubscriptionSource, nonzero,
};
use ruststream_gcp_pubsub::{
    ConnectedPubSubBroker, DELIVERY_ATTEMPT_HEADER, GooglePubSub, PARTITION_KEY_HEADER,
    PubSubBroker, PubSubError, PubSubOrdering, PubSubPublish, PubSubSubscriber,
};
use tokio::runtime;
use tokio::sync::oneshot;

mod live;

const RECV_TIMEOUT: Duration = Duration::from_secs(15);
const TEST_PROJECT: &str = "ruststream-test";

/// The cap the dead-letter case declares. Pub/Sub accepts 5..=100 for `maxDeliveryAttempts`, so
/// five is the shortest run that exercises the policy.
const MAX_ATTEMPTS: u32 = 5;

/// Bytes travelling as themselves. The subject here is the ordering key a built publish carries,
/// so the payload stays exactly what the assertion reads back off the delivery.
#[derive(Outgoing, Serialized)]
struct Wire(Vec<u8>);

fn test_host() -> Option<String> {
    live::host("PUBSUB_TEST_HOST")
}

async fn connect(host: &str) -> ConnectedPubSubBroker {
    PubSubBroker::new(TEST_PROJECT)
        .emulator(host)
        .connect()
        .await
        .expect("broker connects")
}

/// Per-test unique name, so runs do not observe each other's leftovers.
fn unique(name: &str) -> String {
    format!("it-{name}-{}", std::process::id())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn roundtrip_preserves_payload_attributes_and_partition_key() {
    let Some(host) = test_host() else { return };
    let connected = connect(&host).await;

    let name = unique("roundtrip");
    let mut subscriber = connected
        .subscribe_descriptor(GooglePubSub::new(&name).create_with_topic(&name))
        .await
        .expect("subscription opens");

    let mut headers = HeaderMap::new();
    headers.insert("content-type", "application/json");
    headers.insert("x-tenant", "acme");
    headers.insert(PARTITION_KEY_HEADER, "user-42");
    let publisher = connected.publisher();
    publisher
        .publish(
            OutgoingMessage::new(&name, b"{\"id\":1}".as_slice()).with_headers(headers),
            None,
        )
        .await
        .expect("publish succeeds");

    let mut stream = pin!(subscriber.stream());
    let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");

    assert_eq!(message.payload(), b"{\"id\":1}");
    assert_eq!(
        message.headers().get_str("content-type"),
        Some("application/json")
    );
    assert_eq!(message.headers().get_str("x-tenant"), Some("acme"));
    assert_eq!(message.partition_key(), Some(b"user-42".as_slice()));
    message.ack().await.expect("ack succeeds");

    connected.shutdown().await.expect("shutdown succeeds");
}

/// The portable spelling reaches the product's own field: a service that names no broker writes
/// the framework's `partition-key` header, and the message leaves under that ordering key. The
/// header itself never becomes an attribute, so the key travels once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_partition_key_header_orders_a_publish() {
    let Some(host) = test_host() else { return };
    let connected = connect(&host).await;

    let name = unique("header-key");
    let mut subscriber = connected
        .subscribe_descriptor(GooglePubSub::new(&name).create_with_topic(&name))
        .await
        .expect("subscription opens");

    let mut headers = HeaderMap::new();
    headers.insert(PARTITION_KEY_HEADER, "user-42");
    let publisher = connected.publisher();
    publisher
        .publish(
            OutgoingMessage::new(&name, b"by-header".as_slice()).with_headers(headers),
            None,
        )
        .await
        .expect("publish succeeds");

    let mut stream = pin!(subscriber.stream());
    let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");

    assert_eq!(message.payload(), b"by-header");
    assert_eq!(message.partition_key(), Some(b"user-42".as_slice()));
    message.ack().await.expect("ack succeeds");

    connected.shutdown().await.expect("shutdown succeeds");
}

/// The step reaches the product's own field: what the call named comes back as the delivery's
/// partition key, and the mount site's default is what a publish naming nothing is sent under.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_ordering_step_and_the_policy_default_reach_the_product() {
    let Some(host) = test_host() else { return };
    let connected = connect(&host).await;

    let name = unique("ordering-step");
    let mut subscriber = connected
        .subscribe_descriptor(GooglePubSub::new(&name).create_with_topic(&name))
        .await
        .expect("subscription opens");

    let publisher = PubSubPublish::default()
        .ordering_key("user-1")
        .pair(&connected)
        .await
        .expect("the policy pairs with the connected broker");
    publisher
        .message(&Wire(b"by-policy".to_vec()))
        .to(name.as_str())
        .publish()
        .await
        .expect("publish succeeds");
    publisher
        .message(&Wire(b"by-step".to_vec()))
        .to(name.as_str())
        .ordering_key("user-7")
        .publish()
        .await
        .expect("publish succeeds");

    let mut stream = pin!(subscriber.stream());
    for (payload, key) in [(b"by-policy".as_slice(), "user-1"), (b"by-step", "user-7")] {
        let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
            .await
            .expect("delivery arrives")
            .expect("stream is open")
            .expect("delivery is ok");

        assert_eq!(message.payload(), payload);
        assert_eq!(message.partition_key(), Some(key.as_bytes()));
        message.ack().await.expect("ack succeeds");
    }

    connected.shutdown().await.expect("shutdown succeeds");
}

/// Runs one message past the declared cap on `workers` and returns what reached `dead_letter`.
///
/// The declared number of deliveries is spent one by one, each checked for the count the service
/// reports, and the last is settled the way the runtime settles a delivery whose attempts are
/// gone.
async fn dead_letters_a_spent_delivery(
    connected: &ConnectedPubSubBroker,
    topic: &str,
    workers: GooglePubSub,
    dead_letter: &str,
) {
    let subscriber = SubscriptionSource::<ConnectedPubSubBroker>::declare_retry(
        workers,
        &declared_retries(dead_letter),
    )
    .subscribe(connected)
    .await
    .expect("the subscription opens with the declared dead-letter policy");
    // The declaration created the dead-letter topic, so a subscription on it sees what lands
    // there.
    let dead = watch_dead_letter(connected, dead_letter).await;

    spends_its_attempts(connected, topic, subscriber, dead).await;
}

/// What a mount site declares in every dead-letter case here.
fn declared_retries(dead_letter: &str) -> RetryDeclaration {
    RetryDeclaration::new()
        .with_max_attempts(nonzero!(MAX_ATTEMPTS))
        .with_dead_letter(dead_letter.to_owned())
}

/// Opens a subscription on the dead-letter topic, creating the topic where it is not there yet:
/// the policy names a topic the service refuses to write a policy for otherwise.
async fn watch_dead_letter(
    connected: &ConnectedPubSubBroker,
    dead_letter: &str,
) -> PubSubSubscriber {
    connected
        .subscribe_descriptor(
            GooglePubSub::new(format!("{dead_letter}-watcher")).create_with_topic(dead_letter),
        )
        .await
        .expect("the dead-letter subscription opens")
}

/// Publishes one message to `topic` and spends every delivery the policy allows, then reads the
/// copy the service carried to the dead-letter topic.
async fn spends_its_attempts(
    connected: &ConnectedPubSubBroker,
    topic: &str,
    mut subscriber: PubSubSubscriber,
    mut dead: PubSubSubscriber,
) {
    connected
        .publisher()
        .publish(OutgoingMessage::new(topic, b"poison".as_slice()), None)
        .await
        .expect("publish succeeds");

    let mut stream = pin!(subscriber.stream());
    for expected in 1..=MAX_ATTEMPTS {
        let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
            .await
            .expect("delivery arrives")
            .expect("stream is open")
            .expect("delivery is ok");
        // The service counts the deliveries, so the count is on the message rather than in a
        // header this process maintains. The crate's own header reports the same number, which
        // is what a service reads when it stays on headers.
        assert_eq!(message.redelivery_count(), Some(u64::from(expected)));
        assert_eq!(
            message.headers().get_str(DELIVERY_ATTEMPT_HEADER),
            Some(expected.to_string().as_str()),
        );
        // What the runtime settles an immediate retry with on this transport, at every
        // delivery: the subscription moves a spent one itself, so nothing in the process reads
        // the cap and asks for the last delivery to be rejected instead.
        message.nack(true).await.expect("nack succeeds");
    }

    let mut dead_stream = pin!(dead.stream());
    let carried = tokio::time::timeout(RECV_TIMEOUT, dead_stream.next())
        .await
        .expect("the spent delivery reaches the dead-letter topic")
        .expect("stream is open")
        .expect("delivery is ok");
    assert_eq!(carried.payload(), b"poison");
    carried.ack().await.expect("ack succeeds");
}

/// What the mount site declares becomes the subscription's own dead-letter policy: the service
/// gives one message the declared number of deliveries and then publishes it to the declared
/// topic. Nothing in this process moves it, which is the whole of what `BrokerMoves` means.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_created_subscription_opens_with_the_declared_dead_letter_policy() {
    let Some(host) = test_host() else { return };
    let connected = connect(&host).await;

    let topic = unique("dlq-topic");
    let workers = unique("dlq-workers");
    let dead_letter = unique("dlq-dead");

    dead_letters_a_spent_delivery(
        &connected,
        &topic,
        GooglePubSub::new(&workers).create_with_topic(&topic),
        &dead_letter,
    )
    .await;

    connected.shutdown().await.expect("shutdown succeeds");
}

/// A subscription managed as infrastructure is already there when the service starts, so the
/// declaration reaches it as an update instead of riding the create. The message it moves is the
/// same one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_existing_subscription_takes_the_declaration_as_an_update() {
    let Some(host) = test_host() else { return };
    let connected = connect(&host).await;

    let topic = unique("update-topic");
    let workers = unique("update-workers");
    let dead_letter = unique("update-dead");

    // The subscription exists before anything is declared, which is the production shape.
    connected
        .subscribe_descriptor(GooglePubSub::new(&workers).create_with_topic(&topic))
        .await
        .expect("the subscription is created without a policy");

    dead_letters_a_spent_delivery(
        &connected,
        &topic,
        GooglePubSub::new(&workers).create_with_topic(&topic),
        &dead_letter,
    )
    .await;

    connected.shutdown().await.expect("shutdown succeeds");
}

/// A handler that names its subscription with a plain string declares its retries at the mount
/// site like any other, and the broker maps the declaration onto the subscription that name
/// opens. A bare name creates no topology, so the subscription and the dead-letter topic are
/// there before the service starts, which is the shape a bare name is used in.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bare_name_opens_with_the_declared_dead_letter_policy() {
    let Some(host) = test_host() else { return };
    let connected = connect(&host).await;

    let topic = unique("by-name-topic");
    let workers = unique("by-name-workers");
    let dead_letter = unique("by-name-dead");

    connected
        .subscribe_descriptor(GooglePubSub::new(&workers).create_with_topic(&topic))
        .await
        .expect("the subscription exists before the service starts");
    let dead = watch_dead_letter(&connected, &dead_letter).await;

    // What the runtime does for a registration mounted by a bare name: the broker takes the
    // declaration, then opens the subscription that name identifies.
    connected
        .declare_retry(&workers, &declared_retries(&dead_letter))
        .expect("the broker maps the declaration onto the subscription");
    let subscriber = connected
        .subscribe(&workers)
        .await
        .expect("the subscription opens with the declared dead-letter policy");

    spends_its_attempts(&connected, &topic, subscriber, dead).await;

    connected.shutdown().await.expect("shutdown succeeds");
}

/// The delay a live deferred delivery waits out, and the window a test looks in before it.
const LIVE_RETRY_DELAY: Duration = Duration::from_secs(6);
const BEFORE_THE_DELAY: Duration = Duration::from_secs(2);

/// Pub/Sub has no delayed nack, so the crate holds the delivery and rejects it when the delay is
/// out. The service keeps the delivery leased while it is held, so it comes back once - after the
/// delay, not before it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_delayed_nack_redelivers_after_the_delay() {
    let Some(host) = test_host() else { return };
    let connected = connect(&host).await;

    let name = unique("delayed-nack");
    let mut subscriber = connected
        .subscribe_descriptor(GooglePubSub::new(&name).create_with_topic(&name))
        .await
        .expect("subscription opens");
    connected
        .publisher()
        .publish(OutgoingMessage::new(&name, b"later".as_slice()), None)
        .await
        .expect("publish succeeds");

    let mut stream = pin!(subscriber.stream());
    let first = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");
    assert!(
        first.supports_nack_after(),
        "the crate carries the delay itself, so it must say so",
    );
    first
        .nack_after(LIVE_RETRY_DELAY)
        .await
        .expect("the delivery is held");

    // Not before: the lease is being extended, so the service hands it to nobody meanwhile.
    assert!(
        tokio::time::timeout(BEFORE_THE_DELAY, stream.next())
            .await
            .is_err(),
        "a held delivery must not come back before its delay is out",
    );

    // And after: the rejection at the end of the delay is what brings it back.
    let again = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("the held delivery comes back")
        .expect("stream is open")
        .expect("delivery is ok");
    assert_eq!(again.payload(), b"later");
    again.ack().await.expect("ack succeeds");

    connected.shutdown().await.expect("shutdown succeeds");
}

/// A handler on a dedicated thread settles from that thread's own runtime, which may stop before
/// the delay is out. The hold runs on the runtime the broker connected on, so the delivery still
/// comes back after the delay, not at once when the settling runtime goes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_delayed_nack_from_a_stopped_runtime_still_waits_out_the_delay() {
    let Some(host) = test_host() else { return };
    let connected = connect(&host).await;

    let name = unique("delayed-nack-foreign");
    let mut subscriber = connected
        .subscribe_descriptor(GooglePubSub::new(&name).create_with_topic(&name))
        .await
        .expect("subscription opens");
    connected
        .publisher()
        .publish(OutgoingMessage::new(&name, b"later".as_slice()), None)
        .await
        .expect("publish succeeds");

    let mut stream = pin!(subscriber.stream());
    let first = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");
    on_foreign_runtime(async move || {
        first
            .nack_after(LIVE_RETRY_DELAY)
            .await
            .expect("the delivery is held");
    })
    .await;

    assert!(
        tokio::time::timeout(BEFORE_THE_DELAY, stream.next())
            .await
            .is_err(),
        "a delivery held from a runtime that stopped must not come back before its delay is out",
    );
    let again = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("the held delivery comes back")
        .expect("stream is open")
        .expect("delivery is ok");
    assert_eq!(again.payload(), b"later");
    again.ack().await.expect("ack succeeds");

    connected.shutdown().await.expect("shutdown succeeds");
}

/// Runs `work` on a single-threaded runtime of its own thread, stopped as soon as `work` returns,
/// the way a handler on a dedicated thread settles a delivery.
async fn on_foreign_runtime<Output: Send + 'static>(
    work: impl AsyncFnOnce() -> Output + Send + 'static,
) -> Output {
    let (done, finished) = oneshot::channel();
    thread::spawn(move || {
        let runtime = runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a current-thread runtime builds");
        let output = runtime.block_on(work());
        drop(runtime);
        let _ = done.send(output);
    });
    finished
        .await
        .expect("the foreign runtime's work completes")
}

/// A delay the subscription cannot outlast is refused at the call, because the delivery would
/// come back before it elapsed and the handler would never learn.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_delay_past_the_lease_is_refused() {
    let Some(host) = test_host() else { return };
    let connected = connect(&host).await;

    let name = unique("short-lease");
    let mut subscriber = connected
        .subscribe_descriptor(
            GooglePubSub::new(&name)
                .create_with_topic(&name)
                .max_lease(Duration::from_secs(5)),
        )
        .await
        .expect("subscription opens");
    connected
        .publisher()
        .publish(OutgoingMessage::new(&name, b"too-long".as_slice()), None)
        .await
        .expect("publish succeeds");

    let mut stream = pin!(subscriber.stream());
    let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");

    let err = message
        .nack_after(Duration::from_secs(30))
        .await
        .expect_err("a delay past the lease must be refused");
    assert!(
        matches!(
            err,
            AckError::Broker(source)
                if source.to_string().contains("outlives the subscription's maximum lease"),
        ),
        "the error must name the limit it refused against",
    );

    connected.shutdown().await.expect("shutdown succeeds");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nack_with_requeue_redelivers() {
    let Some(host) = test_host() else { return };
    let connected = connect(&host).await;

    let name = unique("requeue");
    let mut subscriber = connected
        .subscribe_descriptor(GooglePubSub::new(&name).create_with_topic(&name))
        .await
        .expect("subscription opens");
    let publisher = connected.publisher();
    publisher
        .publish(OutgoingMessage::new(&name, b"again".as_slice()), None)
        .await
        .expect("publish succeeds");

    let mut stream = pin!(subscriber.stream());
    let first = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");
    first.nack(true).await.expect("nack succeeds");

    let second = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("redelivery arrives")
        .expect("stream is open")
        .expect("redelivery is ok");
    assert_eq!(second.payload(), b"again");
    second.ack().await.expect("ack succeeds");

    connected.shutdown().await.expect("shutdown succeeds");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nack_without_requeue_does_not_redeliver() {
    let Some(host) = test_host() else { return };
    let connected = connect(&host).await;

    let name = unique("drop");
    let mut subscriber = connected
        .subscribe_descriptor(GooglePubSub::new(&name).create_with_topic(&name))
        .await
        .expect("subscription opens");
    let publisher = connected.publisher();
    publisher
        .publish(OutgoingMessage::new(&name, b"poison".as_slice()), None)
        .await
        .expect("publish succeeds");

    let mut stream = pin!(subscriber.stream());
    let poison = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");
    poison.nack(false).await.expect("drop succeeds");

    // The follow-up message must be the next delivery; the dropped one must not come back.
    publisher
        .publish(OutgoingMessage::new(&name, b"next".as_slice()), None)
        .await
        .expect("publish succeeds");
    let next = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");
    assert_eq!(next.payload(), b"next");
    next.ack().await.expect("ack succeeds");

    connected.shutdown().await.expect("shutdown succeeds");
}

/// A subscription that declares no dead-letter policy counts nothing, so a delivery through it
/// reports no attempt at all. It is the service that counts, and a subscription with no cap to
/// count against does not.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_subscription_without_a_policy_reports_no_attempt() {
    let Some(host) = test_host() else { return };
    let connected = connect(&host).await;

    let name = unique("no-policy");
    let mut subscriber = connected
        .subscribe_descriptor(GooglePubSub::new(&name).create_with_topic(&name))
        .await
        .expect("subscription opens");
    connected
        .publisher()
        .publish(OutgoingMessage::new(&name, b"once".as_slice()), None)
        .await
        .expect("publish succeeds");

    let mut stream = pin!(subscriber.stream());
    let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");

    assert_eq!(message.redelivery_count(), None);
    assert_eq!(message.headers().get_str(DELIVERY_ATTEMPT_HEADER), None);
    message.ack().await.expect("ack succeeds");

    connected.shutdown().await.expect("shutdown succeeds");
}

/// A publish that names no key anywhere is unordered, which is Pub/Sub's own default: the
/// delivery carries no partition key, and the portable header the crate reads a key back under
/// is not invented for it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_publish_that_names_no_key_is_unordered() {
    let Some(host) = test_host() else { return };
    let connected = connect(&host).await;

    let name = unique("unkeyed");
    let mut subscriber = connected
        .subscribe_descriptor(GooglePubSub::new(&name).create_with_topic(&name))
        .await
        .expect("subscription opens");
    connected
        .publisher()
        .publish(OutgoingMessage::new(&name, b"plain".as_slice()), None)
        .await
        .expect("publish succeeds");

    let mut stream = pin!(subscriber.stream());
    let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");

    assert_eq!(message.partition_key(), None);
    assert_eq!(message.headers().get_str(PARTITION_KEY_HEADER), None);
    message.ack().await.expect("ack succeeds");

    connected.shutdown().await.expect("shutdown succeeds");
}

/// A publish to a topic the project does not have fails with the topic in the error, rather than
/// succeeding against nothing. The client creates its per-topic handle without a round trip, so
/// this is the first moment the service gets a say.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_publish_to_a_topic_that_is_not_there_names_the_topic() {
    let Some(host) = test_host() else { return };
    let connected = connect(&host).await;

    let name = unique("no-topic");
    let err = connected
        .publisher()
        .publish(OutgoingMessage::new(&name, b"nowhere".as_slice()), None)
        .await
        .expect_err("a publish to a topic that is not there must fail");

    assert!(
        matches!(err, PubSubError::Publish { ref topic, .. } if topic.ends_with(&name)),
        "the error must name the topic it could not reach: {err}",
    );

    connected.shutdown().await.expect("shutdown succeeds");
}

/// A descriptor that names a subscription and creates nothing takes the name on trust, because
/// the subscription is infrastructure. Where the name answers to nothing the streaming pull is
/// what says so, and the error carries the subscription rather than a bare client failure.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_subscription_that_is_not_there_reports_itself_on_the_stream() {
    let Some(host) = test_host() else { return };
    let connected = connect(&host).await;

    let name = unique("no-subscription");
    let mut subscriber = connected
        .subscribe_descriptor(GooglePubSub::new(&name))
        .await
        .expect("nothing is created, so nothing fails yet");

    let mut stream = pin!(subscriber.stream());
    let err = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("the stream reports the missing subscription")
        .expect("stream is open")
        .expect_err("a subscription that is not there cannot deliver");

    assert!(
        matches!(err, PubSubError::Receive { ref subscription, .. } if subscription.ends_with(&name)),
        "the error must name the subscription it was pulling from: {err}",
    );

    connected.shutdown().await.expect("shutdown succeeds");
}

/// The last delivery the policy allows is the one place a rejection means "do not redeliver":
/// the service carries the message to the dead-letter topic instead. So a handler that drops a
/// delivery there has it kept rather than acknowledged away.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_delivery_dropped_at_the_cap_reaches_the_dead_letter_topic() {
    let Some(host) = test_host() else { return };
    let connected = connect(&host).await;

    let topic = unique("drop-cap-topic");
    let workers = unique("drop-cap-workers");
    let dead_letter = unique("drop-cap-dead");

    let mut subscriber = SubscriptionSource::<ConnectedPubSubBroker>::declare_retry(
        GooglePubSub::new(&workers).create_with_topic(&topic),
        &declared_retries(&dead_letter),
    )
    .subscribe(&connected)
    .await
    .expect("the subscription opens with the declared dead-letter policy");
    let mut dead = watch_dead_letter(&connected, &dead_letter).await;

    connected
        .publisher()
        .publish(OutgoingMessage::new(&topic, b"poison".as_slice()), None)
        .await
        .expect("publish succeeds");

    let mut stream = pin!(subscriber.stream());
    for _ in 1..MAX_ATTEMPTS {
        let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
            .await
            .expect("delivery arrives")
            .expect("stream is open")
            .expect("delivery is ok");
        message.nack(true).await.expect("nack succeeds");
    }

    let last = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("the last delivery the policy allows arrives")
        .expect("stream is open")
        .expect("delivery is ok");
    assert_eq!(last.redelivery_count(), Some(u64::from(MAX_ATTEMPTS)));
    last.nack(false).await.expect("dropping succeeds");

    let mut dead_stream = pin!(dead.stream());
    let carried = tokio::time::timeout(RECV_TIMEOUT, dead_stream.next())
        .await
        .expect("the dropped delivery reaches the dead-letter topic")
        .expect("stream is open")
        .expect("delivery is ok");
    assert_eq!(carried.payload(), b"poison");
    carried.ack().await.expect("ack succeeds");

    connected.shutdown().await.expect("shutdown succeeds");
}

/// Before the cap, dropping a delivery is an acknowledgement: Pub/Sub has no verb for "gone but
/// not delivered", and the dead-letter topic is where a message goes when its deliveries run
/// out, not when a handler declines one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_delivery_dropped_before_the_cap_is_acknowledged() {
    let Some(host) = test_host() else { return };
    let connected = connect(&host).await;

    let topic = unique("drop-early-topic");
    let workers = unique("drop-early-workers");
    let dead_letter = unique("drop-early-dead");

    let mut subscriber = SubscriptionSource::<ConnectedPubSubBroker>::declare_retry(
        GooglePubSub::new(&workers).create_with_topic(&topic),
        &declared_retries(&dead_letter),
    )
    .subscribe(&connected)
    .await
    .expect("the subscription opens with the declared dead-letter policy");
    let mut dead = watch_dead_letter(&connected, &dead_letter).await;

    let publisher = connected.publisher();
    publisher
        .publish(OutgoingMessage::new(&topic, b"declined".as_slice()), None)
        .await
        .expect("publish succeeds");

    let mut stream = pin!(subscriber.stream());
    let first = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");
    assert_eq!(first.redelivery_count(), Some(1));
    first.nack(false).await.expect("dropping succeeds");

    // The next message through the same subscription is the fence: once it arrives, the dropped
    // one has been settled and was not handed back.
    publisher
        .publish(OutgoingMessage::new(&topic, b"next".as_slice()), None)
        .await
        .expect("publish succeeds");
    let next = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");
    assert_eq!(next.payload(), b"next");
    next.ack().await.expect("ack succeeds");

    let mut dead_stream = pin!(dead.stream());
    assert!(
        tokio::time::timeout(Duration::from_secs(2), dead_stream.next())
            .await
            .is_err(),
        "a delivery the handler declined before the cap must not reach the dead-letter topic",
    );

    connected.shutdown().await.expect("shutdown succeeds");
}
