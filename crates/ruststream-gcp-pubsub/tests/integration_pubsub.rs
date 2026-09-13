//! End-to-end checks against the Pub/Sub emulator, gated behind `PUBSUB_TEST_HOST`.
//!
//! Start it with `just brokers-up`, then:
//! `PUBSUB_TEST_HOST=127.0.0.1:8085 cargo test --all-features -- --test-threads=1`.

use std::pin::pin;
use std::time::Duration;

use futures::StreamExt;
use ruststream::runtime::PublishExt;
use ruststream::{
    Broker, ConnectedBroker, HeaderMap, IncomingMessage, Outgoing, OutgoingMessage, PublishPolicy,
    Publisher, RetryDeclaration, Serialized, Subscriber, SubscriptionSource, nonzero,
};
use ruststream_gcp_pubsub::{
    ConnectedPubSubBroker, GooglePubSub, PARTITION_KEY_HEADER, PubSubBroker, PubSubOrdering,
    PubSubPublish,
};

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

/// What the mount site declares becomes the subscription's own dead-letter policy: the service
/// gives one message the declared number of deliveries and then publishes it to the declared
/// topic. Nothing in this process moves it, which is the whole of what `BrokerMoves` means.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_declaration_becomes_the_subscriptions_dead_letter_policy() {
    let Some(host) = test_host() else { return };
    let connected = connect(&host).await;

    let topic = unique("dlq-topic");
    let workers = unique("dlq-workers");
    let dead_letter = unique("dlq-dead");
    let watcher = unique("dlq-watcher");

    let declaration = RetryDeclaration::new()
        .with_max_attempts(nonzero!(MAX_ATTEMPTS))
        .with_dead_letter(dead_letter.clone());
    let source = SubscriptionSource::<ConnectedPubSubBroker>::declare_retry(
        GooglePubSub::new(&workers).create_with_topic(&topic),
        &declaration,
    );
    let mut subscriber = source
        .subscribe(&connected)
        .await
        .expect("the subscription opens with the declared dead-letter policy");
    // The declaration created the dead-letter topic, so a subscription on it sees what lands
    // there.
    let mut dead = connected
        .subscribe_descriptor(GooglePubSub::new(&watcher).create_with_topic(&dead_letter))
        .await
        .expect("the dead-letter subscription opens");

    connected
        .publisher()
        .publish(OutgoingMessage::new(&topic, b"poison".as_slice()), None)
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
        // header this process maintains.
        assert_eq!(message.redelivery_count(), Some(u64::from(expected)));
        // The settlement the runtime sends once the attempts are spent. Below the cap it asks
        // for another delivery; at the cap it says this delivery is the last, and on this
        // transport both are the same rejection.
        let requeue = expected < MAX_ATTEMPTS;
        message.nack(requeue).await.expect("nack succeeds");
    }

    let mut dead_stream = pin!(dead.stream());
    let carried = tokio::time::timeout(RECV_TIMEOUT, dead_stream.next())
        .await
        .expect("the spent delivery reaches the dead-letter topic")
        .expect("stream is open")
        .expect("delivery is ok");
    assert_eq!(carried.payload(), b"poison");
    carried.ack().await.expect("ack succeeds");

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
