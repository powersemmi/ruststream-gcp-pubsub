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
    Publisher, RedeliveryAddress, Serialized, Subscriber, SubscriptionSource,
};
use ruststream_gcp_pubsub::{
    ConnectedPubSubBroker, GooglePubSub, PARTITION_KEY_HEADER, PubSubBroker, PubSubOrdering,
    PubSubPublish,
};

mod live;

const RECV_TIMEOUT: Duration = Duration::from_secs(15);
const TEST_PROJECT: &str = "ruststream-test";

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

/// The promise a reported redelivery address carries: publish there and the subscription that
/// reported it receives. On Pub/Sub that address is the topic, never the subscription name, and
/// the descriptor asks the API for it when it did not create the binding itself.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_reported_redelivery_address_is_the_subscriptions_topic() {
    let Some(host) = test_host() else { return };
    let connected = connect(&host).await;

    let topic = unique("retry-topic");
    let subscription = unique("retry-subscription");
    let mut subscriber = connected
        .subscribe_descriptor(GooglePubSub::new(&subscription).create_with_topic(&topic))
        .await
        .expect("subscription opens");

    // A descriptor that names no topic has to ask the API, which is the case the runtime hits for
    // a subscription managed as infrastructure.
    let address = GooglePubSub::new(&subscription)
        .redelivery_address(&connected)
        .await
        .expect("the lookup succeeds against a live connection");
    assert_eq!(
        address,
        Some(RedeliveryAddress::new(format!(
            "projects/{TEST_PROJECT}/topics/{topic}"
        )))
    );

    let publisher = connected.publisher();
    publisher
        .publish(
            OutgoingMessage::new(
                address.expect("the address is reported").as_str(),
                b"deferred".as_slice(),
            ),
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
    assert_eq!(message.payload(), b"deferred");
    message.ack().await.expect("ack succeeds");

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
