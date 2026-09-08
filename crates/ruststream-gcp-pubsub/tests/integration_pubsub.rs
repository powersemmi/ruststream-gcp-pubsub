//! End-to-end checks against the Pub/Sub emulator, gated behind `PUBSUB_TEST_HOST`.
//!
//! Start it with `just brokers-up`, then:
//! `PUBSUB_TEST_HOST=127.0.0.1:8085 cargo test --all-features -- --test-threads=1`.

use std::pin::pin;
use std::time::Duration;

use futures::StreamExt;
use ruststream::runtime::PublishExt;
use ruststream::{
    Broker, ConnectedBroker, HeaderMap, IncomingMessage, Outgoing, OutgoingMessage, Publisher,
    Serialized, Subscriber,
};
use ruststream_gcp_pubsub::{
    ConnectedPubSubBroker, PARTITION_KEY_HEADER, PubSubBroker, PubSubOrdering, PubSubSubscription,
};

const RECV_TIMEOUT: Duration = Duration::from_secs(15);
const TEST_PROJECT: &str = "ruststream-test";

/// Bytes travelling as themselves. The subject here is the ordering key a built publish carries,
/// so the payload stays exactly what the assertion reads back off the delivery.
#[derive(Outgoing, Serialized)]
struct Wire(Vec<u8>);

fn test_host() -> Option<String> {
    match std::env::var("PUBSUB_TEST_HOST") {
        Ok(host) if !host.is_empty() => Some(host),
        _ => {
            eprintln!("PUBSUB_TEST_HOST is not set; skipping the emulator integration test");
            None
        }
    }
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
        .subscribe_descriptor(PubSubSubscription::new(&name).create_with_topic(&name))
        .await
        .expect("subscription opens");

    let mut headers = HeaderMap::new();
    headers.insert("content-type", "application/json");
    headers.insert("x-tenant", "acme");
    headers.insert(PARTITION_KEY_HEADER, "user-42");
    let publisher = connected.publisher();
    publisher
        .publish(OutgoingMessage::new(&name, b"{\"id\":1}".as_slice()).with_headers(headers))
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_ordering_step_sets_the_key_of_a_built_publish() {
    let Some(host) = test_host() else { return };
    let connected = connect(&host).await;

    let name = unique("ordering-step");
    let mut subscriber = connected
        .subscribe_descriptor(PubSubSubscription::new(&name).create_with_topic(&name))
        .await
        .expect("subscription opens");

    let publisher = connected.publisher();
    publisher
        .with_ordering_key("user-7")
        .message(&Wire(b"ordered".to_vec()))
        .to(name.as_str())
        .publish()
        .await
        .expect("publish succeeds");

    let mut stream = pin!(subscriber.stream());
    let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");

    assert_eq!(message.payload(), b"ordered");
    assert_eq!(message.partition_key(), Some(b"user-7".as_slice()));
    message.ack().await.expect("ack succeeds");

    connected.shutdown().await.expect("shutdown succeeds");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nack_with_requeue_redelivers() {
    let Some(host) = test_host() else { return };
    let connected = connect(&host).await;

    let name = unique("requeue");
    let mut subscriber = connected
        .subscribe_descriptor(PubSubSubscription::new(&name).create_with_topic(&name))
        .await
        .expect("subscription opens");
    let publisher = connected.publisher();
    publisher
        .publish(OutgoingMessage::new(&name, b"again".as_slice()))
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
        .subscribe_descriptor(PubSubSubscription::new(&name).create_with_topic(&name))
        .await
        .expect("subscription opens");
    let publisher = connected.publisher();
    publisher
        .publish(OutgoingMessage::new(&name, b"poison".as_slice()))
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
        .publish(OutgoingMessage::new(&name, b"next".as_slice()))
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
