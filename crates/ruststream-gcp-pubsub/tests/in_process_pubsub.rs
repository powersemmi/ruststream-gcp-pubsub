//! The broker's in-process mode where the transport is the subject: what `connect_in_process`
//! connects, driven directly on the connected form the harness would connect, so a failure points
//! at the transport rather than at an app.
//!
//! Each case is a Pub/Sub behaviour the transport holds to: a subscription keeps what its topic
//! got while no consumer was open, consumers of one subscription share it, a dropped delivery comes
//! back, and what the service refuses at publish time is refused here with the same error variant.
//! What only the service does is covered against the emulator by `tests/integration_pubsub.rs`.

#![cfg(feature = "testing")]

use std::time::Duration;

use futures::{Stream, StreamExt};
use ruststream::testing::{InProcess, TestableBroker};
use ruststream::{
    ConnectedBroker, HeaderMap, IncomingMessage, OutgoingMessage, Publisher, Subscriber,
    SubscriptionSource,
};
use ruststream_gcp_pubsub::{
    ConnectedPubSubBroker, GooglePubSub, PubSubBroker, PubSubError, PubSubMessage,
};

const WAIT: Duration = Duration::from_secs(1);

/// The transition the harness connects through: the production broker, connected in process.
async fn connected() -> ConnectedPubSubBroker {
    PubSubBroker::new("my-project")
        .connect_in_process()
        .await
        .expect("connect in process")
}

/// The subscription every case consumes, created attached to the `orders` topic.
fn workers() -> GooglePubSub {
    GooglePubSub::new("orders-workers").create_with_topic("orders")
}

async fn publish(broker: &ConnectedPubSubBroker, topic: &str, payload: &[u8]) {
    broker
        .publisher()
        .publish(OutgoingMessage::new(topic, payload), None)
        .await
        .expect("publish");
}

async fn next<S>(stream: &mut S) -> PubSubMessage
where
    S: Stream<Item = Result<PubSubMessage, PubSubError>> + Unpin,
{
    tokio::time::timeout(WAIT, stream.next())
        .await
        .expect("a delivery within the timeout")
        .expect("the stream is open")
        .expect("the delivery is ok")
}

async fn nothing_more<S>(stream: &mut S)
where
    S: Stream<Item = Result<PubSubMessage, PubSubError>> + Unpin,
{
    assert!(
        tokio::time::timeout(Duration::from_millis(100), stream.next())
            .await
            .is_err(),
        "no further delivery is due",
    );
}

/// A subscription is a resource: what its topic got while no consumer was open waits for the
/// next one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_subscription_keeps_what_its_topic_got_while_no_consumer_was_open() {
    let broker = connected().await;
    drop(workers().subscribe(&broker).await.expect("subscribe"));

    publish(&broker, "orders", b"while-away").await;

    let mut subscriber = workers().subscribe(&broker).await.expect("subscribe again");
    let mut stream = std::pin::pin!(subscriber.stream());
    let delivery = next(&mut stream).await;
    assert_eq!(delivery.payload(), b"while-away");
    delivery.ack().await.expect("ack");
    broker.shutdown().await.expect("shutdown");
}

/// A publish goes to a topic, so a publish to the subscription's own name reaches nothing, and a
/// full resource name reaches the same topic as the short one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_subscription_receives_what_its_topic_receives() {
    let broker = connected().await;
    let mut subscriber = workers().subscribe(&broker).await.expect("subscribe");

    publish(&broker, "orders-workers", b"to-the-subscription").await;
    publish(
        &broker,
        "projects/my-project/topics/orders",
        b"to-the-topic",
    )
    .await;

    let mut stream = std::pin::pin!(subscriber.stream());
    let delivery = next(&mut stream).await;
    assert_eq!(delivery.payload(), b"to-the-topic");
    delivery.ack().await.expect("ack");
    nothing_more(&mut stream).await;
    broker.shutdown().await.expect("shutdown");
}

/// The harness learns which subscriptions a publish reaches from the broker: the ones attached to
/// the topic, and no other.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn routes_answer_the_subscriptions_attached_to_the_topic() {
    let broker = connected().await;
    let _workers = workers().subscribe(&broker).await.expect("subscribe");
    let _audit = GooglePubSub::new("orders-audit")
        .create_with_topic("orders")
        .subscribe(&broker)
        .await
        .expect("subscribe");
    let _payments = GooglePubSub::new("payments-workers")
        .create_with_topic("payments")
        .subscribe(&broker)
        .await
        .expect("subscribe");

    let subscriptions = [
        "orders-workers",
        "payments-workers",
        "orders-audit",
        "orders",
    ];
    assert_eq!(broker.routes("orders", &subscriptions), [0, 2]);
    assert_eq!(broker.routes("orders-workers", &subscriptions), [0usize; 0]);
    broker.shutdown().await.expect("shutdown");
}

/// Two consumers of one subscription compete: each message reaches one of them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consumers_of_one_subscription_share_its_messages() {
    let broker = connected().await;
    let mut first = workers().subscribe(&broker).await.expect("subscribe");
    let mut second = workers().subscribe(&broker).await.expect("subscribe");

    publish(&broker, "orders", b"one").await;
    publish(&broker, "orders", b"two").await;

    let mut first = std::pin::pin!(first.stream());
    let mut second = std::pin::pin!(second.stream());
    let mut payloads = Vec::new();
    let delivery = next(&mut first).await;
    payloads.push(delivery.payload().to_vec());
    delivery.ack().await.expect("ack");
    let delivery = next(&mut second).await;
    payloads.push(delivery.payload().to_vec());
    delivery.ack().await.expect("ack");
    nothing_more(&mut first).await;
    nothing_more(&mut second).await;
    payloads.sort();
    assert_eq!(payloads, [b"one".to_vec(), b"two".to_vec()]);
    broker.shutdown().await.expect("shutdown");
}

/// A delivery dropped without a settlement is rejected, as the client's handler rejects one on
/// drop, so the subscription hands it out again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_delivery_dropped_unsettled_comes_back() {
    let broker = connected().await;
    let mut subscriber = workers().subscribe(&broker).await.expect("subscribe");
    publish(&broker, "orders", b"dropped").await;

    let mut stream = std::pin::pin!(subscriber.stream());
    drop(next(&mut stream).await);
    let again = next(&mut stream).await;
    assert_eq!(again.payload(), b"dropped");
    again.ack().await.expect("ack");
    nothing_more(&mut stream).await;
    broker.shutdown().await.expect("shutdown");
}

/// The ladder makes owner-side misuse a compile error; a publisher that outlived the shutdown is
/// what stays checkable at runtime, and it answers with the variant a service matches on.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publishing_after_shutdown_errors() {
    let broker = connected().await;
    let publisher = broker.publisher();
    broker.shutdown().await.expect("shutdown");

    let err = publisher
        .publish(OutgoingMessage::new("orders", b"late".as_slice()), None)
        .await
        .expect_err("a publish through the closed connection must error");
    assert!(matches!(err, PubSubError::NotConnected), "got {err}");
}

/// What the service refuses at publish time is refused here with the same variant: a topic id it
/// does not accept, a message with nothing in it, an attribute past its limit, a reserved key.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_publish_the_service_refuses_is_refused() {
    let broker = connected().await;
    let publisher = broker.publisher();

    let mut oversized = HeaderMap::new();
    oversized.insert("x-note", "n".repeat(1025));
    let mut reserved = HeaderMap::new();
    reserved.insert("googclient_origin", "test");

    for (case, message) in [
        (
            "a topic id with a space",
            OutgoingMessage::new("orders eu", b"{}".as_slice()),
        ),
        (
            "a topic id starting with a digit",
            OutgoingMessage::new("1orders", b"{}".as_slice()),
        ),
        (
            "no data and no attribute",
            OutgoingMessage::new("orders", b"".as_slice()),
        ),
        (
            "an attribute value past 1024 bytes",
            OutgoingMessage::new("orders", b"{}".as_slice()).with_headers(oversized),
        ),
        (
            "an attribute key starting with goog",
            OutgoingMessage::new("orders", b"{}".as_slice()).with_headers(reserved),
        ),
    ] {
        let err = publisher.publish(message, None).await.expect_err(case);
        assert!(
            matches!(err, PubSubError::Publish { .. }),
            "{case}: got {err}"
        );
    }
    assert!(
        broker.published("orders").is_empty(),
        "a refused publish reaches no topic",
    );
    broker.shutdown().await.expect("shutdown");
}

/// A descriptor that creates a resource the service refuses to name fails at subscribe, with the
/// admin error the service's refusal is reported as.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_resource_name_the_service_refuses_does_not_open() {
    let broker = connected().await;
    let err = GooglePubSub::new("orders-workers")
        .create_with_topic("goog-orders")
        .subscribe(&broker)
        .await
        .expect_err("a reserved topic id is refused");
    assert!(matches!(err, PubSubError::Admin { .. }), "got {err}");

    let err = GooglePubSub::new("")
        .subscribe(&broker)
        .await
        .expect_err("a descriptor naming no subscription must not open one");
    assert!(
        matches!(err, PubSubError::InvalidDescriptor(_)),
        "got {err}"
    );
    broker.shutdown().await.expect("shutdown");
}
