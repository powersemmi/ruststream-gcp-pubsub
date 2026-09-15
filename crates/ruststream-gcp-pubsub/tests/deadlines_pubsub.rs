//! The deadlines one delivery runs on, against the Pub/Sub emulator, gated behind
//! `PUBSUB_TEST_HOST`.
//!
//! Three of the subscription descriptor's settings are durations, and all three are only
//! themselves in time: how long the client keeps a delivery leased, how far each extension of
//! that lease reaches, and how long a batch short of its size waits for company. Each case here
//! drives one of them past its edge and asserts on both sides of it, because a setting that went
//! unread looks exactly like one that was honoured until the clock disagrees.
//!
//! These cases wait out real deadlines, so they are the slow half of the live suite and live
//! apart from the routing checks in `integration_pubsub.rs`.
//!
//! Start the emulator with `just brokers-up`, then:
//! `PUBSUB_TEST_HOST=127.0.0.1:8085 cargo test --all-features -- --test-threads=1`.

use std::num::NonZeroUsize;
use std::pin::pin;
use std::time::{Duration, Instant};

use futures::StreamExt;
use ruststream::{
    BatchSubscriber, Broker, ConnectedBroker, IncomingMessage, OutgoingMessage, Publisher,
    Subscriber,
};
use ruststream_gcp_pubsub::{ConnectedPubSubBroker, GooglePubSub, PubSubBroker};

mod live;

const RECV_TIMEOUT: Duration = Duration::from_secs(15);
const TEST_PROJECT: &str = "ruststream-test";

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
    format!("dl-{name}-{}", std::process::id())
}

/// The deadline the descriptor names, and the window a test looks in before it. The client
/// clamps an ack extension to 10s at the low end, so a value the subscription's own 10s cannot
/// be mistaken for is what proves the descriptor's reaches the wire.
const ACK_EXTENSION: Duration = Duration::from_secs(20);
const BEFORE_THE_DEADLINE: Duration = Duration::from_secs(15);
const REDELIVERY_TIMEOUT: Duration = Duration::from_secs(45);

/// A delivery nothing settles is leased for as long as the client keeps extending it, and
/// [`GooglePubSub::max_lease`] is the budget. Spent, the client stops extending, the deadline the
/// descriptor named runs out, and the subscription hands the message to somebody again.
///
/// This is what makes the refusal of a longer delayed retry mean something: past the lease the
/// delivery really does come back.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_delivery_comes_back_once_its_lease_is_spent() {
    let Some(host) = test_host() else { return };
    let connected = connect(&host).await;

    let name = unique("lease-spent");
    let mut subscriber = connected
        .subscribe_descriptor(
            GooglePubSub::new(&name)
                .create_with_topic(&name)
                .max_lease(Duration::from_secs(1))
                .ack_extension(ACK_EXTENSION),
        )
        .await
        .expect("subscription opens");
    connected
        .publisher()
        .publish(OutgoingMessage::new(&name, b"unsettled".as_slice()), None)
        .await
        .expect("publish succeeds");

    let mut stream = pin!(subscriber.stream());
    // Held, not settled: the handle stays alive for the whole case, because dropping it would
    // reject the delivery and the redelivery would prove nothing about the lease.
    let held = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");

    let waited = Instant::now();
    let again = tokio::time::timeout(REDELIVERY_TIMEOUT, stream.next())
        .await
        .expect("the subscription redelivers once the lease is spent")
        .expect("stream is open")
        .expect("delivery is ok");
    assert_eq!(again.payload(), b"unsettled");
    assert!(
        waited.elapsed() >= BEFORE_THE_DEADLINE,
        "the redelivery waited {:?}, so the deadline was the subscription's own and not the \
         {ACK_EXTENSION:?} the descriptor named",
        waited.elapsed(),
    );

    drop(held);
    again.ack().await.expect("ack succeeds");
    connected.shutdown().await.expect("shutdown succeeds");
}

/// The same delivery under the lease the descriptor leaves alone: the client goes on extending
/// the deadline, so the subscription hands it to nobody else. A handler that takes its time is
/// the reason the extension exists.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_delivery_whose_lease_is_extended_stays_with_its_handler() {
    let Some(host) = test_host() else { return };
    let connected = connect(&host).await;

    let name = unique("lease-held");
    let mut subscriber = connected
        .subscribe_descriptor(
            GooglePubSub::new(&name)
                .create_with_topic(&name)
                // The shortest deadline the client will negotiate, so the window below outlasts
                // two of them and an extension is the only way nothing comes back.
                .ack_extension(Duration::from_secs(10)),
        )
        .await
        .expect("subscription opens");
    connected
        .publisher()
        .publish(OutgoingMessage::new(&name, b"working".as_slice()), None)
        .await
        .expect("publish succeeds");

    let mut stream = pin!(subscriber.stream());
    let held = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");

    assert!(
        tokio::time::timeout(Duration::from_secs(25), stream.next())
            .await
            .is_err(),
        "a delivery whose lease is being extended must not be handed to anybody else",
    );

    held.ack().await.expect("ack succeeds");
    connected.shutdown().await.expect("shutdown succeeds");
}

/// How long a partial batch waits for company. The window below has to sit between the crate's
/// 50ms default and this, so that a batch arriving early would mean the descriptor went unread.
const BATCH_WAIT: Duration = Duration::from_secs(4);
const BEFORE_THE_BATCH: Duration = Duration::from_secs(2);

/// A batch handler asks for a size, and the deadline is what closes a batch that never reaches
/// it. Below the deadline the subscription holds what it has; past it the handler gets the
/// partial batch rather than nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_partial_batch_waits_out_the_descriptors_deadline() {
    let Some(host) = test_host() else { return };
    let connected = connect(&host).await;

    let name = unique("batch-wait");
    let mut subscriber = connected
        .subscribe_descriptor(
            GooglePubSub::new(&name)
                .create_with_topic(&name)
                .batch_wait(BATCH_WAIT),
        )
        .await
        .expect("subscription opens");
    connected
        .publisher()
        .publish(OutgoingMessage::new(&name, b"alone".as_slice()), None)
        .await
        .expect("publish succeeds");

    let mut batches = pin!(subscriber.batches(NonZeroUsize::new(5).expect("a batch size")));
    assert!(
        tokio::time::timeout(BEFORE_THE_BATCH, batches.next())
            .await
            .is_err(),
        "a batch short of its size must wait out the deadline the descriptor named",
    );

    let batch = tokio::time::timeout(RECV_TIMEOUT, batches.next())
        .await
        .expect("the partial batch arrives once the deadline is out")
        .expect("stream is open")
        .expect("batch is ok");
    assert_eq!(batch.len(), 1);
    for message in batch {
        message.ack().await.expect("ack succeeds");
    }

    connected.shutdown().await.expect("shutdown succeeds");
}
