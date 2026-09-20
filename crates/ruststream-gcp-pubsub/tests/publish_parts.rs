//! What a publish hands to the client: the payload buffer the framework wrote, and the header map
//! the transforms filled.
//!
//! Both travel rather than being copied, and content equality cannot say so - the bytes are equal
//! either way. The payload is read off its address; the map is read off this thread's allocation
//! count, because `Bytes` keeps its data pointer across a clone.
#![cfg(feature = "testing")]

use std::time::Duration;

use ruststream::testing::expect_published;
use ruststream::{Broker, BytesMut, ConnectedBroker, OutgoingMessage, Publisher};
use ruststream_gcp_pubsub::testing::PubSubTestBroker;

const WAIT: Duration = Duration::from_secs(1);

/// Pub/Sub keeps the payload, so the buffer the framework wrote reaches the router rather than a
/// copy of it, and the in-process transport answers the same way.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn published_payload_is_the_buffer_the_framework_wrote() {
    let broker = PubSubTestBroker::new().connect().await.expect("connect");
    let publisher = broker.publisher();
    let payload = BytesMut::from(&b"first"[..]);
    let written_at = payload.as_ptr();

    publisher
        .publish(OutgoingMessage::produced("events", payload), None)
        .await
        .expect("publish");

    let observed = expect_published(&broker, "events", 1, WAIT).await;
    assert_eq!(
        observed[0].payload().as_ptr(),
        written_at,
        "the transport keeps the payload, so it takes the buffer instead of copying it",
    );
    broker.shutdown().await.expect("shutdown");
}
