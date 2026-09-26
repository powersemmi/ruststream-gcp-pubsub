//! What a publish hands to the client: the payload buffer the framework wrote.
//!
//! It travels rather than being copied, and content equality cannot say so - the bytes are equal
//! either way. The payload is read off its address, on the in-process transport, which frames a
//! publish with the conversion a publish to the service goes through.
#![cfg(feature = "testing")]

use std::time::Duration;

use ruststream::testing::{InProcess, expect_published};
use ruststream::{BytesMut, ConnectedBroker, OutgoingMessage, Publisher};
use ruststream_gcp_pubsub::PubSubBroker;

const WAIT: Duration = Duration::from_secs(1);

/// Pub/Sub keeps the payload, so the buffer the framework wrote becomes the message's data rather
/// than a copy of it, and the in-process transport keeps that same buffer.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn published_payload_is_the_buffer_the_framework_wrote() {
    let broker = PubSubBroker::new("my-project")
        .connect_in_process()
        .await
        .expect("connect in process");
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
        "the client keeps the payload, so the message takes the buffer instead of copying it",
    );
    broker.shutdown().await.expect("shutdown");
}
