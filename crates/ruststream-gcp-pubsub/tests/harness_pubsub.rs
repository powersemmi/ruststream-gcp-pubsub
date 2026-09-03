//! The ordering step through an `Out` slot, under the `TestApp` harness.
//!
//! The step adapts a publisher, so it has to resolve on the slot entry a handler body holds.
//! Resolved anywhere below that entry it still reaches the broker, but the publish leaves through
//! the unwrapped publisher and the harness's per-slot capture misses it - a silent hole this test
//! closes from the outside.

#![cfg(feature = "testing")]

use ruststream::runtime::Out;
use ruststream::testing::TestApp;
use ruststream::{Outgoing, Serialized};
use ruststream_gcp_pubsub::PARTITION_KEY_HEADER;
use ruststream_gcp_pubsub::prelude::*;
use ruststream_gcp_pubsub::testing::{PubSubTestBroker, PubSubTestPublish};
use serde::{Deserialize, Serialize};

/// The order the harness injects.
#[derive(Debug, Serialize, Deserialize, Outgoing)]
struct Order {
    id: u64,
}

/// What the handler forwards. The subject is the ordering key, so the payload takes the lane that
/// leaves it alone.
#[derive(Outgoing, Serialized)]
struct Wire(Vec<u8>);

/// Forwards every order under its own ordering key.
#[subscriber("orders-workers")]
async fn forward(order: &Order, Out(out): Out<impl PubSubOrdering>) -> HandlerOutcome {
    let keyed = out.with_ordering_key(format!("order-{}", order.id));
    if keyed
        .message(&Wire(b"forwarded".to_vec()))
        .to("confirmations")
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_ordering_step_on_a_slot_keeps_the_key_and_its_attribution() {
    let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        PubSubTestBroker::new(),
        |b| {
            b.include(forward).publisher(PubSubTestPublish);
        },
    );
    let tb = TestApp::start(app)
        .await
        .expect("the harness starts the app");

    tb.broker::<PubSubTestBroker>()
        .message(&Order { id: 7 })
        .to("orders-workers")
        .publish()
        .await
        .expect("the harness accepts the injection");
    tb.settle().await.expect("the handler settles");

    // The key reached the wire, under the header this crate maps onto the ordering key.
    let broker = tb.broker::<PubSubTestBroker>();
    let published = broker
        .published::<Vec<u8>>("confirmations")
        .assert_called_once()
        .with_raw(b"forwarded");
    assert_eq!(
        published.messages()[0]
            .headers()
            .get_str(PARTITION_KEY_HEADER),
        Some("order-7")
    );

    // And the publish is still the slot's, which is what a service asserts on.
    tb.out::<DefaultSlot>()
        .assert_called_once()
        .with_raw(b"forwarded");

    tb.shutdown().await.expect("graceful shutdown");
}
