//! Services under `TestApp` whose handlers are mounted by hand: a `Handle` body bound to a
//! `GooglePubSub` descriptor with `subscriber(source, body)`, the manual twin of
//! `#[subscriber(..)]`.

#![cfg(feature = "testing")]

use std::future::{Future, ready};

use ruststream::Outgoing;
use ruststream::runtime::{AppInfo, Context, Handle, HandlerOutcome, RustStream, subscriber};
use ruststream::testing::TestApp;
use ruststream_gcp_pubsub::{GooglePubSub, PubSubBroker};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq, Outgoing)]
struct Order {
    id: u64,
}

/// Takes every order it is handed.
struct TakeOrder;

impl Handle<Order> for TakeOrder {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        let _ = order.id;
        ready(Ok(()))
    }
}

/// The descriptor mounts as it is, the way the attribute takes it, and its subscription reads
/// what is published to the topic it is created on.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_handle_body_mounts_on_a_pubsub_descriptor() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(
        PubSubBroker::new("my-project"),
        |b| {
            b.include(
                subscriber(
                    GooglePubSub::new("orders-workers").create_with_topic("orders"),
                    TakeOrder,
                )
                .undocumented()
                .build(),
            );
        },
    );
    let tb = TestApp::start(app).await.expect("start");

    tb.broker::<PubSubBroker>()
        .message(&Order { id: 1 })
        .to("orders")
        .publish()
        .await
        .expect("publish");

    tb.broker::<PubSubBroker>()
        .subscriber("orders-workers")
        .assert_called_once()
        .with(&Order { id: 1 })
        .settled(HandlerOutcome::ack());
    tb.shutdown().await.expect("shutdown");
}
