//! Four things a service asserts on the stand-in: the ordering step through an `Out` slot, a
//! batch handler, where a returned reply lands, and the subscription descriptor it declares its
//! handlers with.
//!
//! The step adapts a publisher, so it has to resolve on the slot entry a handler body holds.
//! Resolved anywhere below that entry it still reaches the broker, but the publish leaves through
//! the unwrapped publisher and the harness's per-slot capture misses it - a silent hole this test
//! closes from the outside.
//!
//! The batch handler is the other half: the stand-in assembles batches the way the real subscriber
//! does, so a `&[T]` body is unit-testable here rather than only against the emulator.
//!
//! A reply destination is resolved from the reply type, and the resolution runs through this
//! crate's default publish policy, so both spellings are checked against real Pub/Sub wiring.
//!
//! The routes file is what the cases at the end are about, and the point is that it needs no
//! editing to be mounted here: the same `GooglePubSub` that opens a streaming pull opens an
//! in-process subscription, and the same `Publish` policy that reaches Pub/Sub pairs with the
//! stand-in.

#![cfg(feature = "testing")]

use std::time::Duration;

use ruststream::runtime::{Out, PublishError};
use ruststream::testing::TestApp;
use ruststream::{ConnectedBroker as _, Outgoing, Serialized, SubscriptionSource as _};
use ruststream_gcp_pubsub::prelude::*;
use ruststream_gcp_pubsub::testing::PubSubTestBroker;
use ruststream_gcp_pubsub::{PARTITION_KEY_HEADER, PubSubError};
use serde::{Deserialize, Serialize};

/// The order the harness injects.
#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
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
            b.include(forward).out(DefaultSlot, Publish).build();
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

/// Settles whole batches of orders. The slice is what makes it a batch handler; the size it is
/// mounted with is what the batches are built to.
#[subscriber("orders-batches")]
async fn settle(orders: &[Order]) -> HandlerOutcome {
    // However the buffer's deadline split the run, a batch that reaches a body is never empty.
    assert!(!orders.is_empty(), "an empty batch reached the body");
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_batch_handler_runs_against_the_stand_in() {
    let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        PubSubTestBroker::new(),
        |b| {
            b.include(settle.batch(nonzero!(4)));
        },
    );
    let tb = TestApp::start(app)
        .await
        .expect("the harness starts the app");

    for id in 1..=4 {
        tb.broker::<PubSubTestBroker>()
            .message(&Order { id })
            .to("orders-batches")
            .publish()
            .await
            .expect("the harness accepts the injection");
    }
    tb.settle().await.expect("the batches settle");

    // How the four split across batches is the buffer's business (its deadline against the
    // injection timing); that every order reached the body is the contract.
    let received: Vec<u64> = tb
        .broker::<PubSubTestBroker>()
        .subscriber("orders-batches")
        .received::<Order>()
        .into_iter()
        .map(|order| order.id)
        .collect();
    assert_eq!(received, [1, 2, 3, 4]);

    tb.shutdown().await.expect("graceful shutdown");
}

/// The receipt every accepted order gets. It always goes to the same topic, so the topic is part
/// of the type and no mount site repeats it.
#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
#[outgoing(name = "receipts")]
struct Receipt {
    id: u64,
}

/// Answers an order with its receipt.
#[subscriber("orders-receipts", publish)]
async fn receipt(order: &Order) -> Receipt {
    Receipt { id: order.id }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reply_type_that_declares_a_topic_is_published_there() {
    let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        PubSubTestBroker::new(),
        |b| {
            b.include(receipt);
        },
    );
    let tb = TestApp::start(app)
        .await
        .expect("the harness starts the app");

    tb.broker::<PubSubTestBroker>()
        .message(&Order { id: 7 })
        .to("orders-receipts")
        .publish()
        .await
        .expect("the harness accepts the injection");
    tb.settle().await.expect("the handler settles");

    let broker = tb.broker::<PubSubTestBroker>();
    broker.subscriber("orders-receipts").assert_called_once();
    broker
        .published::<Receipt>("receipts")
        .assert_called_once()
        .with(&Receipt { id: 7 });

    tb.shutdown().await.expect("graceful shutdown");
}

/// The audit copy of an order. The same shape serves several topics, so each mount site says
/// which one it feeds.
#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
struct Audited {
    id: u64,
}

/// Copies an order to wherever it is mounted.
#[subscriber("orders-audit", publish("audit-eu"))]
async fn audit(order: &Order) -> Audited {
    Audited { id: order.id }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reply_type_without_a_topic_is_published_where_the_mount_site_says() {
    let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        PubSubTestBroker::new(),
        |b| {
            b.include(audit);
        },
    );
    let tb = TestApp::start(app)
        .await
        .expect("the harness starts the app");

    tb.broker::<PubSubTestBroker>()
        .message(&Order { id: 11 })
        .to("orders-audit")
        .publish()
        .await
        .expect("the harness accepts the injection");
    tb.settle().await.expect("the handler settles");

    let broker = tb.broker::<PubSubTestBroker>();
    broker.subscriber("orders-audit").assert_called_once();
    broker
        .published::<Audited>("audit-eu")
        .assert_called_once()
        .with(&Audited { id: 11 });

    tb.shutdown().await.expect("graceful shutdown");
}

/// The declaration a service ships, carrying the options a production subscription is opened
/// with. Nothing about it is test-shaped, and nothing about it needs to be.
#[subscriber(GooglePubSub::new("orders-descriptor")
    .create_with_topic("orders")
    .max_outstanding(500)
    .ack_extension(Duration::from_secs(30)))]
async fn confirm(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_production_descriptor_mounts_on_the_stand_in() {
    let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        PubSubTestBroker::new(),
        |b| {
            b.include(confirm);
        },
    );
    let tb = TestApp::start(app)
        .await
        .expect("the harness starts the app");

    tb.broker::<PubSubTestBroker>()
        .message(&Order { id: 11 })
        .to("orders-descriptor")
        .publish()
        .await
        .expect("the harness accepts the injection");
    tb.settle().await.expect("the handler settles");

    tb.broker::<PubSubTestBroker>()
        .subscriber("orders-descriptor")
        .assert_called_once()
        .with(&Order { id: 11 })
        .settled(HandlerOutcome::ack());

    // The stand-in routes by the subscription name and holds no topics, so `create_with_topic`
    // names no second address to reach this handler by. Documented on the source impl, pinned
    // here so it reads as a decision: the topic-to-subscription hop is the emulator's to prove.
    tb.broker::<PubSubTestBroker>()
        .message(&Order { id: 12 })
        .to("orders")
        .publish()
        .await
        .expect("the harness accepts the injection");
    tb.settle()
        .await
        .expect("a publish nothing subscribes to settles on the spot");
    tb.broker::<PubSubTestBroker>()
        .subscriber("orders-descriptor")
        .assert_called_once();

    tb.shutdown().await.expect("graceful shutdown");
}

/// What the planner returns; the mount site says where it goes.
#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
struct PlanItem {
    order_id: u64,
}

/// The other half of a routes file: a handler whose reply the mount site publishes.
#[subscriber(GooglePubSub::new("orders-plan"), publish("plan-items"))]
async fn plan(order: &Order) -> PlanItem {
    PlanItem { order_id: order.id }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_routes_file_a_service_ships_mounts_on_the_stand_in() {
    let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        PubSubTestBroker::new(),
        // Character for character what a routes file writes against the real broker: the
        // descriptor on the subscribe side, the policy under its mount-site name on the publish
        // side. Neither has a test-only spelling to swap in.
        |b| {
            b.include(plan).out(Reply, Publish);
        },
    );
    let tb = TestApp::start(app)
        .await
        .expect("the harness starts the app");

    tb.broker::<PubSubTestBroker>()
        .message(&Order { id: 5 })
        .to("orders-plan")
        .publish()
        .await
        .expect("the harness accepts the injection");
    tb.settle().await.expect("the handler settles");

    tb.broker::<PubSubTestBroker>()
        .published::<PlanItem>("plan-items")
        .assert_called_once()
        .with(&PlanItem { order_id: 5 });

    tb.shutdown().await.expect("graceful shutdown");
}

/// The batch half of the same declaration: the descriptor names how long a partial batch waits,
/// and the registration names how large it may grow.
#[subscriber(GooglePubSub::new("orders-descriptor-batches")
    .batch_wait(Duration::from_millis(20)))]
async fn settle_descriptor(orders: &[Order]) -> HandlerOutcome {
    assert!(!orders.is_empty(), "an empty batch reached the body");
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_descriptor_mounted_batch_handler_runs_against_the_stand_in() {
    let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        PubSubTestBroker::new(),
        |b| {
            b.include(settle_descriptor.batch(nonzero!(4)));
        },
    );
    let tb = TestApp::start(app)
        .await
        .expect("the harness starts the app");

    for id in 1..=4 {
        tb.broker::<PubSubTestBroker>()
            .message(&Order { id })
            .to("orders-descriptor-batches")
            .publish()
            .await
            .expect("the harness accepts the injection");
    }
    tb.settle().await.expect("the batches settle");

    let received: Vec<u64> = tb
        .broker::<PubSubTestBroker>()
        .subscriber("orders-descriptor-batches")
        .received::<Order>()
        .into_iter()
        .map(|order| order.id)
        .collect();
    assert_eq!(received, [1, 2, 3, 4]);

    tb.shutdown().await.expect("graceful shutdown");
}

/// The stand-in runs the descriptor's own check rather than a looser one, so a descriptor that
/// names no subscription fails here exactly where it fails against the product: before any
/// subscription is opened.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_empty_descriptor_is_rejected_by_the_stand_in() {
    let broker = PubSubTestBroker::new()
        .connect()
        .await
        .expect("the stand-in connects");

    let err = GooglePubSub::new("")
        .subscribe(&broker)
        .await
        .expect_err("a descriptor naming no subscription must not open one");

    assert!(
        matches!(err, PubSubError::InvalidDescriptor(_)),
        "got {err}"
    );
}

/// The ladder makes owner-side misuse a compile error; a publisher that outlived the shutdown is
/// what stays checkable at runtime, and the real policy pairs here now, so a service can write
/// this test against the stand-in.
///
/// That the publish fails at all is the framework's contract, pinned by `harness::lifecycle` in
/// `tests/conformance_pubsub.rs`. What this adds is the answer's shape: the variant a service
/// matches on is the broker's own, the same one the real publisher reports.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publishing_after_shutdown_errors() {
    let broker = PubSubTestBroker::new()
        .connect()
        .await
        .expect("the stand-in connects");
    let publisher = broker.publisher();
    broker.shutdown().await.expect("graceful shutdown");

    let err = publisher
        .message(&Order { id: 1 })
        .to("orders")
        .publish()
        .await
        .expect_err("a publish through the closed transport must error");

    // The broker's own variant, through the builder's wrapper: the same answer the real
    // publisher gives once its connection cell is closed.
    assert!(
        matches!(err, PublishError::Publish(PubSubError::NotConnected)),
        "got {err}"
    );
}
