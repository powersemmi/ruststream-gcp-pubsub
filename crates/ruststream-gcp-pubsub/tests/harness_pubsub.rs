//! Five things a service asserts on the stand-in: the ordering step through an `Out` slot, a
//! batch handler, where a returned reply lands, the subscription descriptor it declares its
//! handlers with, and what happens to a delivery whose retries run out.
//!
//! The step is a position on the publish builder, so a keyed publish is still the slot's: it keeps
//! the slot's attribution, the codec the mount site named, and the key the call asked for. A step
//! that wrapped the publisher instead would lose all three, which is what the codec case pins.
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
//!
//! The retries a message gets are the last one: the mount site declares the cap and the
//! destination, and the subscription's own dead-letter policy carries a spent delivery away.
//!
//! A reply has no call site to name its key, so a transform on the reply position writes the
//! setting instead, and the harness reads back what it wrote.

#![cfg(feature = "testing")]

use std::time::Duration;

use ruststream::codec::CborCodec;
// `Outgoing` names the derive at the crate root and the publish pipeline's message type in
// `runtime`; a publish transform takes the second one, and the two live in different namespaces.
use ruststream::runtime::{Out, Outgoing, PublishContext, PublishError};
use ruststream::testing::TestApp;
use ruststream::{ConnectedBroker as _, HeaderMap, Outgoing, SubscriptionSource as _};
use ruststream_gcp_pubsub::prelude::*;
use ruststream_gcp_pubsub::testing::PubSubTestBroker;
use ruststream_gcp_pubsub::{DELIVERY_ATTEMPT_HEADER, PARTITION_KEY_HEADER, PubSubError};
use serde::{Deserialize, Serialize};

/// The order the harness injects.
#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
struct Order {
    id: u64,
}

/// Forwards every order under its own ordering key. The body names a per-message setting, so it
/// imports this crate's prelude and bounds its slot on this broker's settings type.
#[subscriber("orders-workers")]
async fn forward(
    order: &Order,
    Out(out): Out<impl Publisher<Options = PubSubPublishOptions>>,
) -> HandlerOutcome {
    if out
        .message(order)
        .to("confirmations")
        .ordering_key(format!("order-{}", order.id))
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
            b.include(forward)
                .out(DefaultSlot, Publish::default())
                .build();
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

    // The key reached the wire: the stand-in reports it where a delivery off Pub/Sub does.
    tb.broker::<PubSubTestBroker>()
        .published::<Order>("confirmations")
        .assert_called_once()
        .with(&Order { id: 7 })
        .with_header(PARTITION_KEY_HEADER, "order-7");

    // And the publish is still the slot's, carrying the setting the call asked for.
    tb.out::<DefaultSlot>()
        .assert_called_once()
        .with_options(&PubSubPublishOptions {
            ordering_key: Some("order-7".to_owned()),
        });

    tb.shutdown().await.expect("graceful shutdown");
}

/// The defect the typed settings close: a step is a position on the builder, not a wrapper around
/// the publisher, so a keyed publish still encodes with the codec the mount site named. Under the
/// adapter this crate shipped before, this message left as JSON.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_keyed_publish_keeps_the_codec_the_mount_site_named() {
    let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        PubSubTestBroker::new(),
        |b| {
            b.include(forward)
                .out(DefaultSlot, Publish::default())
                .codec(CborCodec)
                .build();
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

    tb.out::<DefaultSlot>()
        .assert_called_once()
        .decoded_as::<Order>()
        .with_codec(&CborCodec, &Order { id: 7 });

    tb.shutdown().await.expect("graceful shutdown");
}

/// The other half of the pair: the mount site fixes one key for a whole slot, and a body that
/// names no step sends under it. That is where a slot belongs to one entity end to end.
#[subscriber("orders-audit")]
async fn audit_trail(order: &Order, Out(out): Out<impl Publisher>) -> HandlerOutcome {
    if out
        .message(order)
        .to("audit-trail")
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_mount_sites_key_applies_where_the_call_names_none() {
    let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        PubSubTestBroker::new(),
        |b| {
            b.include(audit_trail)
                .out(DefaultSlot, Publish::default().ordering_key("audit"))
                .build();
        },
    );
    let tb = TestApp::start(app)
        .await
        .expect("the harness starts the app");

    tb.broker::<PubSubTestBroker>()
        .message(&Order { id: 7 })
        .to("orders-audit")
        .publish()
        .await
        .expect("the harness accepts the injection");
    tb.settle().await.expect("the handler settles");

    tb.broker::<PubSubTestBroker>()
        .published::<Order>("audit-trail")
        .assert_called_once()
        .with_header(PARTITION_KEY_HEADER, "audit");

    // Nothing on the call touched a setting, which is what makes the mount site the whole answer.
    tb.out::<DefaultSlot>()
        .assert_called_once()
        .assert_options_default();

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
            b.include(plan).out_reply(Publish::default());
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

/// A payment the upstream never settles, so every delivery asks for another attempt.
#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
struct Payment {
    id: u64,
}

/// How many deliveries one payment gets before the subscription carries it away.
const MAX_ATTEMPTS: u32 = 5;

/// Where a payment goes once those attempts are spent.
const DEAD_LETTER: &str = "payments-dead";

/// The delivery a handler was called on, as the subscription counts it.
fn attempt_of(headers: &HeaderMap) -> Option<u32> {
    headers
        .get_str(DELIVERY_ATTEMPT_HEADER)
        .and_then(|value| value.parse().ok())
}

/// Never settles, so the subscription's own cap is what ends the message.
#[subscriber(GooglePubSub::new("payments-workers"))]
async fn never_settles(payment: &Payment) -> HandlerOutcome {
    let _ = payment.id;
    HandlerOutcome::retry()
}

/// Drops its first delivery, which is a handler saying this message is done with.
#[subscriber(GooglePubSub::new("payments-workers"))]
async fn drops_the_first_delivery(payment: &Payment) -> HandlerOutcome {
    let _ = payment.id;
    HandlerOutcome::drop()
}

/// Settles on the third delivery, which it reads off the count the subscription reports.
#[subscriber(GooglePubSub::new("payments-workers"))]
async fn settles_on_the_third_attempt(payment: &Payment, ctx: &mut Context<'_>) -> HandlerOutcome {
    let _ = payment.id;
    if attempt_of(ctx.headers()) == Some(3) {
        return HandlerOutcome::ack();
    }
    HandlerOutcome::retry()
}

/// The delay a deferred delivery waits out before the subscription gets it back, split so a test
/// can stand just short of it and then step over.
const RETRY_DELAY: Duration = Duration::from_secs(2);
const JUST_SHORT_OF_IT: Duration = Duration::from_millis(1_999);
const THE_LAST_TICK: Duration = Duration::from_millis(1);

/// Asks for a later attempt on the first delivery and settles whatever comes back.
#[subscriber(GooglePubSub::new("payments-workers"))]
async fn defers_the_first_delivery(payment: &Payment, ctx: &mut Context<'_>) -> HandlerOutcome {
    let _ = payment.id;
    if attempt_of(ctx.headers()) == Some(1) {
        return HandlerOutcome::retry_after(RETRY_DELAY);
    }
    HandlerOutcome::ack()
}

/// Never settles and always asks for a later attempt, so the cap is what ends the message.
#[subscriber(GooglePubSub::new("payments-workers"))]
async fn always_defers(payment: &Payment) -> HandlerOutcome {
    let _ = payment.id;
    HandlerOutcome::retry_after(RETRY_DELAY)
}

/// Pub/Sub has no delayed nack, so the crate holds the delivery in the process and hands it back
/// when the delay is out. The clock is paused because the delay is what the case is about:
/// `advance` returns the delivery that is due instead of waiting two seconds for it.
#[tokio::test(start_paused = true)]
async fn a_deferred_delivery_comes_back_when_the_delay_is_out() {
    let app = RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(
        PubSubTestBroker::new(),
        |b| {
            b.include(defers_the_first_delivery)
                .max_attempts(nonzero!(MAX_ATTEMPTS))
                .dead_letter(DEAD_LETTER);
        },
    );
    let tb = TestApp::start(app)
        .await
        .expect("the harness starts the app");

    tb.broker::<PubSubTestBroker>()
        .message(&Payment { id: 5 })
        .to("payments-workers")
        .publish()
        .await
        .expect("the harness accepts the injection");

    // Not before: the delivery is held, so nothing has come back yet.
    tb.advance(JUST_SHORT_OF_IT)
        .await
        .expect("nothing is due yet");
    tb.broker::<PubSubTestBroker>()
        .subscriber("payments-workers")
        .assert_called(1);

    // And after: the rest of the delay is what brings it back.
    tb.advance(THE_LAST_TICK)
        .await
        .expect("the held delivery comes back");
    tb.broker::<PubSubTestBroker>()
        .subscriber("payments-workers")
        .assert_called(2)
        .settled(HandlerOutcome::ack());

    tb.shutdown().await.expect("graceful shutdown");
}

/// A delayed retry spends the same attempts an immediate one does, so a handler that only ever
/// defers still ends at the dead-letter topic rather than holding the message forever.
#[tokio::test(start_paused = true)]
async fn a_deferred_delivery_still_runs_out_of_attempts() {
    let app = RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(
        PubSubTestBroker::new(),
        |b| {
            b.include(always_defers)
                .max_attempts(nonzero!(MAX_ATTEMPTS))
                .dead_letter(DEAD_LETTER);
        },
    );
    let tb = TestApp::start(app)
        .await
        .expect("the harness starts the app");

    tb.broker::<PubSubTestBroker>()
        .message(&Payment { id: 9 })
        .to("payments-workers")
        .publish()
        .await
        .expect("the harness accepts the injection");
    for _ in 1..MAX_ATTEMPTS {
        tb.advance(RETRY_DELAY)
            .await
            .expect("the held delivery comes back");
    }

    tb.broker::<PubSubTestBroker>()
        .subscriber("payments-workers")
        .assert_called(MAX_ATTEMPTS as usize);
    tb.broker::<PubSubTestBroker>()
        .published::<Payment>(DEAD_LETTER)
        .assert_called(1)
        .with(&Payment { id: 9 });

    tb.shutdown().await.expect("graceful shutdown");
}

/// Pub/Sub moves a spent delivery itself, so the mount site declares the cap and the destination
/// and the subscription's dead-letter policy carries the message away. Nothing is published from
/// the service, which is why `.out_retry(..)` does not compile over this descriptor.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_spent_delivery_leaves_for_the_declared_dead_letter_topic() {
    let app = RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(
        PubSubTestBroker::new(),
        |b| {
            b.include(never_settles)
                .max_attempts(nonzero!(MAX_ATTEMPTS))
                .dead_letter(DEAD_LETTER);
        },
    );
    let tb = TestApp::start(app)
        .await
        .expect("the harness starts the app");

    tb.broker::<PubSubTestBroker>()
        .message(&Payment { id: 3 })
        .to("payments-workers")
        .publish()
        .await
        .expect("the harness accepts the injection");
    tb.settle().await.expect("the retries run out");

    // Five deliveries, the declared cap, and not a sixth.
    tb.broker::<PubSubTestBroker>()
        .subscriber("payments-workers")
        .assert_called(MAX_ATTEMPTS as usize);

    // The payment itself is on the dead-letter topic, as it arrived.
    tb.broker::<PubSubTestBroker>()
        .published::<Payment>(DEAD_LETTER)
        .assert_called(1)
        .with(&Payment { id: 3 });

    tb.shutdown().await.expect("graceful shutdown");
}

/// A delivery the handler drops is done with, and a dead-letter policy does not change that: the
/// destination is where a message goes when its attempts run out, not where a handler sends what
/// it decided to discard.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dropped_delivery_does_not_reach_the_dead_letter_topic() {
    let app = RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(
        PubSubTestBroker::new(),
        |b| {
            b.include(drops_the_first_delivery)
                .max_attempts(nonzero!(MAX_ATTEMPTS))
                .dead_letter(DEAD_LETTER);
        },
    );
    let tb = TestApp::start(app)
        .await
        .expect("the harness starts the app");

    tb.broker::<PubSubTestBroker>()
        .message(&Payment { id: 11 })
        .to("payments-workers")
        .publish()
        .await
        .expect("the harness accepts the injection");
    tb.settle().await.expect("the handler drops the delivery");

    tb.broker::<PubSubTestBroker>()
        .subscriber("payments-workers")
        .assert_called(1);

    tb.broker::<PubSubTestBroker>()
        .published::<Payment>(DEAD_LETTER)
        .assert_called(0);

    tb.shutdown().await.expect("graceful shutdown");
}

/// The count a delivery carries is the subscription's own, so a handler can branch on how many
/// times a message has come back. A message that settles before the cap never reaches the
/// dead-letter topic.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_delivery_reports_which_attempt_it_is() {
    let app = RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(
        PubSubTestBroker::new(),
        |b| {
            b.include(settles_on_the_third_attempt)
                .max_attempts(nonzero!(MAX_ATTEMPTS))
                .dead_letter(DEAD_LETTER);
        },
    );
    let tb = TestApp::start(app)
        .await
        .expect("the harness starts the app");

    tb.broker::<PubSubTestBroker>()
        .message(&Payment { id: 7 })
        .to("payments-workers")
        .publish()
        .await
        .expect("the harness accepts the injection");
    tb.settle().await.expect("the third delivery settles");

    tb.broker::<PubSubTestBroker>()
        .subscriber("payments-workers")
        .assert_called(3)
        .settled(HandlerOutcome::ack());

    tb.broker::<PubSubTestBroker>()
        .published::<Payment>(DEAD_LETTER)
        .assert_called(0);

    tb.shutdown().await.expect("graceful shutdown");
}

/// The receipt an order gets back, under the key of the order it answers.
#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
#[outgoing(name = "order-receipts")]
struct KeyedReceipt {
    id: u64,
}

/// Sends each receipt under the key of the order it answers, read off the delivery. A transform
/// that writes a setting names the settings type it writes, so it mounts over a Pub/Sub publisher
/// and over no other broker's.
#[derive(Debug, Clone, Copy)]
struct ReplyUnderTheOrdersKey;

impl<C> PublishTransform<ForReply<C>, PubSubPublishOptions> for ReplyUnderTheOrdersKey {
    type Destination = Reads;

    fn apply(
        &self,
        _out: &mut Outgoing<'_>,
        options: &mut Option<PubSubPublishOptions>,
        cx: &PublishContext<'_, C>,
    ) {
        if let Some(key) = cx.headers().get_str(PARTITION_KEY_HEADER) {
            options
                .get_or_insert_with(PubSubPublishOptions::default)
                .ordering_key = Some(key.to_owned());
        }
    }
}

/// Answers an order with its receipt.
#[subscriber("orders-keyed", publish)]
async fn keyed_receipt(order: &Order) -> KeyedReceipt {
    KeyedReceipt { id: order.id }
}

/// A reply carries no call site, so a key that differs per reply is a transform's to write: it
/// reads the delivery and fills the message's settings, which is where a setting belongs. The
/// harness reads the value back as this broker's own type, so the assertion holds the transform
/// to the setting rather than to a header that happens to travel.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_transform_on_the_reply_names_the_ordering_key() {
    let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        PubSubTestBroker::new(),
        |b| {
            b.include(keyed_receipt)
                .out_reply(Publish::default())
                .transform(ReplyUnderTheOrdersKey);
        },
    );
    let tb = TestApp::start(app)
        .await
        .expect("the harness starts the app");

    let mut headers = HeaderMap::new();
    headers.insert(PARTITION_KEY_HEADER, "order-7");
    tb.broker::<PubSubTestBroker>()
        .message(&Order { id: 7 })
        .with_headers(headers)
        .to("orders-keyed")
        .publish()
        .await
        .expect("the harness accepts the injection");
    tb.settle().await.expect("the handler settles");

    tb.broker::<PubSubTestBroker>()
        .published::<KeyedReceipt>("order-receipts")
        .assert_called_once()
        .with(&KeyedReceipt { id: 7 })
        .with_options(&PubSubPublishOptions {
            ordering_key: Some("order-7".to_owned()),
        })
        // And the publisher resolved it: the stand-in reports the key where a delivery does.
        .with_header(PARTITION_KEY_HEADER, "order-7");

    tb.shutdown().await.expect("graceful shutdown");
}

/// The subscription a bare name opens, with no descriptor for the mount site to declare on.
const BY_NAME: &str = "payments-by-name";

/// Never settles, on a subscription the handler names with a plain string. The declaration has
/// to reach the subscription through the broker, because there is no descriptor to carry it.
#[subscriber("payments-by-name")]
async fn never_settles_by_name(payment: &Payment) -> HandlerOutcome {
    let _ = payment.id;
    HandlerOutcome::retry()
}

/// A handler naming its subscription with a plain string gets the dead-letter policy the mount
/// site declared: five deliveries, then the declared topic. The broker takes the declaration for
/// the name and opens the subscription with it, so `GooglePubSub` is no longer the only spelling
/// a cap reaches.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bare_name_opens_with_the_declared_dead_letter_policy() {
    let app = RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(
        PubSubTestBroker::new(),
        |b| {
            b.include(never_settles_by_name)
                .max_attempts(nonzero!(MAX_ATTEMPTS))
                .dead_letter(DEAD_LETTER);
        },
    );
    let tb = TestApp::start(app)
        .await
        .expect("the harness starts the app");

    tb.broker::<PubSubTestBroker>()
        .message(&Payment { id: 13 })
        .to(BY_NAME)
        .publish()
        .await
        .expect("the harness accepts the injection");
    tb.settle().await.expect("the retries run out");

    tb.broker::<PubSubTestBroker>()
        .subscriber(BY_NAME)
        .assert_called(MAX_ATTEMPTS as usize);
    tb.broker::<PubSubTestBroker>()
        .published::<Payment>(DEAD_LETTER)
        .assert_called(1)
        .with(&Payment { id: 13 });

    tb.shutdown().await.expect("graceful shutdown");
}

/// Half a declaration is half a dead-letter policy, which Pub/Sub has no field for. The broker
/// refuses it for a bare name exactly as the descriptor refuses it, so the service does not
/// start without the cap it asked for.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bare_name_refuses_half_a_declaration() {
    let app = RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(
        PubSubTestBroker::new(),
        |b| {
            b.include(never_settles_by_name)
                .max_attempts(nonzero!(MAX_ATTEMPTS));
        },
    );

    let err = TestApp::start(app)
        .await
        .expect_err("a cap with nowhere to send a spent message must not start");
    let reported = format!("{err:#}");
    assert!(
        reported.contains("dead-letter policy needs the topic too"),
        "the refusal must name what is missing: {reported}",
    );
}

/// Pub/Sub bounds `maxDeliveryAttempts`, and the bound holds for a bare name too: the refusal
/// comes at startup rather than from the admin call that would reject the policy.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bare_name_refuses_a_cap_outside_the_services_range() {
    let app = RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(
        PubSubTestBroker::new(),
        |b| {
            b.include(never_settles_by_name)
                .max_attempts(nonzero!(3u32))
                .dead_letter(DEAD_LETTER);
        },
    );

    let err = TestApp::start(app)
        .await
        .expect_err("a cap Pub/Sub does not accept must not start");
    let reported = format!("{err:#}");
    assert!(
        reported.contains("max_attempts(3)"),
        "the refusal must name the cap: {reported}",
    );
}
