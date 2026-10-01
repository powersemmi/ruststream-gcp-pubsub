//! Conformance: every suite this crate's surface justifies, run twice over the same production
//! broker - connected in process, and connected to the Pub/Sub emulator (gated behind
//! `PUBSUB_TEST_HOST`).
//!
//! The pairing is the point. The in-process leg holds the in-process mode to the framework's own
//! definition of a broker, over the same descriptors and policies the emulator leg uses, and it
//! runs on every `cargo test`; the emulator leg is what proves the in-process mode is not passing
//! by lying.
//!
//! Which suites those are follows from what the crate implements: the routing suite and the
//! lifecycle everywhere, [`capabilities::batches`] because the subscriber is a
//! `BatchSubscriber`, and the credential scan because the broker describes itself. Request/reply,
//! transactions and seeking have no impl here, so their suites have nothing to run against;
//! `Partitioned` is covered by the integration tests, the framework shipping no suite for it.
//! `harness::redelivery_address` has nothing to check either: a Pub/Sub subscription moves a
//! spent delivery itself, so no descriptor here publishes a copy and none reports an address.
//!
//! Start the emulator with `just brokers-up`, then:
//! `PUBSUB_TEST_HOST=127.0.0.1:8085 cargo test --all-features`.

#![cfg(feature = "testing")]

use ruststream::Name;
use ruststream::conformance::harness::InProcessBroker;
use ruststream::conformance::{capabilities, harness};
use ruststream_gcp_pubsub::{GooglePubSub, PubSubBroker};

mod live;

const TEST_PROJECT: &str = "ruststream-test";

fn test_host() -> Option<String> {
    live::host("PUBSUB_TEST_HOST")
}

/// The production broker, connected in process by the suites that take any broker.
fn in_process() -> InProcessBroker<PubSubBroker> {
    InProcessBroker::new(PubSubBroker::new(TEST_PROJECT))
}

/// The routing contract over bare names. A subscription the service names without creating is
/// infrastructure the in-process mode takes to be attached to the topic of its own name, which is
/// the name the suite publishes to.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_in_process_mode_passes_conformance_suite() {
    harness::run_suite(|| PubSubBroker::new(TEST_PROJECT)).await;
}

// In every check below `make_source` / `make_publisher` must stay closures: their bounds are
// higher-ranked (`Fn(&str) -> _` / `Fn(&B) -> _`), so a bare method path - which binds one
// concrete lifetime - would not type-check.

/// The ladder contract, in process: synchronous construction, the consuming `connect`, a
/// subscription opened through the descriptor, publish, receive, ack, the consuming `shutdown` -
/// and, last, that a publisher which outlived the shutdown errors rather than routing into a
/// transport that is gone. The descriptor is the one the emulator leg below opens.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_passes_lifecycle() {
    harness::lifecycle(
        in_process,
        |name| GooglePubSub::new(name).create_with_topic(name),
        |connected| connected.publisher(),
    )
    .await;
}

/// The same ladder over the bare-string form, which resolves through `Subscribe` rather than
/// through the crate's descriptor.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_passes_lifecycle_by_name() {
    harness::lifecycle(
        in_process,
        |name| Name::new(name.to_owned()),
        |connected| connected.publisher(),
    )
    .await;
}

/// The in-process mode batches the way a streaming pull does, over the same buffer, so it owes the
/// same contract: a batch never carries more than the size the subscription was opened with.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_honours_the_batch_size() {
    capabilities::batches(
        in_process,
        |name| GooglePubSub::new(name).create_with_topic(name),
        |connected| connected.publisher(),
    )
    .await;
}

/// The same ladder against the product, where `connect` authenticates, the subscription is a
/// streaming pull, and the publisher's connection cell is what goes dead on shutdown.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pubsub_broker_passes_lifecycle() {
    let Some(host) = test_host() else { return };
    harness::lifecycle(
        || PubSubBroker::new(TEST_PROJECT).emulator(host.clone()),
        |name| GooglePubSub::new(name).create_with_topic(name),
        |connected| connected.publisher(),
    )
    .await;
}

/// The document a service publishes is shared, so a password written into an endpoint must not
/// reach it - neither the server description nor a binding body.
#[cfg(feature = "asyncapi")]
#[test]
fn pubsub_broker_describes_itself_without_credentials() {
    harness::describes_without_credentials(
        &PubSubBroker::new(TEST_PROJECT).endpoint("https://admin:hunter2@pubsub.example.com:443"),
        &GooglePubSub::new("orders-workers"),
        "hunter2",
    );
}

/// The same contract against the product itself, where the deliveries the buffer batches come
/// off a real streaming pull.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pubsub_broker_honours_the_batch_size() {
    let Some(host) = test_host() else { return };
    capabilities::batches(
        || PubSubBroker::new(TEST_PROJECT).emulator(host.clone()),
        |name| GooglePubSub::new(name).create_with_topic(name),
        |connected| connected.publisher(),
    )
    .await;
}
