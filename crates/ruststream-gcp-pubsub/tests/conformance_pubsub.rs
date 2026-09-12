//! Conformance: every suite this crate's surface justifies, run twice - against the in-process
//! stand-in, and against the Pub/Sub emulator (gated behind `PUBSUB_TEST_HOST`).
//!
//! The pairing is the point. The in-process leg holds the stand-in to the framework's own
//! definition of a broker rather than to a bespoke test of what it happens to do, and it runs on
//! every `cargo test`; the emulator leg is what proves the stand-in is not passing by lying.
//!
//! Which suites those are follows from what the crate implements: the routing suite and the
//! lifecycle everywhere, [`capabilities::batches`] because both subscribers are
//! `BatchSubscriber`. Request/reply, transactions and seeking have no impl here, so their suites
//! have nothing to run against; `Partitioned` is covered by the integration tests, the framework
//! shipping no suite for it.
//!
//! Start the emulator with `just brokers-up`, then:
//! `PUBSUB_TEST_HOST=127.0.0.1:8085 cargo test --all-features`.

#![cfg(feature = "testing")]

use ruststream::conformance::{capabilities, harness};
use ruststream_gcp_pubsub::testing::PubSubTestBroker;
use ruststream_gcp_pubsub::{GooglePubSub, PubSubBroker};

mod live;

const TEST_PROJECT: &str = "ruststream-test";

fn test_host() -> Option<String> {
    live::host("PUBSUB_TEST_HOST")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pubsub_test_broker_passes_conformance_suite() {
    harness::run_suite(PubSubTestBroker::new).await;
}

// In every check below `make_source` / `make_publisher` must stay closures: their bounds are
// higher-ranked (`Fn(&str) -> _` / `Fn(&B) -> _`), so a bare method path - which binds one
// concrete lifetime - would not type-check.

/// The ladder contract, in process: synchronous construction, the consuming `connect`, a
/// subscription opened through the descriptor, publish, receive, ack, the consuming `shutdown` -
/// and, last, that a publisher which outlived the shutdown errors rather than routing into a
/// transport that is gone. The stand-in follows the same ladder, so it owes the same answers.
///
/// The descriptor names no topic here, unlike the emulator leg below: the stand-in has no
/// resources to create, and routes by the subscription name.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pubsub_test_broker_passes_lifecycle() {
    harness::lifecycle(
        PubSubTestBroker::new,
        |name| GooglePubSub::new(name),
        |connected| connected.publisher(),
    )
    .await;
}

/// The stand-in batches the way the real subscriber does, so it owes the same contract: a batch
/// never carries more than the size the subscription was opened with.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pubsub_test_broker_honours_the_batch_size() {
    capabilities::batches(
        PubSubTestBroker::new,
        |name| GooglePubSub::new(name),
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
