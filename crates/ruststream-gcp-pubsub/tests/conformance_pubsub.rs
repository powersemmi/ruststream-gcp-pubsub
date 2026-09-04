//! Conformance: the routing suite against the in-process transport, and the lifecycle and
//! paging checks against the Pub/Sub emulator (gated behind `PUBSUB_TEST_HOST`).
//!
//! Start the emulator with `just brokers-up`, then:
//! `PUBSUB_TEST_HOST=127.0.0.1:8085 cargo test --all-features`.

#![cfg(feature = "testing")]

use ruststream::Name;
use ruststream::conformance::{capabilities, harness};
use ruststream_gcp_pubsub::testing::PubSubTestBroker;
use ruststream_gcp_pubsub::{PubSubBroker, PubSubSubscription};

const TEST_PROJECT: &str = "ruststream-test";

fn test_host() -> Option<String> {
    match std::env::var("PUBSUB_TEST_HOST") {
        Ok(host) if !host.is_empty() => Some(host),
        _ => {
            eprintln!("PUBSUB_TEST_HOST is not set; skipping the emulator conformance check");
            None
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pubsub_test_broker_passes_conformance_suite() {
    harness::run_suite(PubSubTestBroker::new).await;
}

// In every check below `make_source` / `make_publisher` must stay closures: their bounds are
// higher-ranked (`Fn(&str) -> _` / `Fn(&B) -> _`), so a bare method path - which binds one
// concrete lifetime - would not type-check.

/// The stand-in pages the way the real subscriber does, so it owes the same contract: a page
/// never carries more than the size the subscription was opened with.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pubsub_test_broker_honours_the_page_size() {
    capabilities::batches(
        PubSubTestBroker::new,
        |name| Name::new(name.to_owned()),
        |connected| connected.publisher(),
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pubsub_broker_passes_lifecycle() {
    let Some(host) = test_host() else { return };
    harness::lifecycle(
        || PubSubBroker::new(TEST_PROJECT).emulator(host.clone()),
        |name| PubSubSubscription::new(name).create_with_topic(name),
        |connected| connected.publisher(),
    )
    .await;
}

/// The same contract against the product itself, where the deliveries the buffer pages come off
/// a real streaming pull.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pubsub_broker_honours_the_page_size() {
    let Some(host) = test_host() else { return };
    capabilities::batches(
        || PubSubBroker::new(TEST_PROJECT).emulator(host.clone()),
        |name| PubSubSubscription::new(name).create_with_topic(name),
        |connected| connected.publisher(),
    )
    .await;
}
