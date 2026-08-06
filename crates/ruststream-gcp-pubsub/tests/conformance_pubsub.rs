//! Conformance: the routing suite against the in-process transport, and the lifecycle check
//! against the Pub/Sub emulator (gated behind `PUBSUB_TEST_HOST`).
//!
//! Start the emulator with `just brokers-up`, then:
//! `PUBSUB_TEST_HOST=127.0.0.1:8085 cargo test --all-features`.

#![cfg(feature = "testing")]

use ruststream::conformance::harness;
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

// `make_source` / `make_publisher` must stay closures: their bounds are higher-ranked
// (`Fn(&str) -> _` / `Fn(&B) -> _`), so a bare method path - which binds one concrete lifetime -
// would not type-check.
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
