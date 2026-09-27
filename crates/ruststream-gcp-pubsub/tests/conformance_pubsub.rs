//! Conformance: every suite this crate's surface justifies, run twice over the same production
//! broker - connected in process, and connected to the Pub/Sub emulator (gated behind
//! `PUBSUB_TEST_HOST`).
//!
//! The pairing is the point. The in-process leg holds the in-process mode to the framework's own
//! definition of a broker, over the same descriptors and policies the emulator leg uses, and it
//! runs on every `cargo test`; the emulator leg is what proves the in-process mode is not passing
//! by lying, and the suites that compare the two transports run there.
//!
//! Which suites those are follows from what the crate implements: a shutdown that finishes what
//! was handed to it; the settlement meanings; [`capabilities::batches`] because the subscriber
//! is a `BatchSubscriber`; [`retry::broker_moves`] because a Pub/Sub subscription moves a spent
//! delivery itself, so both the descriptor and a bare name declare `Copies = BrokerMoves`; the
//! keyed order and the per-message options because a publish carries an ordering key; and the
//! credential scan because the broker describes itself.
//!
//! A bare subscription name is infrastructure the service expects to exist, so the suites that
//! open one run in process, where such a subscription is taken to be attached to the topic of its
//! own name; against the emulator the descriptor that creates its topology stands in.
//!
//! Start the emulator with `just brokers-up`, then:
//! `PUBSUB_TEST_HOST=127.0.0.1:8085 cargo test --all-features`.

#![cfg(feature = "testing")]

use std::num::NonZeroU32;
use std::time::Duration;

use ruststream::conformance::harness::InProcessBroker;
use ruststream::conformance::helpers::unique_subject;
use ruststream::conformance::in_process::Refusal;
use ruststream::conformance::message_shape::{self, OptionCases};
use ruststream::conformance::{capabilities, harness, in_process, lifecycle, retry, settlement};
use ruststream::testing::Backlog;
use ruststream::{HeaderMap, IncomingMessage, Name, nonzero};
use ruststream_gcp_pubsub::{
    GooglePubSub, PubSubBroker, PubSubMessage, PubSubPublish, PubSubPublishOptions,
};

mod live;

const TEST_PROJECT: &str = "ruststream-test";

/// The cap the retry check declares: the smallest a Pub/Sub dead-letter policy accepts.
const ATTEMPTS: NonZeroU32 = nonzero!(5u32);

/// The longest ordering key the service accepts is 1024 bytes, so one past it is refused.
const OVERLONG_KEY: usize = 1025;

/// How long the settlement suite waits for a delivery the broker must not redeliver. Nothing here
/// sets an ack deadline, so a message left unsettled comes back once its lease lapses; the wait
/// covers the ten seconds the service gives a subscription by default.
const REDELIVERY_TIMEOUT: Duration = Duration::from_secs(12);

fn test_host() -> Option<String> {
    live::host("PUBSUB_TEST_HOST")
}

/// The production broker, connected in process by the suites that take any broker.
fn in_process() -> InProcessBroker<PubSubBroker> {
    InProcessBroker::new(PubSubBroker::new(TEST_PROJECT))
}

/// The descriptor every suite opens: the subscription `name`, created on the topic of the same
/// name, so a publish to `name` reaches it on either transport.
fn created(name: &str) -> GooglePubSub {
    GooglePubSub::new(name).create_with_topic(name)
}

/// The ordering key a delivery reports, as text.
fn ordering_key(delivery: &PubSubMessage) -> Option<String> {
    delivery
        .partition_key()
        .map(|key| String::from_utf8_lossy(key).into_owned())
}

/// The key cases the options check publishes: the policy's key and a call's own key.
fn key_cases() -> OptionCases<PubSubPublishOptions, Option<String>> {
    OptionCases::new(Some("policy-key".to_owned())).overrides(
        PubSubPublishOptions {
            ordering_key: Some("call-key".to_owned()),
        },
        Some("call-key".to_owned()),
    )
}

/// The key cases plus a key past the service's limit, which the service refuses at publish time.
fn key_cases_with_refusal() -> OptionCases<PubSubPublishOptions, Option<String>> {
    key_cases().refuses(PubSubPublishOptions {
        ordering_key: Some("k".repeat(OVERLONG_KEY)),
    })
}

/// A keyed publish names its key through the options, the native spelling.
#[allow(clippy::unnecessary_wraps)] // the signature is the check's: a broker may key by header
fn keyed(key: &[u8], _headers: &mut HeaderMap) -> Option<PubSubPublishOptions> {
    Some(PubSubPublishOptions {
        ordering_key: Some(String::from_utf8_lossy(key).into_owned()),
    })
}

// In every check below `make_source` / `make_publisher` must stay closures: their bounds are
// higher-ranked (`Fn(&str) -> _` / `Fn(&B) -> _`), so a bare method path - which binds one
// concrete lifetime - would not type-check.

/// The in-process mode batches the way a streaming pull does, over the same buffer, so it owes the
/// same contract: a batch never carries more than the size the subscription was opened with.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_honours_the_batch_size() {
    capabilities::batches(
        in_process,
        |name| created(name),
        |connected| connected.publisher(),
    )
    .await;
}

/// The dead-letter policy a registration declares, applied in process over the descriptor.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_moves_a_spent_delivery() {
    retry::broker_moves(
        in_process,
        |name| created(name),
        |connected| connected.publisher(),
        ATTEMPTS,
    )
    .await;
}

/// The same policy declared on a bare name, which carries it through `Subscribe::declare_retry`.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_moves_a_spent_delivery_by_name() {
    retry::broker_moves(
        in_process,
        |name| Name::new(name.to_owned()),
        |connected| connected.publisher(),
        ATTEMPTS,
    )
    .await;
}

/// A keyed message reports its ordering key on delivery, and one key keeps its order.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_keeps_keyed_order() {
    message_shape::keyed_order(
        in_process,
        &unique_subject("conformance-keyed"),
        |name| created(name),
        |connected| connected.publisher(),
        keyed,
    )
    .await;
}

/// The ordering key resolves over the policy's, and a key past the service's limit fails the
/// publish.
#[allow(clippy::redundant_closure)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_resolves_publish_options() {
    message_shape::publish_options(
        in_process,
        &unique_subject("conformance-options"),
        |name| created(name),
        PubSubPublish::default().ordering_key("policy-key"),
        key_cases_with_refusal(),
        ordering_key,
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

/// An acknowledgement and a publish made right before `shutdown` are finished by it. The
/// subscription is a resource that outlives the connection and keeps what reaches its topic, so
/// the second connection reads the same subscription after the shutdown.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pubsub_broker_flushes_on_shutdown() {
    let Some(host) = test_host() else { return };
    lifecycle::shutdown_flushes(
        move || PubSubBroker::new(TEST_PROJECT).emulator(host.clone()),
        |name| created(name),
        |connected| connected.publisher(),
        Backlog::Delivered,
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
        |name| created(name),
        |connected| connected.publisher(),
    )
    .await;
}

/// Every settlement means the same against the emulator and in process: an in-process answer the
/// service would not give passes a handler's retry in a test and loses the message in production.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn settlements_match_the_emulator() {
    let Some(host) = test_host() else { return };
    settlement::matches_in_process(
        || PubSubBroker::new(TEST_PROJECT).emulator(host.clone()),
        |name| created(name),
        |connected| connected.publisher(),
        REDELIVERY_TIMEOUT,
    )
    .await;
}

/// The dead-letter policy a registration declares, applied by the service itself.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pubsub_broker_moves_a_spent_delivery() {
    let Some(host) = test_host() else { return };
    retry::broker_moves(
        || PubSubBroker::new(TEST_PROJECT).emulator(host.clone()),
        |name| created(name),
        |connected| connected.publisher(),
        ATTEMPTS,
    )
    .await;
}

/// What the emulator refuses, the in-process mode refuses too.
#[allow(clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refusals_match_the_emulator() {
    let Some(host) = test_host() else { return };
    let topic = unique_subject("conformance-refusals");
    in_process::refuses_like_the_server(
        || PubSubBroker::new(TEST_PROJECT).emulator(host.clone()),
        |connected| connected.publisher(),
        [
            Refusal::Publish {
                name: "goog-reserved".to_owned(),
            },
            Refusal::Subscription {
                source: GooglePubSub::new("goog-reserved").create_with_topic(&topic),
            },
        ],
    )
    .await;
}

/// A keyed message reports its ordering key on a real subscription, and one key keeps its order
/// on a subscription that enables message ordering.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pubsub_broker_keeps_keyed_order() {
    let Some(host) = test_host() else { return };
    message_shape::keyed_order(
        || PubSubBroker::new(TEST_PROJECT).emulator(host.clone()),
        &unique_subject("conformance-keyed"),
        |name| created(name),
        |connected| connected.publisher(),
        keyed,
    )
    .await;
}

/// The ordering key resolves over the policy's against the emulator. The emulator takes an
/// ordering key of any length, so the key past the service's limit is refused only in process.
#[allow(clippy::redundant_closure)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pubsub_broker_resolves_publish_options() {
    let Some(host) = test_host() else { return };
    message_shape::publish_options(
        || PubSubBroker::new(TEST_PROJECT).emulator(host.clone()),
        &unique_subject("conformance-options"),
        |name| created(name),
        PubSubPublish::default().ordering_key("policy-key"),
        key_cases(),
        ordering_key,
    )
    .await;
}
