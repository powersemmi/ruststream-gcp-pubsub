//! What a subscription looks like on the service after the crate has opened it, gated behind
//! `PUBSUB_TEST_HOST`.
//!
//! The other live suite reads the topology through behaviour: publish a message, spend its
//! deliveries, watch the copy arrive on the dead-letter topic. That proves the service does
//! something, not that it does what the registration declared - a cap of its own and a
//! destination of its own would look the same from here. This suite therefore asks the admin API
//! what the subscription is configured with, which is the only place the declaration can be read
//! back as a declaration.
//!
//! Start the emulator with `just brokers-up`, then:
//! `PUBSUB_TEST_HOST=127.0.0.1:8085 cargo test --all-features -- --test-threads=1`.

use std::num::NonZeroU32;

use google_cloud_auth::credentials::anonymous;
use google_cloud_pubsub::client::{SubscriptionAdmin, TopicAdmin};
use google_cloud_pubsub::model::{DeadLetterPolicy, Subscription};
use google_cloud_wkt::FieldMask;
use ruststream::{
    Broker, ConnectedBroker, RetryDeclaration, Subscribe, SubscriptionSource, nonzero,
};
use ruststream_gcp_pubsub::{ConnectedPubSubBroker, GooglePubSub, PubSubBroker};

mod live;

const TEST_PROJECT: &str = "ruststream-test";

/// The cap these cases declare. Five is the smallest a Pub/Sub dead-letter policy accepts.
const MAX_ATTEMPTS: u32 = 5;

fn test_host() -> Option<String> {
    live::host("PUBSUB_TEST_HOST")
}

async fn connect(host: &str) -> ConnectedPubSubBroker {
    PubSubBroker::new(TEST_PROJECT)
        .emulator(host)
        .connect()
        .await
        .expect("broker connects")
}

/// The admin API of the same emulator: where the service states what a subscription carries,
/// rather than what a delivery through it happened to do.
async fn subscriptions(host: &str) -> SubscriptionAdmin {
    SubscriptionAdmin::builder()
        .with_endpoint(format!("http://{host}"))
        .with_credentials(anonymous::Builder::new().build())
        .build()
        .await
        .expect("the admin client builds")
}

/// Creates `topic`, which a dead-letter policy the service accepts has to name.
async fn create_topic(host: &str, topic: &str) {
    TopicAdmin::builder()
        .with_endpoint(format!("http://{host}"))
        .with_credentials(anonymous::Builder::new().build())
        .build()
        .await
        .expect("the admin client builds")
        .create_topic()
        .set_name(topic_name(topic))
        .send()
        .await
        .expect("the topic is created");
}

/// Per-test unique name, so runs do not observe each other's leftovers.
fn unique(name: &str) -> String {
    format!("tp-{name}-{}", std::process::id())
}

fn subscription_name(name: &str) -> String {
    format!("projects/{TEST_PROJECT}/subscriptions/{name}")
}

fn topic_name(name: &str) -> String {
    format!("projects/{TEST_PROJECT}/topics/{name}")
}

/// What a mount site declares in every case here.
fn declared_retries(dead_letter: &str, attempts: u32) -> RetryDeclaration {
    RetryDeclaration::new()
        .with_max_attempts(NonZeroU32::new(attempts).expect("a declared cap is non-zero"))
        .with_dead_letter(dead_letter.to_owned())
}

/// What the service says subscription `name` is configured with.
async fn configuration(host: &str, name: &str) -> Subscription {
    subscriptions(host)
        .await
        .get_subscription()
        .set_subscription(subscription_name(name))
        .send()
        .await
        .expect("the subscription is there")
}

/// The dead-letter policy as the service reports it: the topic a spent delivery goes to and how
/// many deliveries one message gets.
fn dead_letter_policy(subscription: &Subscription) -> Option<(String, i32)> {
    subscription.dead_letter_policy.as_ref().map(|policy| {
        (
            policy.dead_letter_topic.clone(),
            policy.max_delivery_attempts,
        )
    })
}

/// A registration's declaration becomes the subscription's own dead-letter policy, in the
/// service's words: the topic it names and the cap it asked for, both on the resource.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_created_subscription_carries_the_declared_policy() {
    let Some(host) = test_host() else { return };
    let connected = connect(&host).await;

    let topic = unique("created-topic");
    let workers = unique("created-workers");
    let dead_letter = unique("created-dead");

    SubscriptionSource::<ConnectedPubSubBroker>::declare_retry(
        GooglePubSub::new(&workers).create_with_topic(&topic),
        &declared_retries(&dead_letter, MAX_ATTEMPTS),
    )
    .subscribe(&connected)
    .await
    .expect("the subscription opens with the declared dead-letter policy");

    let configured = configuration(&host, &workers).await;
    assert_eq!(
        dead_letter_policy(&configured),
        Some((
            topic_name(&dead_letter),
            i32::try_from(MAX_ATTEMPTS).expect("a cap fits the wire type")
        )),
    );
    assert_eq!(configured.topic, topic_name(&topic));

    connected.shutdown().await.expect("shutdown succeeds");
}

/// A subscription managed as infrastructure exists before the service starts, so the declaration
/// reaches it as an update. The resource carries the same policy either way.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_existing_subscription_carries_the_declaration_as_an_update() {
    let Some(host) = test_host() else { return };
    let connected = connect(&host).await;

    let topic = unique("update-topic");
    let workers = unique("update-workers");
    let dead_letter = unique("update-dead");
    create_topic(&host, &dead_letter).await;

    connected
        .subscribe_descriptor(GooglePubSub::new(&workers).create_with_topic(&topic))
        .await
        .expect("the subscription is created without a policy");
    assert_eq!(
        dead_letter_policy(&configuration(&host, &workers).await),
        None,
        "nothing was declared yet, so the service counts nothing",
    );

    SubscriptionSource::<ConnectedPubSubBroker>::declare_retry(
        GooglePubSub::new(&workers).create_with_topic(&topic),
        &declared_retries(&dead_letter, MAX_ATTEMPTS),
    )
    .subscribe(&connected)
    .await
    .expect("the declaration reaches the subscription that is already there");

    assert_eq!(
        dead_letter_policy(&configuration(&host, &workers).await),
        Some((
            topic_name(&dead_letter),
            i32::try_from(MAX_ATTEMPTS).expect("a cap fits the wire type")
        )),
    );

    connected.shutdown().await.expect("shutdown succeeds");
}

/// A handler mounted by a plain subscription name declares its retries at the mount site like any
/// other, and the policy lands on the resource that name identifies.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bare_names_declaration_reaches_the_subscription() {
    let Some(host) = test_host() else { return };
    let connected = connect(&host).await;

    let topic = unique("byname-topic");
    let workers = unique("byname-workers");
    let dead_letter = unique("byname-dead");
    create_topic(&host, &dead_letter).await;

    connected
        .subscribe_descriptor(GooglePubSub::new(&workers).create_with_topic(&topic))
        .await
        .expect("the subscription exists before the service starts");

    connected
        .declare_retry(&workers, &declared_retries(&dead_letter, MAX_ATTEMPTS))
        .expect("the broker takes the declaration for this name");
    connected
        .subscribe(&workers)
        .await
        .expect("the subscription opens with the declared dead-letter policy");

    assert_eq!(
        dead_letter_policy(&configuration(&host, &workers).await),
        Some((
            topic_name(&dead_letter),
            i32::try_from(MAX_ATTEMPTS).expect("a cap fits the wire type")
        )),
    );

    connected.shutdown().await.expect("shutdown succeeds");
}

/// A registration that declared nothing leaves the subscription's own topology alone: no policy
/// on the resource, so the service counts no deliveries and moves nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_subscription_nothing_declared_for_carries_no_policy() {
    let Some(host) = test_host() else { return };
    let connected = connect(&host).await;

    let name = unique("nopolicy");
    connected
        .subscribe_descriptor(GooglePubSub::new(&name).create_with_topic(&name))
        .await
        .expect("the subscription opens");

    assert_eq!(dead_letter_policy(&configuration(&host, &name).await), None);

    connected.shutdown().await.expect("shutdown succeeds");
}

/// An ordering key orders deliveries only where the subscription says so, and the subscription
/// this descriptor creates says so. The emulator hands a keyed run over in publish order either
/// way, so the field on the resource is what the assertion reads.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_created_subscription_orders_the_deliveries_of_one_key() {
    let Some(host) = test_host() else { return };
    let connected = connect(&host).await;

    let name = unique("ordered");
    connected
        .subscribe_descriptor(GooglePubSub::new(&name).create_with_topic(&name))
        .await
        .expect("the subscription opens");

    assert!(
        configuration(&host, &name).await.enable_message_ordering,
        "a keyed publish through this crate has to reach the handler in publish order",
    );

    connected.shutdown().await.expect("shutdown succeeds");
}

/// The cap the crate refuses before any I/O never reaches the service, so the run stops with
/// nothing created rather than with a subscription that honours neither half of the declaration.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cap_outside_the_services_range_never_reaches_the_service() {
    let Some(host) = test_host() else { return };
    let connected = connect(&host).await;

    let name = unique("badcap");
    let err = SubscriptionSource::<ConnectedPubSubBroker>::declare_retry(
        GooglePubSub::new(&name).create_with_topic(&name),
        &declared_retries(&name, 3),
    )
    .subscribe(&connected)
    .await
    .expect_err("a cap the service would refuse is refused here first");
    assert!(
        err.to_string().contains("outside the 5..=100"),
        "the refusal must name the range it held the declaration to: {err}",
    );

    assert!(
        subscriptions(&host)
            .await
            .get_subscription()
            .set_subscription(subscription_name(&name))
            .send()
            .await
            .is_err(),
        "the refusal happens before any I/O, so nothing was created",
    );

    connected.shutdown().await.expect("shutdown succeeds");
}

/// The range the crate holds a declaration to is the service's own, not a number of its choosing:
/// the API refuses either side of it and accepts both ends.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_range_the_crate_refuses_against_is_the_services_own() {
    let Some(host) = test_host() else { return };
    let connected = connect(&host).await;

    let name = unique("range");
    let dead_letter = unique("range-dead");
    create_topic(&host, &dead_letter).await;
    connected
        .subscribe_descriptor(GooglePubSub::new(&name).create_with_topic(&name))
        .await
        .expect("the subscription opens");

    let admin = subscriptions(&host).await;
    for (attempts, accepted) in [(4, false), (5, true), (100, true), (101, false)] {
        let written = admin
            .update_subscription()
            .set_subscription(
                Subscription::new()
                    .set_name(subscription_name(&name))
                    .set_dead_letter_policy(
                        DeadLetterPolicy::new()
                            .set_dead_letter_topic(topic_name(&dead_letter))
                            .set_max_delivery_attempts(attempts),
                    ),
            )
            .set_update_mask(FieldMask::default().set_paths(["dead_letter_policy"]))
            .send()
            .await;
        assert_eq!(
            written.is_ok(),
            accepted,
            "the service must {} max_delivery_attempts({attempts})",
            if accepted { "accept" } else { "refuse" },
        );
    }

    connected.shutdown().await.expect("shutdown succeeds");
}

/// One subscription carries one dead-letter policy, so a second registration declaring something
/// else refuses to start - and the refusal leaves the policy the first one asked for on the
/// resource, rather than the last writer's.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_contradicting_declaration_leaves_the_first_policy_in_place() {
    let Some(host) = test_host() else { return };
    let connected = connect(&host).await;

    let workers = unique("twice-workers");
    let dead_letter = unique("twice-dead");
    create_topic(&host, &dead_letter).await;
    connected
        .subscribe_descriptor(GooglePubSub::new(&workers).create_with_topic(&workers))
        .await
        .expect("the subscription exists before the service starts");

    connected
        .declare_retry(&workers, &declared_retries(&dead_letter, MAX_ATTEMPTS))
        .expect("the first registration's declaration is taken");
    let err = connected
        .declare_retry(&workers, &declared_retries(&dead_letter, 10))
        .expect_err("a second registration declaring something else must refuse to start");
    assert!(
        err.to_string().contains("mounted twice"),
        "the refusal must say what it refused: {err}",
    );

    connected
        .subscribe(&workers)
        .await
        .expect("the subscription opens with the declaration that was taken");
    assert_eq!(
        dead_letter_policy(&configuration(&host, &workers).await),
        Some((
            topic_name(&dead_letter),
            i32::try_from(MAX_ATTEMPTS).expect("a cap fits the wire type")
        )),
    );

    connected.shutdown().await.expect("shutdown succeeds");
}

/// Half a declaration cannot become a dead-letter policy, and the run stops before it opens a
/// subscription that honours neither half.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn half_a_declaration_never_reaches_the_service() {
    let Some(host) = test_host() else { return };
    let connected = connect(&host).await;

    for (case, declaration) in [
        (
            "cap-only",
            RetryDeclaration::new().with_max_attempts(nonzero!(MAX_ATTEMPTS)),
        ),
        (
            "destination-only",
            RetryDeclaration::new().with_dead_letter(unique("half-dead")),
        ),
    ] {
        let name = unique(&format!("half-{case}"));
        let err = SubscriptionSource::<ConnectedPubSubBroker>::declare_retry(
            GooglePubSub::new(&name).create_with_topic(&name),
            &declaration,
        )
        .subscribe(&connected)
        .await
        .expect_err("half a declaration must be refused");
        assert!(
            err.to_string().contains("dead-letter policy needs"),
            "the refusal must name the half that is missing: {err}",
        );
        assert!(
            subscriptions(&host)
                .await
                .get_subscription()
                .set_subscription(subscription_name(&name))
                .send()
                .await
                .is_err(),
            "the refusal happens before any I/O, so nothing was created",
        );
    }

    connected.shutdown().await.expect("shutdown succeeds");
}
