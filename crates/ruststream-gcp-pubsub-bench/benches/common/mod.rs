//! Shared parts of the crate's code-cost benchmarks: what is measured, and how the measurement is
//! kept to one region.
//!
//! # What a scenario looks like
//!
//! One scenario per file, and this module carries what they have in common: the payload, the
//! topology, the service setup, the fill, the latch a handler counts deliveries down on, and the
//! measurement configuration. The method is the core's, described in its `benches/common` and on
//! the [RustStream benchmarks page](https://powersemmi.github.io/ruststream/latest/benchmarks/).
//!
//! A scenario runs the service a user writes: the app, built on [`PubSubBroker`] with the
//! constructor a user writes and started through [`RustStream::start`], against the Pub/Sub
//! emulator of the compose stand. The service runs on a single-threaded tokio runtime, so all of
//! its work happens on one thread: the framework's dispatch, this crate's code, and the
//! `google-cloud-pubsub` client, whose streaming pull, lease handling, acknowledgements and
//! publishing run as tasks on the runtime they were created on. The emulator is another process,
//! and nothing it does is in the number.
//!
//! # Steady state and cold start
//!
//! Every scenario is measured over one delivery, over [`MESSAGES`] deliveries and over twice as
//! many. The slope between the last two is the steady-state cost of a message: everything that
//! happens once is in both totals and cancels in the subtraction. The one-delivery run is the
//! cold start, reported on its own.
//!
//! What a body measures is the start and the drain, in two regions. The topology is created
//! before the first and the queue is filled between them, and neither is counted. The fill runs
//! on a thread of its own, with a runtime of its own, through the client's publisher, and returns
//! once the emulator has confirmed every message. The service's runtime is not polled while it
//! runs, so nothing is consumed before the drain. The client opens the streaming pull on its first
//! poll, so the connection to the emulator is made inside the drain region, once per run.
//!
//! # What is counted
//!
//! Collection starts switched off and is switched on for [`measure`], which every body wraps its
//! work in. Everything on the service's thread inside the region is counted: the dispatcher, the
//! codec, this crate's code, the client library's work, and tokio's share of driving them. Work
//! on another thread is not. [`measure`] is the only frame that carries its name, because a
//! toggle on a name that also appears inside closure types switches collection off again one
//! frame deeper. DHAT is pointed at the same frame; the number read is `Total blocks`,
//! allocations per run.

// Each benchmark target compiles this module on its own and uses the part it needs; what another
// target uses looks unused here.
#![allow(dead_code)]

use std::convert::Infallible;
use std::env;
use std::future::Future;
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use google_cloud_auth::credentials::{Credentials, anonymous};
use google_cloud_pubsub::client::{BasePublisher, SubscriptionAdmin, TopicAdmin};
use google_cloud_pubsub::model::Message as GcpMessage;
use gungraun::{Callgrind, Dhat, DhatMetric, EntryPoint, EventKind, LibraryBenchmarkConfig};
use ruststream::runtime::{AppInfo, BrokerScope, Identity, RunningApp, RustStream};
use ruststream_gcp_pubsub::PubSubBroker;
use serde::Deserialize;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::Notify;
use tokio::time::timeout;

// The code is counted as it ships. The `testing` feature compiles the in-process mode into this
// crate and turns on the framework's harness, and neither is in a deployed service.
#[cfg(feature = "testing")]
compile_error!(
    "benchmarks must be built without the `testing` feature; run them through `just bench-code`"
);

/// The variable naming the emulator; `just bench-code` sets it.
const HOST: &str = "PUBSUB_TEST_HOST";
/// The project the emulator serves, which is the one `docker-compose.test.yml` starts it with.
const PROJECT: &str = "ruststream-test";
/// The topic the fill publishes to.
const TOPIC: &str = "orders";
/// The subscription every scenario consumes. A handler names it in its own
/// `GooglePubSub::new(..)` descriptor, which the decorator takes as written.
pub const SUBSCRIPTION: &str = "orders-workers";
/// The topic a reply goes to: a scenario's reply type names it in its own `#[outgoing(..)]`.
const REPLIES: &str = "confirmations";

/// How long the emulator lets a delivery go unsettled before it sends it again.
///
/// The longest the API accepts. A run under valgrind is many times slower than the service it
/// counts, and a redelivery would be a message the scenario did not publish.
const ACK_DEADLINE_SECONDS: i32 = 600;
/// How long a drain may go without a delivery before the run is called stuck.
const STALL: Duration = Duration::from_secs(120);

/// The values every body carries. Fixed, so that every delivery of a run costs the same.
const ID: u64 = 1_000_000;
const QUANTITY: u32 = 37;

/// The payload every scenario decodes: two integer fields, so a decode allocates nothing and the
/// number is about the crate and the framework rather than about `serde_json`'s string handling.
#[derive(Debug, Deserialize)]
pub struct Order {
    pub id: u64,
    pub quantity: u32,
}

/// Deliveries per measured run: large enough that entering and leaving the region is lost in the
/// per-message number, small enough that a scenario stays within two minutes of valgrind time.
/// `scripts/bench_results.py` divides by the same count.
pub const MESSAGES: usize = 1_000;

/// The measurement configuration every gated scenario shares.
///
/// `steady` is what one delivery allocates in the steady state and `cold` what starting the
/// service and taking the first delivery allocate once; together they are the hard limit the
/// longest run of the scenario (twice [`MESSAGES`] deliveries) is held to, so the run fails when
/// the path allocates more than it does today. The client acknowledges in batches and extends
/// leases on timers, so a run's count moves a little on an unchanged tree: the limit is the
/// highest count seen over repeated runs plus a margin at least as large as the spread seen, and
/// a tenth of a percent at the least. One allocation more per delivery adds `2 * MESSAGES` to
/// the run, more than any margin here, so the gate still catches it. A number that goes down is
/// lowered here in the same change. The instruction totals moved by at most half a percent, a
/// quarter of the relative limit: `just bench-code --save-baseline=main` records a baseline and
/// `just bench-code --baseline=main` fails on two percent more.
pub fn config(steady: u64, cold: u64) -> LibraryBenchmarkConfig {
    config_every(steady, 1, cold)
}

/// The same for a scenario whose allocations do not come one per delivery: `steady` blocks per
/// `per` deliveries, as a batch handler allocates per batch.
pub fn config_every(steady: u64, per: u64, cold: u64) -> LibraryBenchmarkConfig {
    let mut config = LibraryBenchmarkConfig::default();
    config
        .pass_through_env(HOST)
        .tool(callgrind().soft_limits([(EventKind::Ir, 2f64)]))
        .tool(dhat().hard_limits([(DhatMetric::TotalBlocks, blocks(steady, per, cold))]));
    config
}

/// The limit for the configured count: the cold part once, plus the steady rate over the longest
/// run of the scenario, which is twice [`MESSAGES`]. The division rounds up.
const fn blocks(steady: u64, per: u64, cold: u64) -> u64 {
    cold + (steady * 2 * MESSAGES as u64).div_ceil(per)
}

/// Callgrind collecting inside the measured region alone.
fn callgrind() -> Callgrind {
    let mut callgrind = Callgrind::with_args([
        "--collect-atstart=no",
        &format!("--toggle-collect={REGION}"),
    ]);
    callgrind.entry_point(EntryPoint::None);
    callgrind
}

/// The measured region: everything this runs is counted, nothing around it is.
#[inline(never)]
pub fn measure<T>(body: impl FnOnce() -> T) -> T {
    // `black_box` runs after the body returns, so the call cannot become a tail jump: DHAT
    // attributes an allocation to this region only while this frame is on the stack.
    black_box(body())
}

/// DHAT with a stack window deep enough to reach the measured frame from a publish inside a
/// dispatched handler.
fn dhat() -> Dhat {
    let mut dhat = Dhat::with_args(["--num-callers=128"]);
    dhat.entry_point(EntryPoint::Custom(REGION.to_owned()));
    dhat
}

/// The frame both tools are pointed at.
const REGION: &str = "*common::measure*";

/// A single-threaded runtime: the service does all of its work on the thread the region is
/// entered on, the client's tasks included.
pub fn runtime() -> Runtime {
    Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current-thread runtime")
}

/// Counts deliveries down and wakes the benchmark body when the last one has been handled.
///
/// Handlers reach it as the application state. What a delivery pays for it is one relaxed
/// decrement and the branch that reads it.
#[derive(Clone, Debug)]
pub struct Latch(Arc<Inner>);

#[derive(Debug)]
struct Inner {
    remaining: AtomicUsize,
    drained: Notify,
}

impl Default for Latch {
    fn default() -> Self {
        Self(Arc::new(Inner {
            remaining: AtomicUsize::new(0),
            drained: Notify::new(),
        }))
    }
}

impl Latch {
    /// Arms the latch for `count` deliveries.
    pub fn expect(&self, count: usize) {
        self.0.remaining.store(count, Ordering::Release);
    }

    /// Records one handled delivery, waking the waiter on the last one.
    pub fn arrived(&self) {
        if self.0.remaining.fetch_sub(1, Ordering::Relaxed) == 1 {
            self.0.drained.notify_one();
        }
    }

    /// How many deliveries the latch is still waiting for.
    pub fn remaining(&self) -> usize {
        self.0.remaining.load(Ordering::Acquire)
    }

    /// Resolves once every expected delivery has been handled.
    pub async fn drained(&self) {
        while self.0.remaining.load(Ordering::Acquire) > 0 {
            self.0.drained.notified().await;
        }
    }
}

/// The JSON body every delivery carries: the two fields a handler reads.
pub fn json_body() -> Vec<u8> {
    format!("{{\"id\":{ID},\"quantity\":{QUANTITY}}}").into_bytes()
}

/// The emulator the stand runs.
fn host() -> String {
    env::var(HOST)
        .unwrap_or_else(|_| panic!("{HOST} names the emulator; `just bench-code` sets it"))
}

/// Runs `work` to completion on a thread of its own, with a runtime of its own, and waits for it.
///
/// What talks to the emulator on the scenario's behalf does it here: its work is never on the
/// service's thread, so it is never in a region, and the service's runtime is not polled while
/// it runs.
fn aside<Work, Done>(work: Work)
where
    Work: FnOnce() -> Done + Send + 'static,
    Done: Future<Output = ()>,
{
    thread::spawn(move || runtime().block_on(work()))
        .join()
        .expect("the work aside finishes");
}

/// Clients against the emulator, built the way [`PubSubBroker::emulator`] builds them: a
/// plaintext endpoint and anonymous credentials.
macro_rules! client {
    ($builder:expr, $host:expr) => {{
        let credentials: Credentials = anonymous::Builder::new().build();
        $builder
            .with_endpoint(format!("http://{}", $host))
            .with_credentials(credentials)
            .build()
            .await
            .expect("the emulator accepts a client")
    }};
}

fn topic_path(topic: &str) -> String {
    format!("projects/{PROJECT}/topics/{topic}")
}

fn subscription_path(subscription: &str) -> String {
    format!("projects/{PROJECT}/subscriptions/{subscription}")
}

/// Recreates the topology every scenario runs on: the topic the fill publishes to, the
/// subscription the service consumes, and the topic replies go to.
///
/// Each run starts from an empty subscription, so what it drains is what it published.
fn topology(host: String) {
    aside(async move || {
        let topics: TopicAdmin = client!(TopicAdmin::builder(), host);
        let subscriptions: SubscriptionAdmin = client!(SubscriptionAdmin::builder(), host);
        // What a run before this one left behind; on a fresh stand there is nothing to delete.
        let _ = subscriptions
            .delete_subscription()
            .set_subscription(subscription_path(SUBSCRIPTION))
            .send()
            .await;
        for topic in [TOPIC, REPLIES] {
            let _ = topics
                .delete_topic()
                .set_topic(topic_path(topic))
                .send()
                .await;
            topics
                .create_topic()
                .set_name(topic_path(topic))
                .send()
                .await
                .expect("the emulator creates the topic");
        }
        subscriptions
            .create_subscription()
            .set_name(subscription_path(SUBSCRIPTION))
            .set_topic(topic_path(TOPIC))
            .set_ack_deadline_seconds(ACK_DEADLINE_SECONDS)
            .send()
            .await
            .expect("the emulator creates the subscription");
    });
}

/// Publishes `count` bodies to [`TOPIC`] through the client's own publisher, and returns once the
/// emulator has confirmed every one of them.
fn fill(host: String, count: usize) {
    aside(async move || {
        let base: BasePublisher = client!(BasePublisher::builder(), host);
        let publisher = base.publisher(topic_path(TOPIC)).build();
        let body = Bytes::from(json_body());
        let mut confirmed: FuturesUnordered<_> = (0..count)
            .map(|_| publisher.publish(GcpMessage::new().set_data(body.clone())))
            .collect();
        while let Some(published) = confirmed.next().await {
            published.expect("the emulator accepts the publish");
        }
    });
}

/// A service that is built but not started, and what its queue will hold.
///
/// The start is part of the measurement rather than of the setup, because the cold number is
/// what starting costs. It is held as a boxed call so that every scenario hands over the same
/// type; the one indirect call it adds lands in the cold number and nowhere else.
pub struct Pending {
    runtime: Runtime,
    latch: Latch,
    host: String,
    start: Box<dyn FnOnce(&Runtime) -> RunningApp>,
    messages: usize,
}

/// The mount a scenario passes in: what `with_broker` does with the scope.
pub type Mount<'a> = &'a mut BrokerScope<PubSubBroker, Identity, (), Latch>;

/// Recreates the topology and builds a one-handler service on the production broker, ready to be
/// started by the body.
pub fn pending(messages: usize, mount: impl FnOnce(Mount<'_>)) -> Pending {
    let host = host();
    topology(host.clone());
    let latch = Latch::default();
    let state = latch.clone();
    let app = RustStream::new(AppInfo::new("bench", "0.0.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(state))
        .with_broker(PubSubBroker::new(PROJECT).emulator(&host), mount);
    Pending {
        runtime: runtime(),
        latch,
        host,
        start: Box::new(move |runtime| runtime.block_on(app.start()).expect("the service starts")),
        messages,
    }
}

/// Waits for the latch, and fails with what it was waiting for if the drain stops moving.
async fn drain(latch: &Latch, messages: usize) {
    let mut remaining = messages;
    while timeout(STALL, latch.drained()).await.is_err() {
        let now = latch.remaining();
        assert!(
            now < remaining,
            "{now} of {messages} deliveries still expected and nothing moved for {STALL:?}"
        );
        remaining = now;
    }
}

/// Starts the service, fills its queue, and drains it: the shape of every scenario here.
///
/// Two measured regions, and the fill between them is in neither. The first is the cold start,
/// the second the deliveries.
pub fn start_and_drain(pending: Pending) {
    let Pending {
        runtime,
        latch,
        host,
        start,
        messages,
    } = pending;
    let running = measure(|| start(&runtime));
    latch.expect(messages);
    fill(host, messages);
    assert_eq!(
        latch.remaining(),
        messages,
        "the queue was consumed while it was being filled, so the measured region would be short"
    );
    measure(|| runtime.block_on(drain(&latch, messages)));
    runtime
        .block_on(running.shutdown())
        .expect("the service stops");
}
