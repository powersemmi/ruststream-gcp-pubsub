// The benchmark is a binary of its own, not library surface: a measured loop panics on a broker
// fault rather than threading a `Result` through a scenario nobody recovers from.
#![allow(
    missing_docs,
    unreachable_pub,
    unused_qualifications,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
//! What this crate, and then the runtime on top of it, cost over the `google-cloud-pubsub` client.
//!
//! Two scenarios, each run as three loops that differ in one thing each - what carries the
//! messages:
//!
//! * **raw** drives the client directly: a `MessageStream` and its `Handler`.
//! * **adapter** drives this crate and nothing above it: [`PubSubBroker`], [`GooglePubSub`], the
//!   [`Subscriber`] stream it yields, the [`IncomingMessage`] it delivers and its `ack`, and
//!   [`PubSubPublisher`] on the way in. No handler, no app, no dispatch.
//!   * **framework** is the whole service a user writes: a `#[subscriber]` handler, the app, the
//!     runtime.
//!
//! `adapter` less `raw` is what this crate's own consumer and publisher cost over the client they
//! wrap. `framework` less `adapter` is what the runtime costs on top of them, over this broker in
//! particular - worth a number of its own, because where every adapter is thin and the runtime's
//! share still differs between brokers, the difference lives in how the two meet.
//!
//! Everything else is identical across the three: the endpoint and the credentials, the topology,
//! the streaming-pull settings, the ack position, the decode into the same type, the payload
//! bytes, the tokio runtime and the binary. The procedure the numbers follow is the framework's
//! own, published at <https://powersemmi.github.io/ruststream/latest/benchmarks/>.
//!
//! # What a run is
//!
//! The consumer is attached first, a publisher then feeds it, and the window runs from the first
//! delivery to the end of the last settlement. Connecting, creating the topic and the
//! subscription, and opening the stream are startup cost and sit outside it. Every run creates a
//! fresh topic and a fresh subscription and deletes both afterwards, so a run never sees what the
//! one before it left behind.
//!
//! The message count is not a constant: a probe run measures the raw loop's rate and the count is
//! set from it, so a measured run lasts at least [`SECONDS`] on whatever machine it is taken on.
//!
//! Rounds are interleaved - raw, adapter, framework, raw, adapter, framework - and each loop
//! reports its best round: noise only ever slows a run down, so the fastest round is the closest
//! to the undisturbed cost. Running one loop to the end and then the next would charge every
//! drift of the machine to whichever ran last.
//!
//! # What the numbers do not say
//!
//! The target is the local emulator, which answers the same API over plaintext on the loopback.
//! What that leaves out is everything a hosted subscription adds around a delivery: the TLS
//! handshake, the credential refresh, the regional round trip. The pair stays valid because both
//! halves talk to the same emulator, but the rate a row reports is not what a service reads from
//! a region.
//!
//! Whether the transport paced a run is decided by arithmetic, not by impression. A probe outside
//! both halves times one round trip to the emulator, a delivery is charged one of them for its
//! acknowledgement, and a row is reported broker-bound when that product covers at least half the
//! time one message took. What such a row measures is the emulator, not this crate.

use std::convert::Infallible;
use std::env;
use std::fmt::Write as _;
use std::future::Future;
use std::hint::black_box;
use std::iter::repeat_n;
use std::num::NonZeroUsize;
use std::pin::pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use google_cloud_auth::credentials::{Credentials, anonymous};
use google_cloud_pubsub::client::{
    BasePublisher, Publisher as GcpPublisher, Subscriber as GcpSubscriber, SubscriptionAdmin,
    TopicAdmin,
};
use google_cloud_pubsub::model::Message as GcpMessage;
// `Context` is not imported: the decorator rewrites the handler's signature and emits its own
// path for it, so a `use` here would be an unused import.
use ruststream::runtime::{AppInfo, HandlerOutcome, RunningApp, RustStream};
use ruststream::{
    Broker, ConnectedBroker, IncomingMessage, OutgoingMessage, Publisher, Subscriber,
    SubscriptionSource, subscriber,
};
use ruststream_gcp_pubsub::{GooglePubSub, PubSubBroker, PubSubPublishOptions, PubSubPublisher};
use serde::Deserialize;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::Notify;
use tokio::time::{sleep, timeout};

// A benchmark measures what ships. The `testing` feature compiles an in-process stand-in into this
// crate and turns on the framework's harness, and neither is in a deployed service. Nothing here
// enables it; the feature exists so that asking for it is the compile error below rather than a
// number nobody can trust. The benchmark lives in a package of its own for the same reason:
// `ruststream-gcp-pubsub`'s dev-dependencies enable that feature through the conformance harness,
// and a benchmark inside that package would link it.
#[cfg(feature = "testing")]
compile_error!(
    "benchmarks must be built without the `testing` feature; run them through `just bench`"
);

/// The project the emulator serves, which is the one `docker-compose.test.yml` starts it with.
const PROJECT: &str = "ruststream-test";

/// Deliveries the probe run takes to measure the raw half's rate, and the floor under a
/// calibrated count.
const PROBE_MESSAGES: usize = 10_000;
/// How long a measured run lasts, at least.
const SECONDS: f64 = 5.0;
/// How much the calibrated count is raised above the probe's estimate.
///
/// The probe is short and cold, so it reads the machine low; without the margin the faster
/// scenario lands just under the floor.
const MARGIN: f64 = 1.25;
/// The ceiling on a calibrated count, so a machine an order faster does not turn a run into an
/// afternoon.
const MAX_MESSAGES: usize = 4_000_000;
/// Rounds run. The best of them is reported.
const PAIRS: usize = 3;
/// Worker threads both halves are driven on.
const WORKERS: usize = 4;

/// How far the publisher may run ahead of the consumer, in messages.
///
/// The emulator does not enforce the streaming pull's flow control, so nothing but this keeps a
/// run's backlog bounded. 32768 bodies is 16 MiB outstanding, and far more than either half of a
/// pair is ever behind.
const IN_FLIGHT: usize = 32_768;
/// How often the publisher checks that ceiling.
const CHECK_EVERY: usize = 256;
/// How deep the publisher's own pipeline runs: publishes handed over and not yet accepted by the
/// emulator.
///
/// Both halves publish one message per call and await the answer, so without a pipeline every
/// batch would leave with a single message in it. Deliberately far below [`IN_FLIGHT`], because
/// the ceiling above counts messages handed over rather than messages delivered: were the two
/// close, a run would hit that ceiling with its backlog still inside the client.
const PUBLISH_IN_FLIGHT: usize = 4_096;
/// How long a run may go without a delivery before it is called stuck.
const STALL: Duration = Duration::from_secs(60);

/// Calls the round-trip probe makes, on one connection, outside both halves of every pair.
///
/// Enough of them that scheduler noise averages out of a figure measured in fractions of a
/// millisecond.
const ROUND_TRIPS: usize = 20_000;

/// Round trips one delivery costs this transport on the consumer side.
///
/// A streaming pull pushes deliveries down a stream the consumer already holds, so receiving one
/// costs no request of its own; what a delivery does charge is its acknowledgement. The client
/// coalesces acknowledgements, so one per delivery is an upper bound - and the flag it decides
/// therefore errs towards saying the transport paced a run, which is the humbler claim.
const ROUND_TRIPS_PER_DELIVERY: f64 = 1.0;

/// How long the client keeps extending the ack deadline of a delivery nothing has settled. It is
/// the client's own default, and the value [`GooglePubSub`] hands the streaming pull, so the raw
/// half asks for the same thing.
const MAX_LEASE: Duration = Duration::from_secs(60 * 60);

/// Distinct ordering keys the ordered scenario publishes under.
///
/// One key would serialize the whole run behind a single ordered stream and measure the
/// emulator's per-key latency. Enough of them keeps the subscription busy while every key still
/// carries a run of messages that has to stay in order.
const ORDERING_KEYS: usize = 64;

/// The body size both halves publish and decode, to the byte: the scenario is published under
/// this number, so the bytes on the wire have to be it.
const BODY_BYTES: usize = 512;
/// How wide one padding value is before the next field starts.
const PAD_WIDTH: usize = 16;
/// The values every body carries. Fixed, so every delivery of a run costs the same.
const ID: u64 = 1_000_000;
const QUANTITY: u32 = 37;

/// What both halves decode a delivery into.
///
/// Two integer fields the loop reads, and a padding the type ignores: a decode that allocates
/// nothing, so the number is about this crate rather than about `serde_json`'s string handling.
#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
    quantity: u32,
}

/// A JSON body carrying the two fields, padded with fields [`Order`] ignores until it is exactly
/// `size` bytes.
///
/// The padding is a run of equally wide fields and one last field cut to whatever is left, so a
/// scenario published as a 512 byte body is one. Building it is startup work, and the assertion
/// below holds the promise the published name makes.
fn json_body(size: usize) -> Vec<u8> {
    let mut body = format!("{{\"id\":{ID},\"quantity\":{QUANTITY}");
    let mut field = 0u32;
    loop {
        let key = format!(",\"f{field}\":\"\"");
        // One byte stays reserved for the closing brace.
        let Some(room) = size.checked_sub(body.len() + key.len() + 1) else {
            break;
        };
        // A full-width field only when what it leaves behind can still hold the next one, whose
        // key is at most one digit longer. Otherwise this is the last field and it takes the
        // rest, because a remainder too small to start a field would come out as a short body.
        let width = if room > PAD_WIDTH + key.len() {
            PAD_WIDTH
        } else {
            room
        };
        body.push_str(&key[..key.len() - 1]);
        body.extend(repeat_n('x', width));
        body.push('"');
        field += 1;
    }
    body.push('}');
    assert_eq!(
        body.len(),
        size,
        "a body has to be the size the scenario publishes"
    );
    body.into_bytes()
}

/// The topology one run owns: nothing is shared with the run before it.
#[derive(Clone, Debug)]
struct RunNames {
    topic: String,
    subscription: String,
}

impl RunNames {
    fn fresh() -> Self {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|since| since.as_nanos())
            .unwrap_or_default();
        Self {
            topic: format!("rs-bench-topic-{stamp}"),
            subscription: format!("rs-bench-sub-{stamp}"),
        }
    }

    fn topic_path(&self) -> String {
        format!("projects/{PROJECT}/topics/{}", self.topic)
    }

    fn subscription_path(&self) -> String {
        format!("projects/{PROJECT}/subscriptions/{}", self.subscription)
    }
}

/// Counts deliveries and marks the ends of the measured window.
///
/// Both halves call the same methods, so both pay for the signal. A delivery pays one relaxed
/// increment and two comparisons; the waiter is a single future for the whole run, woken once.
#[derive(Clone, Debug)]
struct Run(Arc<RunInner>);

#[derive(Debug)]
struct RunInner {
    total: usize,
    seen: AtomicUsize,
    first: OnceLock<Instant>,
    last: OnceLock<Instant>,
    drained: Notify,
}

impl Run {
    fn new(total: usize) -> Self {
        Self(Arc::new(RunInner {
            total,
            seen: AtomicUsize::new(0),
            first: OnceLock::new(),
            last: OnceLock::new(),
            drained: Notify::new(),
        }))
    }

    /// Records one handled delivery, and answers whether the run is over.
    fn arrived(&self) -> bool {
        let seen = self.0.seen.fetch_add(1, Ordering::Relaxed) + 1;
        if seen == 1 {
            let _ = self.0.first.set(Instant::now());
        }
        if seen == self.0.total {
            let _ = self.0.last.set(Instant::now());
            self.0.drained.notify_one();
        }
        seen >= self.0.total
    }

    fn handled(&self) -> usize {
        self.0.seen.load(Ordering::Acquire).min(self.0.total)
    }

    /// Resolves once every expected delivery has been handled.
    async fn drained(&self) {
        while self.0.seen.load(Ordering::Acquire) < self.0.total {
            self.0.drained.notified().await;
        }
    }

    /// The measured window: the first delivery to the end of the last one.
    fn window(&self) -> Duration {
        let first = *self.0.first.get().expect("the run took a delivery");
        let last = *self.0.last.get().expect("the run took its last delivery");
        last - first
    }
}

/// Waits for the run to finish, and fails with what it was waiting for if it stops moving.
async fn drain(run: &Run, half: &str) {
    let mut seen = 0;
    loop {
        if timeout(STALL, run.drained()).await.is_ok() {
            return;
        }
        let handled = run.handled();
        assert!(
            handled > seen,
            "{half}: {handled} of {} deliveries handled and nothing moved for {STALL:?}",
            run.0.total
        );
        seen = handled;
    }
}

/// Deliveries a second, over the window one half of a pair measured.
fn rate(window: Duration, messages: usize) -> f64 {
    messages as f64 / window.as_secs_f64()
}

// ---------------------------------------------------------------------------------------------
// The client, built the way the broker builds it
// ---------------------------------------------------------------------------------------------

/// The four clients a run needs, built against the emulator exactly as
/// [`PubSubBroker::emulator`] builds them: a plaintext endpoint and anonymous credentials.
struct Clients {
    subscriber: GcpSubscriber,
    publisher: BasePublisher,
    topics: TopicAdmin,
    subscriptions: SubscriptionAdmin,
}

macro_rules! build_client {
    ($builder:expr, $endpoint:expr, $credentials:expr) => {
        $builder
            .with_endpoint($endpoint.to_owned())
            .with_credentials($credentials.clone())
            .build()
            .await
            .expect("the emulator accepts a client")
    };
}

async fn clients(host: &str) -> Clients {
    let endpoint = format!("http://{host}");
    let credentials: Credentials = anonymous::Builder::new().build();
    Clients {
        subscriber: build_client!(GcpSubscriber::builder(), &endpoint, &credentials),
        publisher: build_client!(BasePublisher::builder(), &endpoint, &credentials),
        topics: build_client!(TopicAdmin::builder(), &endpoint, &credentials),
        subscriptions: build_client!(SubscriptionAdmin::builder(), &endpoint, &credentials),
    }
}

impl Clients {
    /// The topology the crate's `create_with_topic` creates, spelled out so the two halves consume
    /// from the same kind of subscription. Message ordering is on either way, because that is what
    /// the descriptor turns on and a subscription is ordered or not for every delivery it makes.
    async fn create(&self, names: &RunNames) {
        self.topics
            .create_topic()
            .set_name(names.topic_path())
            .send()
            .await
            .expect("the emulator creates the topic");
        self.subscriptions
            .create_subscription()
            .set_name(names.subscription_path())
            .set_topic(names.topic_path())
            .set_enable_message_ordering(true)
            .send()
            .await
            .expect("the emulator creates the subscription");
    }

    async fn delete(&self, names: &RunNames) {
        self.subscriptions
            .delete_subscription()
            .set_subscription(names.subscription_path())
            .send()
            .await
            .expect("the emulator deletes the subscription");
        self.topics
            .delete_topic()
            .set_topic(names.topic_path())
            .send()
            .await
            .expect("the emulator deletes the topic");
    }
}

/// Feeds a run, never letting the consumer fall further behind than [`IN_FLIGHT`] and never
/// leaving more than [`PUBLISH_IN_FLIGHT`] publishes unanswered.
///
/// The half supplies `publish`, and that call is the only thing the two halves do differently on
/// the way in.
async fn publish_all<Publish, Published>(run: &Run, publish: Publish)
where
    Publish: Fn(usize) -> Published,
    Published: Future<Output = ()>,
{
    let mut pending = FuturesUnordered::new();
    for sent in 0..run.0.total {
        if sent % CHECK_EVERY == 0 {
            while sent.saturating_sub(run.handled()) > IN_FLIGHT {
                sleep(Duration::from_micros(200)).await;
            }
        }
        if pending.len() >= PUBLISH_IN_FLIGHT {
            pending.next().await;
        }
        pending.push(publish(sent));
    }
    while pending.next().await.is_some() {}
}

/// Feeds a run through this crate's publisher, which is what the adapter and the framework loops
/// share: the runtime is the only thing between them.
async fn feed(publisher: &PubSubPublisher, topic: &str, scenario: Scenario, run: &Run) {
    let body = json_body(BODY_BYTES);
    let keys = ordering_keys(scenario);
    publish_all(run, |sent| {
        let key = keys.get(sent % ORDERING_KEYS).cloned();
        let body = &body;
        async move {
            let options = key.map(|ordering_key| PubSubPublishOptions {
                ordering_key: Some(ordering_key),
            });
            publisher
                .publish(
                    OutgoingMessage::new(topic, body.as_slice()),
                    options.as_ref(),
                )
                .await
                .expect("the emulator accepts the publish");
        }
    })
    .await;
}

/// The ordering keys a run publishes under, or an empty list for the unordered scenario.
fn ordering_keys(scenario: Scenario) -> Vec<String> {
    match scenario {
        Scenario::Plain => Vec::new(),
        Scenario::Ordered => (0..ORDERING_KEYS)
            .map(|key| format!("bench-{key}"))
            .collect(),
    }
}

// ---------------------------------------------------------------------------------------------
// The half that goes through this crate
// ---------------------------------------------------------------------------------------------

async fn through_crate(
    scenario: Scenario,
    host: &str,
    names: &RunNames,
    messages: usize,
) -> Duration {
    let admin = clients(host).await;
    let connected = PubSubBroker::new(PROJECT)
        .emulator(host)
        .connect()
        .await
        .expect("the broker connects");
    // The descriptor creates its topology as it subscribes, which is also what attaches the
    // consumer before anything is published.
    let subscriber = GooglePubSub::new(&names.subscription)
        .create_with_topic(&names.topic)
        .subscribe(&connected)
        .await
        .expect("the subscription opens");

    let run = Run::new(messages);
    let consuming = tokio::spawn({
        let run = run.clone();
        async move {
            let mut subscriber = subscriber;
            let mut stream = pin!(subscriber.stream());
            while let Some(delivery) = stream.next().await {
                let message = delivery.expect("the subscription delivers");
                let order: Order =
                    serde_json::from_slice(message.payload()).expect("the body decodes");
                black_box((order.id, order.quantity));
                let done = run.arrived();
                message
                    .ack()
                    .await
                    .expect("the acknowledgement is accepted");
                if done {
                    break;
                }
            }
        }
    });

    feed(&connected.publisher(), &names.topic, scenario, &run).await;
    drain(&run, "adapter").await;
    consuming.await.expect("the consuming task ends");
    let window = run.window();
    connected.shutdown().await.expect("the broker shuts down");
    admin.delete(names).await;
    window
}

// ---------------------------------------------------------------------------------------------
// The loop that goes through the whole service
// ---------------------------------------------------------------------------------------------

/// The names the service being built subscribes to.
///
/// `#[subscriber(..)]` takes an expression and evaluates it where the handler is mounted, which is
/// inside the builder of the run that is starting. A run installs its own names here first, so the
/// subscription the runtime opens is the one this run publishes to.
static NAMES: Mutex<Option<RunNames>> = Mutex::new(None);

fn install(names: &RunNames) {
    *NAMES
        .lock()
        .expect("the names cell is never held across a panic") = Some(names.clone());
}

fn installed() -> RunNames {
    NAMES
        .lock()
        .expect("the names cell is never held across a panic")
        .clone()
        .expect("a run installs its names before it builds the service")
}

#[subscriber(GooglePubSub::new(installed().subscription).create_with_topic(installed().topic))]
async fn handle(order: &Order, ctx: &mut Context<'_, (), Run>) -> HandlerOutcome {
    black_box((order.id, order.quantity));
    ctx.state().arrived();
    HandlerOutcome::ack()
}

async fn start(broker: PubSubBroker, run: Run) -> RunningApp {
    RustStream::new(AppInfo::new("pubsub-bench", "0.0.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(run))
        .with_broker(broker, |b| {
            b.include(handle);
        })
        .start()
        .await
        .expect("the service starts")
}

async fn through_framework(
    scenario: Scenario,
    host: &str,
    names: &RunNames,
    messages: usize,
) -> Duration {
    let admin = clients(host).await;
    let broker = PubSubBroker::new(PROJECT).emulator(host);
    // The same publisher the adapter loop feeds itself with, resolved through the broker's cell
    // once the service connects: what separates this loop from that one is the runtime, and a
    // second publish path would be a second difference.
    let publisher = broker.publisher();
    let run = Run::new(messages);
    install(names);
    // The descriptor creates its topology as it subscribes, and the service does that while it
    // starts - which is also what attaches the consumer before anything is published.
    let app = start(broker, run.clone()).await;

    feed(&publisher, &names.topic, scenario, &run).await;
    drain(&run, "framework").await;
    let window = run.window();
    app.shutdown().await.expect("the service stops");
    admin.delete(names).await;
    window
}

// ---------------------------------------------------------------------------------------------
// The loop that goes through the client
// ---------------------------------------------------------------------------------------------

async fn through_client(
    scenario: Scenario,
    host: &str,
    names: &RunNames,
    messages: usize,
) -> Duration {
    let clients = clients(host).await;
    clients.create(names).await;
    // The crate hands the streaming pull this one setting and leaves the rest of the client's
    // defaults alone, so this half asks for the same thing.
    let mut stream = clients
        .subscriber
        .subscribe(names.subscription_path())
        .set_max_lease(MAX_LEASE)
        .build();
    let shutdown = stream.shutdown_token();

    let run = Run::new(messages);
    let consuming = tokio::spawn({
        let run = run.clone();
        async move {
            while let Some(delivery) = stream.next().await {
                let (message, handler) = delivery.expect("the subscription delivers");
                let order: Order = serde_json::from_slice(&message.data).expect("the body decodes");
                black_box((order.id, order.quantity));
                let done = run.arrived();
                handler.ack();
                if done {
                    break;
                }
            }
        }
    });

    let publisher: GcpPublisher = clients.publisher.publisher(names.topic_path()).build();
    let body = Bytes::from(json_body(BODY_BYTES));
    let keys = ordering_keys(scenario);
    publish_all(&run, |sent| {
        let key = keys.get(sent % ORDERING_KEYS).cloned();
        let publisher = &publisher;
        let body = &body;
        async move {
            let mut message = GcpMessage::new().set_data(body.clone());
            if let Some(key) = key {
                message = message.set_ordering_key(key);
            }
            publisher
                .publish(message)
                .await
                .expect("the emulator accepts the publish");
        }
    })
    .await;

    drain(&run, "client").await;
    consuming.await.expect("the consuming task ends");
    shutdown.shutdown().await;
    let window = run.window();
    clients.delete(names).await;
    window
}

/// What one round trip to the emulator costs, measured outside both halves of every pair.
///
/// The probe is what decides whether a row was paced by the transport: a delivery is charged
/// [`ROUND_TRIPS_PER_DELIVERY`] of these, and a row whose product covers half the time a message
/// took is one the emulator paced. A subscription lookup is the cheapest call the API has whose
/// answer the client waits for, so what it times is the round trip and not the work behind it.
async fn round_trip(host: &str) -> Duration {
    let clients = clients(host).await;
    let names = RunNames::fresh();
    clients.create(&names).await;
    let started = Instant::now();
    for _ in 0..ROUND_TRIPS {
        clients
            .subscriptions
            .get_subscription()
            .set_subscription(names.subscription_path())
            .send()
            .await
            .expect("the emulator answers the lookup");
    }
    let each = started.elapsed() / ROUND_TRIPS as u32;
    clients.delete(&names).await;
    each
}

// ---------------------------------------------------------------------------------------------
// The run
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Scenario {
    Plain,
    Ordered,
}

impl Scenario {
    fn name(self) -> &'static str {
        match self {
            Self::Plain => "Streaming pull, 512 B JSON, ack each",
            Self::Ordered => "Streaming pull with ordering keys, 512 B JSON, ack each",
        }
    }
}

/// Best and worst of the rounds.
///
/// Noise on the machine only ever slows a run down, so the fastest round is the closest to the
/// undisturbed cost, and the slowest says how far from quiet the machine was.
#[derive(Clone, Copy, Debug)]
struct Stats {
    best: f64,
    worst: f64,
}

impl Stats {
    fn of(rates: &[f64]) -> Self {
        assert!(!rates.is_empty(), "no round was run");
        Self {
            best: rates.iter().copied().fold(f64::MIN, f64::max),
            worst: rates.iter().copied().fold(f64::MAX, f64::min),
        }
    }

    fn spread(self) -> f64 {
        self.best - self.worst
    }
}

#[derive(Debug)]
struct Measured {
    scenario: Scenario,
    messages: usize,
    pairs: usize,
    raw: Stats,
    adapter: Stats,
    framework: Stats,
    overhead_percent: f64,
    adapter_overhead_percent: f64,
    verdict: &'static str,
    adapter_verdict: &'static str,
    broker_bound: bool,
}

/// The honesty rule of the procedure, applied to one difference: a gap smaller than the spread
/// between runs of either side is a verdict, never a percentage.
fn verdict(raw: Stats, other: Stats) -> &'static str {
    if (raw.best - other.best).abs() < raw.spread().max(other.spread()) {
        "indistinguishable"
    } else {
        "measured"
    }
}

/// How much slower `other` is than the raw client, as a percentage of the raw client's rate.
fn overhead(raw: Stats, other: Stats) -> f64 {
    (raw.best - other.best) / raw.best * 100.0
}

async fn measure(
    scenario: Scenario,
    host: &str,
    pairs: usize,
    seconds: f64,
    round_trip: Duration,
) -> Measured {
    // The probe is the warm-up as well: its result is thrown away, and the rate it measured sets
    // a count that makes every run below last at least `seconds`.
    let probe = through_client(scenario, host, &RunNames::fresh(), PROBE_MESSAGES).await;
    let messages = ((rate(probe, PROBE_MESSAGES) * seconds * MARGIN) as usize)
        .clamp(PROBE_MESSAGES, MAX_MESSAGES);
    println!(
        "{}: {messages} messages per run ({:.0} msg/s probed)",
        scenario.name(),
        rate(probe, PROBE_MESSAGES)
    );

    let mut raws = Vec::with_capacity(pairs);
    let mut adapters = Vec::with_capacity(pairs);
    let mut frameworks = Vec::with_capacity(pairs);
    for round in 1..=pairs {
        let raw = through_client(scenario, host, &RunNames::fresh(), messages).await;
        let adapter = through_crate(scenario, host, &RunNames::fresh(), messages).await;
        let framework = through_framework(scenario, host, &RunNames::fresh(), messages).await;
        println!(
            "  round {round:>2}: raw {:>9.0}, adapter {:>9.0}, framework {:>9.0} msg/s",
            rate(raw, messages),
            rate(adapter, messages),
            rate(framework, messages)
        );
        raws.push(rate(raw, messages));
        adapters.push(rate(adapter, messages));
        frameworks.push(rate(framework, messages));
    }

    let raw = Stats::of(&raws);
    let adapter = Stats::of(&adapters);
    let framework = Stats::of(&frameworks);
    // What the transport charges the raw consumer per delivery, against what a delivery took.
    // Half is the line: past it the work above the client happened inside a wait that was already
    // being paid, and the row is a lower bound on that work rather than a measurement of it.
    let charged = ROUND_TRIPS_PER_DELIVERY * round_trip.as_secs_f64();
    let per_message = 1.0 / raw.best;
    Measured {
        scenario,
        messages,
        pairs,
        raw,
        adapter,
        framework,
        overhead_percent: overhead(raw, framework),
        adapter_overhead_percent: overhead(raw, adapter),
        verdict: verdict(raw, framework),
        adapter_verdict: verdict(raw, adapter),
        broker_bound: charged >= per_message / 2.0,
    }
}

fn document(measured: &[Measured], round_trip: Duration) -> String {
    let mut out = format!(
        "{{\n  \"round_trip\": \"{:.3} ms ({ROUND_TRIPS} GetSubscription calls on one \
         connection); a delivery is charged {ROUND_TRIPS_PER_DELIVERY} of them\",\n  \
         \"scenarios\": [\n",
        round_trip.as_secs_f64() * 1000.0,
    );
    for (index, row) in measured.iter().enumerate() {
        let comma = if index + 1 == measured.len() { "" } else { "," };
        write!(
            out,
            concat!(
                "    {{\n",
                "      \"name\": \"{name}\",\n",
                "      \"unit\": \"msg/s\",\n",
                "      \"messages\": {messages},\n",
                "      \"pairs\": {pairs},\n",
                "      \"raw\": {{ \"best\": {raw_best:.0}, \"worst\": {raw_worst:.0} }},\n",
                "      \"adapter\": {{ \"best\": {ad_best:.0}, \"worst\": {ad_worst:.0} }},\n",
                "      \"framework\": {{ \"best\": {fw_best:.0}, \"worst\": {fw_worst:.0} }},\n",
                "      \"overhead_percent\": {overhead:.1},\n",
                "      \"adapter_overhead_percent\": {adapter_overhead:.1},\n",
                "      \"verdict\": \"{verdict}\",\n",
                "      \"adapter_verdict\": \"{adapter_verdict}\",\n",
                "      \"broker_bound\": {broker_bound}\n",
                "    }}{comma}\n",
            ),
            name = row.scenario.name(),
            messages = row.messages,
            pairs = row.pairs,
            raw_best = row.raw.best,
            raw_worst = row.raw.worst,
            ad_best = row.adapter.best,
            ad_worst = row.adapter.worst,
            fw_best = row.framework.best,
            fw_worst = row.framework.worst,
            overhead = row.overhead_percent,
            adapter_overhead = row.adapter_overhead_percent,
            verdict = row.verdict,
            adapter_verdict = row.adapter_verdict,
            broker_bound = row.broker_bound,
            comma = comma,
        )
        .expect("writing to a String");
    }
    out.push_str("  ]\n}\n");
    out
}

fn runtime() -> Runtime {
    Builder::new_multi_thread()
        .worker_threads(WORKERS)
        .enable_all()
        .build()
        .expect("the tokio runtime builds")
}

/// A positive count from the environment, or the default.
///
/// The parse target rejects zero, so a pairs count of zero is refused here rather than after the
/// probe run, where it would panic in the statistics with no round to report.
fn number(name: &str, fallback: usize) -> usize {
    env::var(name).ok().map_or(fallback, |value| {
        value
            .parse::<NonZeroUsize>()
            .unwrap_or_else(|_| panic!("{name} must be a positive number"))
            .get()
    })
}

fn main() {
    let host = env::var("PUBSUB_TEST_HOST")
        .expect("PUBSUB_TEST_HOST names the emulator to measure against; `just bench` sets it");
    let pairs = number("RUSTSTREAM_BENCH_PAIRS", PAIRS);
    let seconds = number("RUSTSTREAM_BENCH_SECONDS", SECONDS as usize) as f64;
    let out = env::var("RUSTSTREAM_BENCH_OUT").unwrap_or_else(|_| "bench-paired.json".to_owned());

    let runtime = runtime();
    // Outside every pair, and once for the whole run: what a round trip to this emulator costs is
    // a property of the transport, not of a scenario.
    let round_trip = runtime.block_on(round_trip(&host));
    println!("round trip: {:.3} ms", round_trip.as_secs_f64() * 1000.0);

    let measured: Vec<Measured> = [Scenario::Plain, Scenario::Ordered]
        .into_iter()
        .map(|scenario| runtime.block_on(measure(scenario, &host, pairs, seconds, round_trip)))
        .collect();

    println!();
    for row in &measured {
        println!(
            "{}: raw {:.0}, adapter {:.0} ({:+.1}%, {}), framework {:.0} ({:+.1}%, {}) msg/s{}",
            row.scenario.name(),
            row.raw.best,
            row.adapter.best,
            row.adapter_overhead_percent,
            row.adapter_verdict,
            row.framework.best,
            row.overhead_percent,
            row.verdict,
            if row.broker_bound {
                ", broker-bound"
            } else {
                ""
            }
        );
    }

    std::fs::write(&out, document(&measured, round_trip)).expect("the summary is written");
    println!("\nwrote {out}");
}
