# Google Cloud Pub/Sub

`ruststream-gcp-pubsub` runs a RustStream service on Google Cloud Pub/Sub, over the official
[`google-cloud-pubsub`](https://docs.rs/google-cloud-pubsub) client. Pub/Sub keeps the topic and the
subscription apart: a service publishes to a topic, and a handler consumes a subscription, which
queues that topic's messages for it. Framework concepts (writing subscribers, routing, codecs,
middleware) are in the [RustStream documentation](https://powersemmi.github.io/ruststream/).

```toml
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-gcp-pubsub = "0.7"
serde = { version = "1", features = ["derive"] }
```

## Capabilities

The framework's optional capabilities on Pub/Sub:

| Capability | Native | Notes |
| --- | --- | --- |
| `Subscribe` | yes | you subscribe by [subscription name](#subscriptions) |
| `BatchSubscriber` | client-side | a slice handler gets [batches assembled on the client](#batches) |
| `TransactionalPublisher` | no | [an ordering key](#ordering-keys) is what groups a run of messages |
| `OwnedTransactions` | no | there is no publish transaction to own |
| `RequestReply` | no | you build it yourself from a reply topic and a correlation attribute |
| `Partitioned` | yes | [the partition key is the message's ordering key](#ordering-keys) |
| `Seekable` and `Positioned` | no | you reposition a whole subscription with the admin `seek`, to a timestamp or a snapshot |
| `DescribeServer` | yes | reports the host and port in use (emulator, custom endpoint, or `pubsub.googleapis.com`) under the `googlepubsub` protocol; a scheme, a path or credentials written into the endpoint stay out of the document |

A handler reads the ordering key off a delivery with `message.partition_key()`, and imports nothing
from this crate for it.

## The lifecycle

Each state of the broker is a distinct type, reached by a consuming transition:

```text
PubSubBroker::new(project)   configuration only, synchronous, no I/O
  .connect()   ->  ConnectedPubSubBroker    the live clients; subscriptions and publishers
  .shutdown()  ->  ()                       flushes every buffered publish batch
```

`shutdown` consumes the connected broker, so a publish or a subscribe after it does not compile. A
publisher handed out earlier is outside that guarantee: once the connection is gone it returns
`PubSubError::NotConnected` instead of accepting a message that nothing will send.

By default the broker authenticates with Application Default Credentials. `credentials(..)` takes
your own `google_cloud_auth::credentials::Credentials` instead. `endpoint(..)` names another
service endpoint, a regional one among them. `emulator(host)` points every client at a local
[emulator](#the-emulator).

## Subscriptions

`GooglePubSub` names the subscription a handler consumes, by short id or by full
`projects/{project}/subscriptions/{name}` resource name. It goes inside the `#[subscriber(..)]`
decorator:

```rust
--8<-- "crates/ruststream-gcp-pubsub/examples/pubsub_service.rs:handler"
```

A mount site pairs the handler with the broker:

```rust
--8<-- "crates/ruststream-gcp-pubsub/examples/pubsub_service.rs:app"
```

The descriptor takes four options:

- `create_with_topic(topic)` creates the subscription and its topic on subscribe when they do not
  exist yet. It is meant for local development and tests against the [emulator](#the-emulator); a
  production subscription is usually managed as infrastructure.
- `max_outstanding(n)` is flow control: how many delivered messages may be unacknowledged at once.
  This is the real prefetch, and it defaults to the client's 1000.
- `ack_extension(duration)` is how far each background extension of the ack deadline reaches while a
  handler runs. The client clamps it to the protocol's 10s to 600s range and defaults to 60s.
- `batch_wait(duration)` is how long a partial [batch](#batches) waits for more deliveries. It
  defaults to 50ms.

A descriptor with an empty subscription or topic name returns `PubSubError::InvalidDescriptor`
before any I/O.

A subscription is consumed with a streaming pull. The client extends the ack deadline in the
background while a handler runs, so a slow handler does not cause a redelivery by itself. Dropping
the subscriber drains the stream.

`#[subscriber("orders-workers")]` with a plain string names the same descriptor with its defaults,
so the subscription has to exist already.

A subscription is identified by its name and nothing else, so the mount site may supply it:
`#[subscriber(GooglePubSub)]` on the handler and `.name("orders-workers")` where it is mounted. That
is how one handler serves two deployments that differ only in subscription name.

## Batches

A handler that takes a slice is handed a whole batch: one database round-trip, one bulk API call,
per batch instead of per order.

```rust
--8<-- "crates/ruststream-gcp-pubsub/examples/pubsub_batches.rs:handler"
```

The mount site names the batch size, and a batch handler does not mount without one:

```rust
--8<-- "crates/ruststream-gcp-pubsub/examples/pubsub_batches.rs:app"
```

The streaming pull hands over one delivery at a time, so batches are assembled on the client. A
batch never holds more than the size the mount named, and may hold fewer.

The size is one half of a batch; `batch_wait` on the descriptor is the other, and its 50ms default
is about one streaming-pull burst. Shorter, and most batches close on their first delivery; longer,
and a quiet subscription holds a batch back for nothing. Raise it when a batch earns its round trip.

`max_outstanding` bounds the pull, not the batch.

`PubSubTestBroker` assembles batches the same way, so a slice handler can be
[tested in process](#testing) and not only against the emulator.

## Acknowledgement

Acknowledgement is native and per message:

| Handler outcome | Pub/Sub call | Effect |
| --- | --- | --- |
| `HandlerOutcome::ack()` | acknowledge | the delivery is done |
| `HandlerOutcome::retry()` | nack | the message becomes available again and is redelivered |
| `HandlerOutcome::drop()` | acknowledge | the message is not redelivered |

Pub/Sub has no drop-without-redelivery verb, which is why `drop()` acknowledges. Poison messages are
routed by the subscription's dead-letter policy, set on the subscription resource. Under such a
policy the delivery-attempt count is delivered as the `pubsub-delivery-attempt` header (exported as
`DELIVERY_ATTEMPT_HEADER`), and a handler can branch on how many times a message has come back.

Pub/Sub has no delayed nack, so `HandlerOutcome::retry_after(delay)` runs on the runtime's
[deferred re-publish](https://powersemmi.github.io/ruststream/latest/guides/subscribers/#delayed-redelivery):
wire a publisher on the scope with `retry_via`, taking it from `b.broker().publisher()`. The runtime
then acknowledges the delivery and publishes a copy of the message after the delay. Without that
publisher the delay is dropped, the message is requeued at once, and the runtime warns.

The copy goes to the topic the subscription is bound to, never to the subscription name: a publish
on Pub/Sub addresses a topic. `GooglePubSub` reports that topic - the one `create_with_topic` names,
or the one the API reports for a subscription managed as infrastructure, asked once at startup.
A handler declared with a plain string cannot report one, because a subscription name reaches
nothing, so a scope that wires `retry_via` over `#[subscriber("orders-workers")]` refuses to start
and names the subscription. Declaring that handler with `GooglePubSub::new("orders-workers")` is the
fix.

### Exactly-once acknowledgement

Exactly-once delivery is a setting on the subscription resource, so the same handler code runs
against both kinds of subscription. On an exactly-once subscription `ack` returns `Ok` only once the
service has confirmed it, and the message is then not redelivered. A refused acknowledgement (an
expired ack id, a lost deadline race) returns `AckError::Broker` instead of passing as success. On
an ordinary subscription `ack` returns `Ok` as soon as the acknowledgement is queued.

## Ordering keys

Messages sharing an ordering key are delivered to one subscriber in publish order. The key is a
field of the message, not a header, and it is the one setting a Pub/Sub publish carries beyond its
payload and attributes. `PubSubPublishOptions` is that setting as a type, with `ordering_key` its
only field.

The key reaches a publish from two places. The mount site fixes it for a whole slot, which is what
a slot belonging to one entity wants:

```rust
--8<-- "crates/ruststream-gcp-pubsub/examples/pubsub_ordered_publish.rs:ordered"
```

A single publish names its own with the `ordering_key` step, between `message(..)` and `publish()`.
The step is a position on the framework's publish builder, so the publish it finishes keeps
everything else the mount site decided: the codec that entry named, its transforms, and the slot a
test asserts on.

A handler body that names the step imports this crate's prelude and says so in its signature, which
is the one place a body names the broker it runs on:

```rust
--8<-- "crates/ruststream-gcp-pubsub/examples/pubsub_ordered_publish.rs:handler"
```

A body that names no key needs neither, and sends under whatever the mount site fixed.

A reply carries no call site, so its key is the policy's: `.out(Reply, Publish::default().ordering_key("receipts"))`.
A key that differs per reply is a mount-chain `.transform(..)`, which reads the delivery and writes
the reply's `partition-key` header.

On the delivery side the framework's partition key *is* the ordering key: a delivery reports it with
`message.partition_key()` and under the `partition-key` header (exported as `PARTITION_KEY_HEADER`),
so a handler that reads keys imports nothing from this crate. That header is the same key on the way
out too, for a service written against no particular broker: writing it on an outgoing message
orders that message, and a call that names the step wins over it. It never travels as an attribute.

Ordered delivery needs message ordering enabled on the subscription. A regional `endpoint` is what
keeps one key in order across publishers in a region. A publish that returns an error on an ordered
key pauses that key in the client; this crate resumes the key and returns the error, so one failure
does not block every later publish on that key.

Every other header is a message attribute, in both directions, with no envelope around it, so a
non-Rust peer sees an ordinary Pub/Sub message.

## Publishing

`PubSubPublish` is the policy that constructs the publisher `PubSubPublisher`, and the runtime
instantiates it at startup on the connected broker. It is also the broker's default policy, so a
replying handler mounted without an `.out(Reply, ..)` call publishes through it.

A file of handler bodies imports the framework's prelude alone and bounds its slot with a
capability: `Out(out): Out<impl Publisher>`. Such a file names no broker. The exception is a body
that names an [ordering key](#ordering-keys) per message.

A routes file imports `ruststream_gcp_pubsub::prelude::*`, where the policy arrives under the
mount-site name `Publish`: `.out(Reply, Publish::default())` for the value a replying handler
returns, `.out(Marker, Publish::default())` for an `Out` slot, and `.ordering_key(..)` on the policy
where the whole slot is ordered. `PubSubPublish` stays at the crate root for a file that speaks to
two brokers at once and has to say which one it means.

A publish addresses a topic, by id or by full resource name. The client publisher for a topic is
created on first use and cached on the broker, so `shutdown` flushes every batch it has buffered.

`ordering_key` is this crate's only addition to the framework's publish chain. A Pub/Sub message is
a payload, its attributes and an ordering key, so the chain covers all three: the value, its
headers, and the key.

A message built by hand and handed to `Publisher::publish` is sent as it was built, which is the way
to control the header map yourself.

## The emulator

`emulator(host)` points the broker at a local Pub/Sub emulator: the plaintext endpoint and anonymous
credentials in one call. The client does not read `PUBSUB_EMULATOR_HOST`, so name the host yourself.

```bash
just brokers-up    # docker compose up: gcloud beta emulators pubsub start on 8085
cargo run --example pubsub_service
just brokers-down
```

The emulator starts empty, which is what `create_with_topic` is for: the subscription and its topic
are created on subscribe. Two services racing on the same names both end up connected, because
creation is a get, then a create, then a get.

The live test suite runs the same way, gated behind `PUBSUB_TEST_HOST`:

```bash
just test-brokers  # starts the emulator, runs the integration and conformance suites
```

## Testing

The `testing` feature ships `PubSubTestBroker`: an in-process transport that reproduces the crate's
routing. Mount the service on it and drive it with the framework's `TestApp` harness. A test file
names both by path, next to the prelude glob:
`use ruststream_gcp_pubsub::testing::PubSubTestBroker;` and
`use ruststream::testing::TestApp;`.

`TestApp::start(app)` connects the app and mounts its handlers.
`tb.broker::<PubSubTestBroker>().message(&order).to("orders-workers").publish()` delivers an order
to the handler's subscription, and `tb.settle()` waits for the reaction to finish.

`tb.broker::<PubSubTestBroker>().subscriber("orders-workers").assert_called_once()` then asserts
that the handler ran, and
`tb.broker::<PubSubTestBroker>().published::<Receipt>("receipts").assert_called_once().with(&expected)`
asserts what it published to the topic. See
[Unit-testing a service with TestApp](https://powersemmi.github.io/ruststream/latest/guides/testing/#unit-testing-a-service-with-testapp).

A routes file mounts on the stand-in as it ships. `#[subscriber(GooglePubSub::new("orders-workers"))]`
opens an in-process subscription as readily as it opens a streaming pull, and `Publish` pairs with
the stand-in as it pairs with the broker. Neither side has a test-only spelling to swap in.

`tb.out::<Marker>().with_options(&PubSubPublishOptions { ordering_key: Some("order-7".to_owned()) })`
reads back the key one publish through a slot asked for, and `assert_options_default()` states that
a publish named none and took the mount site's. The key also reaches the published message, under
the `partition-key` header a delivery reports it by, so either assertion works.

The stand-in routes by one address, the subscription name, because it holds no topics. A test
therefore injects to the subscription, not to the topic. `batch_wait` is honoured, since batching is
on the client either way. `create_with_topic` has nothing to create, and `max_outstanding` and
`ack_extension` name machinery that is not there, so all three are ignored.

A publisher that outlived `shutdown` reports `NotConnected` here as it does against Pub/Sub, so a
test cannot go green on a publish the product would refuse.

What the server itself decides - deadline extension, redelivery timing, ordered delivery,
dead-letter policies - is checked against the [emulator](#the-emulator), where the integration suite
and the framework's conformance lifecycle run.
