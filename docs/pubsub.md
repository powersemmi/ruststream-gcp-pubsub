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
| `TransactionalPublisher` | no | [an ordering key](#ordering-keys-and-the-partition-key) is what groups a run of messages |
| `OwnedTransactions` | no | there is no publish transaction to own |
| `RequestReply` | no | you build it yourself from a reply topic and a correlation attribute |
| `Partitioned` | yes | [the partition key is the message's ordering key](#ordering-keys-and-the-partition-key) |
| `Seekable` and `Positioned` | no | you reposition a whole subscription with the admin `seek`, to a timestamp or a snapshot |
| `DescribeServer` | yes | reports the endpoint in use (emulator host, custom endpoint, or `pubsub.googleapis.com`) under the `googlepubsub` protocol |

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

`PubSubSubscription` names the subscription a handler consumes, by short id or by full
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

### Exactly-once acknowledgement

Exactly-once delivery is a setting on the subscription resource, so the same handler code runs
against both kinds of subscription. On an exactly-once subscription `ack` returns `Ok` only once the
service has confirmed it, and the message is then not redelivered. A refused acknowledgement (an
expired ack id, a lost deadline race) returns `AckError::Broker` instead of passing as success. On
an ordinary subscription `ack` returns `Ok` as soon as the acknowledgement is queued.

## Ordering keys and the partition key

The framework's partition key is Pub/Sub's ordering key. A `partition-key` header (exported as
`PARTITION_KEY_HEADER`) on an outgoing message becomes its ordering key, and a delivery reports its
own ordering key back under that header.

`with_ordering_key` adapts a publisher: every publish built on it sends the key, so one adapter
serves a run of publishes. The rest of the chain (codec, headers, destination) is written as usual.
Headers named at the call are written over the adapter's, so a `partition-key` named there wins.

```rust
--8<-- "crates/ruststream-gcp-pubsub/examples/pubsub_ordered_publish.rs:ordered"
```

On an `Out` slot the step resolves on the slot itself, so a keyed publish is still the slot's and
`tb.out::<Marker>()` sees it in a test.

A handler's reply takes its key from the mount chain's `.transform(..)` step, which reads the
delivery and writes the reply's `partition-key` header per message.

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
capability: `Out(out): Out<impl Publisher>`, or `Out<impl PubSubOrdering>` for a body that names
ordering keys. Such a file names no broker.

A routes file imports `ruststream_gcp_pubsub::prelude::*`, where the policy arrives under the
mount-site name `Publish`: `.out(Reply, Publish)` for the value a replying handler returns,
`.out(Marker, Publish)` for an `Out` slot. `Publish` has no options, so the name is the whole
expression. `PubSubPublish` stays at the crate root for a file that speaks to two brokers at once
and has to say which one it means.

A publish addresses a topic, by id or by full resource name. The client publisher for a topic is
created on first use and cached on the broker, so `shutdown` flushes every batch it has buffered.

`with_ordering_key` is this crate's only addition to the framework's publish chain. A Pub/Sub
message is a payload, its attributes and an ordering key, so the chain covers all three: the value,
its headers, and the key.

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

What the server itself decides is checked against the [emulator](#the-emulator), where the
integration suite runs.

Because it routes by exact name, the stand-in serves the by-name subscriber form
(`#[subscriber("orders-workers")]`). A handler that names a `PubSubSubscription` does not mount on
it: the descriptor opens a subscription on the real broker, and the mismatch is a compile error.
Cover those handlers against the emulator.
