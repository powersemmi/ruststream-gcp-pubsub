# Google Cloud Pub/Sub

`ruststream-gcp-pubsub` is the Google Cloud Pub/Sub broker, built on the official
[`google-cloud-pubsub`](https://docs.rs/google-cloud-pubsub) client. For framework concepts
(writing subscribers, routing, codecs, middleware), see the
[RustStream documentation](https://powersemmi.github.io/ruststream/).

```toml
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-gcp-pubsub = "0.7"
serde = { version = "1", features = ["derive"] }
```

## Capabilities

The framework's optional capability traits, and what this broker implements natively:

| Capability | Native | Notes |
| --- | --- | --- |
| `Subscribe` | yes | [subscribe by subscription name](#subscriptions); the subscription must already exist |
| `BatchSubscriber` | client-side | the streaming pull yields one message at a time, so [pages are assembled on the client](#pages) to the size the mount site names |
| `TransactionalPublisher` | no | the product has no publish transaction; ordering keys, not atomic batches, are its grouping mechanism |
| `OwnedTransactions` | no | the product has no publish transaction |
| `RequestReply` | no | there is no native request/reply; a reply topic and a correlation attribute are an application-level pattern |
| `Partitioned` | yes | [the partition key is the message's ordering key](#ordering-keys-and-the-partition-key) |
| `Seekable` and `Positioned` | no | repositioning is a subscription-level admin `seek` to a timestamp or a snapshot, not an offset the subscriber addresses per stream |
| `DescribeServer` | yes | reports the endpoint in use (emulator host, custom endpoint, or `pubsub.googleapis.com`) with the `googlepubsub` protocol |

A handler reads the ordering key off the delivery it is handed, with `message.partition_key()`
and no broker-specific import.

## The lifecycle

The broker is a ladder of consuming transitions, so each state is a distinct type:

```text
PubSubBroker::new(project)   configuration only, synchronous, no I/O
  .connect()   ->  ConnectedPubSubBroker    the live clients; subscriptions and publishers
  .shutdown()  ->  ()                       flushes every buffered publish batch
```

`new` performs no I/O, so a Pub/Sub service is assembled with the same `#[ruststream::app]` macro
as any other broker: the runtime authenticates and builds the clients once at startup, before
opening subscriptions, and flushes buffered batches at the end. Because `shutdown` consumes the
connected broker, publishing or subscribing after it does not compile. A publisher handed out
earlier still aliases the clients, and reports `PubSubError::NotConnected` once they are gone
rather than accepting a message that would never be sent.

Credentials are Application Default Credentials unless the synchronous form says otherwise:
`credentials(..)` supplies an explicit `google_cloud_auth::credentials::Credentials`, `endpoint(..)`
targets a specific service endpoint (a regional endpoint is what keeps ordering keys ordered across
publishers in one region), and `emulator(host)` points the whole client set at a local emulator.

## Subscriptions

Pub/Sub separates the topic from the subscription, and `PubSubSubscription` keeps both explicit. By
default the descriptor names an existing subscription, by short id or by full
`projects/{project}/subscriptions/{name}` resource name. It implements `SubscriptionSource`, so it
sits inline in the decorator:

```rust
--8<-- "crates/ruststream-gcp-pubsub/examples/pubsub_service.rs:handler"
```

Wiring it onto the broker is the framework's `with_broker` / `include` pair, identical to every
other broker:

```rust
--8<-- "crates/ruststream-gcp-pubsub/examples/pubsub_service.rs:app"
```

Four options ride the descriptor:

- `create_with_topic(topic)` creates the subscription and its topic on subscribe when they do not
  exist. It is meant for local development and tests against the emulator; production subscriptions
  are usually managed as infrastructure, and the plain `new(name)` form expects them to exist.
- `max_outstanding(n)` is flow control: how many received messages may be unacknowledged at once.
  This is the real prefetch, and it defaults to the client's 1000.
- `ack_extension(duration)` sets how far each background ack-deadline extension reaches while a
  handler runs. The client clamps it to the protocol's 10s to 600s range and defaults to 60s.
- `page_wait(duration)` sets how long a partial [page](#pages) waits for the rest of itself.
  It defaults to 50ms and matters only for a handler that takes a slice.

A descriptor that cannot form a subscription (an empty subscription or topic name) is rejected with
`PubSubError::InvalidDescriptor` before any I/O.

Each subscription is a streaming pull rendered as the framework's message `Stream`. The client
extends ack deadlines in the background for as long as a handler runs, so a slow handler does not
by itself cause redelivery. Dropping the subscriber signals the client's shutdown token, which
drains the stream.

The plain string form `#[subscriber("orders-workers")]` also works: a by-name source resolves to
`PubSubSubscription::new`, which requires the subscription to exist already.

## Pages

A handler that takes a slice consumes a whole page - one database round-trip, one bulk API call,
per page instead of per order:

```rust
--8<-- "crates/ruststream-gcp-pubsub/examples/pubsub_pages.rs:handler"
```

Mounting it is what every broker does the same way: the page size is the mount site's one word,
and it is mandatory, because there is no size the framework could invent.

```rust
--8<-- "crates/ruststream-gcp-pubsub/examples/pubsub_pages.rs:app"
```

Pub/Sub's streaming pull hands over one delivery at a time, so the pages are assembled on the
client rather than asked for on the wire. That is invisible at the mount site, which is the
point: the size is the framework's one word either way, and a page never carries more than it,
though it may carry fewer.

What is Pub/Sub's own is the other half - how long a page that is not yet full waits for the rest
of itself. `page_wait` on the descriptor sets that, and defaults to 50ms, roughly one
streaming-pull burst: shorter and most pages close on their first delivery, longer and a quiet
subscription holds a page back for nothing. Raise it for a workload whose batches earn their
round trip; leave it alone otherwise.

The other subscription settings are unaffected. `max_outstanding` is still flow control - how many
deliveries may be unacknowledged at once - and it bounds the pull, not the page: it is the
prefetch a page is assembled out of, not the size of the page.

The in-process test broker pages the same way, so a `&[T]` handler is unit-testable under
`TestApp` and not only against the emulator.

## Acknowledgement

Settlement is native per message, and it is the confirmed variant on a subscription with
exactly-once delivery enabled:

| Handler outcome | Pub/Sub call | Effect |
| --- | --- | --- |
| `HandlerOutcome::ack()` | acknowledge | the delivery is done |
| `HandlerOutcome::retry()` | nack | the message becomes available again and is redelivered |
| `HandlerOutcome::drop()` | acknowledge | the message is not redelivered |

`drop()` acknowledging is the product's model: Pub/Sub has no drop-without-redelivery verb, so
poison-message routing belongs to the subscription's dead-letter policy, which the service
configures on the subscription resource rather than per message. When a dead-letter policy is set,
the delivery-attempt count arrives as the `pubsub-delivery-attempt` header (exported as
`DELIVERY_ATTEMPT_HEADER`), so a handler can branch on how many times a message has come back.

There is no native delayed nack here, so `HandlerOutcome::retry_after(delay)` falls back to the
runtime's broker-agnostic deferred re-publish rather than a broker-side timer.

### Exactly-once acknowledgement

Exactly-once delivery is a subscription setting, enabled on the subscription resource. When it is
on, the client hands over an exactly-once handler, and this crate settles through the confirmed
forms: `Ok` from `ack` means the service accepted the acknowledgement and the message will not be
redelivered. A refused acknowledgement (an expired ack id, a lost deadline race) surfaces as
`AckError::Broker` instead of passing as success. On an ordinary subscription the plain
fire-and-forget forms are used, and `ack` reports success once the acknowledgement is queued.

The distinction is per delivery, not per configuration flag in this crate: the same handler code
runs against both kinds of subscription, and turning exactly-once on server side is what upgrades
the settlement path.

## Ordering keys and the partition key

Pub/Sub keeps per-key FIFO order through ordering keys, and the framework's partition key is the
same idea, so the two are one header. A `partition-key` header (exported as `PARTITION_KEY_HEADER`)
on an outgoing message becomes the message's ordering key, and a delivered message carries its
ordering key back under the same header, feeding the `Partitioned` capability.

A publish names its key with `with_ordering_key`, which adapts the publisher: one adapter serves a
run of publishes, and the rest of the chain (codec, headers, destination) is written as usual. The
key rides under the publish's own headers, so other headers named at the call travel with it and a
`partition-key` named there wins.

```rust
--8<-- "crates/ruststream-gcp-pubsub/examples/pubsub_ordered_publish.rs:ordered"
```

On an `Out` slot the step resolves on the slot itself, so a publish made through it keeps its slot
attribution and `tb.out::<Marker>()` sees it under the test harness.

Ordered delivery needs the subscription to have message ordering enabled, and a regional
`endpoint` is what keeps a key ordered across publishers in one region. A publish failure on an
ordered key pauses that key in the client; this crate resumes it and returns the failure, so one
error cannot silently wedge every later publish on the key.

Every other header rides the message's attributes directly, and attributes come back as headers. No
envelope format is invented, so non-Rust peers see plain Pub/Sub messages.

## Publishing

A publisher is a policy plus the live clients. `PubSubPublish` holds no connection, so it is
constructed anywhere (in a router, in configuration, at a mount site) and the runtime pairs it with
the broker at startup to produce a `PubSubPublisher`. It is also the broker's default publish
policy, so a `#[subscriber(.., publish("topic"))]` handler mounted without an explicit publisher
publishes through it.

A service writes two vocabularies, and the import says which one a file is in. A file of handler
bodies imports the framework's prelude alone and bounds a slot with a capability trait -
`Out(out): Out<impl Publisher>`, or `Out<impl PubSubOrdering>` for a body that also wants this
crate's ordering step - so it names no broker. A routes file imports
`ruststream_gcp_pubsub::prelude::*` and attaches policies under their mount-site names, so this
crate's policy arrives as `Publish` and a mount site writes `.publisher(Publish)` or
`.out(M, Publish)` whichever broker it runs on. This policy holds no options, so the name is the
whole expression. The prefixed `PubSubPublish` stays at the crate root for a file that speaks to
two brokers at once and has to say which one it means.

The destination name is the topic id, short or a full resource name. Per-topic client publishers
are created on first use and cached on the broker, which is what lets `shutdown` flush every
buffered batch instead of dropping it.

Every publish starts at `message(&value)` and ends at `publish()`, on an `Out` slot, on a publisher
held in state, and in a startup hook alike; the value's type picks the wire, so a `Serialize` model
encodes through the codec and a `#[derive(Serialized)]` type's bytes leave as they are.
`with_ordering_key` is this crate's one addition to that chain; message attributes are headers, and
everything else Pub/Sub takes per publish is a subscription-level setting.

A message built by hand and handed to `Publisher::publish` is sent as it is - the path to take
when the header map is what you want to control.

## The emulator

The Pub/Sub emulator is a supported target for local development and tests. `emulator(host)` wires
the plaintext endpoint and anonymous credentials in one call; the client does not read
`PUBSUB_EMULATOR_HOST` on its own, so the host is named explicitly.

```bash
just brokers-up    # docker compose up: gcloud beta emulators pubsub start on 8085
cargo run --example pubsub_service
just brokers-down
```

Against the emulator the resources usually do not exist yet, which is what
`create_with_topic` is for: the subscription and its topic are created on subscribe. Resource
creation is get-then-create, so two services racing on the same names both end up connected rather
than one failing.

The live test suite runs the same way, gated behind `PUBSUB_TEST_HOST`:

```bash
just test-brokers  # starts the emulator, runs the integration and conformance suites
```

## Testing

The `testing` feature ships `PubSubTestBroker`: an in-process transport that reproduces the crate's
core routing with no server and no network. It follows the same ladder as the real broker, and its
connected form implements `ruststream::testing::TestableBroker`, so the same broker drives the
`TestApp` harness and the framework's conformance suite. Inject traffic with
`broker.inject(OutgoingMessage::new(..))` and assert on published output with the free
`ruststream::testing::expect_published`. See
[Unit-testing a service with TestApp](https://powersemmi.github.io/ruststream/latest/guides/testing/#unit-testing-a-service-with-testapp).

The test broker routes by exact name match and does not simulate product behaviour (deadline
extension, redelivery timing, ordered delivery, dead-letter policies). Those are verified end to
end against the emulator, where the integration tests and the framework's conformance lifecycle
suite run.

Because it routes by name, the stand-in serves the by-name subscriber form
(`#[subscriber("orders-workers")]`). A handler that names a `PubSubSubscription` descriptor is
bound to the real broker, since the descriptor resolves a subscription against a topic and the
stand-in models neither; test those handlers by injecting on the connected stand-in directly, as
above.
