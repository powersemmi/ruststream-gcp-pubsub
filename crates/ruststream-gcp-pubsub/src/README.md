Google Cloud Pub/Sub broker for the [RustStream](https://github.com/powersemmi/ruststream)
messaging framework, backed by the official
[`google-cloud-pubsub`](https://docs.rs/google-cloud-pubsub) client.

A service publishes to a topic, and a handler reads a subscription attached to it through one
streaming pull. Pub/Sub acknowledges each message on its own, and a subscription's dead-letter
policy, not the service, moves a message that has had its last delivery. The ordering key of a
Pub/Sub message is the framework's partition key.

# A service

A handler is an `async fn` over the decoded payload, mounted on a broker inside the
application object, and the attribute writes the `main`:

```
# mod demo {
use ruststream_gcp_pubsub::prelude::*;
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u64,
}

#[subscriber(GooglePubSub::new("orders-workers").create_with_topic("orders"))]
async fn handle(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "accepted");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        PubSubBroker::new("my-project").emulator("localhost:8085"),
        |b| {
            b.include(handle);
        },
    )
}
# }
# fn main() {}
```

`cargo run -- run` starts it against a local emulator. [`PubSubBroker::new`] only records the
project; the runtime connects the broker at startup and flushes its publishers when the service
stops.

# Where things are

The [guides][guides] take a Pub/Sub service from installation to production. The
[architecture][architecture] pages show how the crate sits between `google-cloud-pubsub` and the
runtime, and the [benchmarks][benchmarks] what it costs.

Each topic below links to its component page on the site, which shows it in a service; the
item's own documentation keeps the detail.

## Guides

- [Getting started][g-getting-started]: install the crate, run a first subscriber against the emulator and test it.
- [Topics and subscriptions][g-topics-and-subscriptions]: fan-out, replicas sharing a subscription, topology the service or operations own.
- [Publishing and replies][g-publishing]: replies whose type names the topic, `Out` slots and a publish at startup.
- [Acknowledgement and retries][g-retries]: ack, retry, `retry_after` and drop on Pub/Sub, the dead-letter policy and exactly-once.
- [Flow control and leases][g-flow-control]: `max_outstanding`, `ack_extension` and `max_lease`.
- [Ordering keys][g-ordering]: ordered subscriptions, keyed lanes and the key on what a service publishes.
- [Batch processing][g-batches]: a slice handler, the batch size and wait, settling each message.
- [Running in production][g-production]: credentials, a regional endpoint, operations-owned topology and IAM roles.
- [Testing a Pub/Sub service][g-testing]: one test body in process and against the emulator.

## Broker

- [PubSubBroker][c-broker]: [`PubSubBroker`], connected into [`ConnectedPubSubBroker`].
- [emulator][c-emulator]: [`PubSubBroker::emulator`], a local Pub/Sub emulator with anonymous
  credentials.
- [credentials][c-credentials]: [`PubSubBroker::credentials`] in place of Application Default
  Credentials.
- [endpoint][c-endpoint]: [`PubSubBroker::endpoint`], another endpoint, such as a locational one.
- [Startup][c-startup]: the clients built at connect, then each subscription's topology and
  streaming pull.
- [Shutdown][c-shutdown]: the flush of every publisher, and [`PubSubError::NotConnected`] for a
  publisher used afterwards.
- [Errors][c-errors]: [`PubSubError`].
- [Prelude][c-prelude]: [`prelude`], the one glob a Pub/Sub service imports.

## Subscriptions

- [Subscriptions][c-subscriptions]: a bare subscription name in `#[subscriber]`.
- [GooglePubSub][c-google-pubsub]: [`GooglePubSub`], one subscription and every setting it takes,
  read through a [`PubSubSubscriber`].
- [name][c-name]: `#[subscriber(GooglePubSub)]`, with the name given at the mount site.
- [create_with_topic][c-create-with-topic]: [`GooglePubSub::create_with_topic`], the subscription
  and its topic created at startup.
- [max_outstanding][c-max-outstanding]: [`GooglePubSub::max_outstanding`], the cap on unsettled
  deliveries.
- [ack_extension][c-ack-extension]: [`GooglePubSub::ack_extension`], how far each lease extension
  reaches.
- [max_lease][c-max-lease]: [`GooglePubSub::max_lease`], how long one delivery stays leased.
- [Batches][c-batches]: a slice handler, with [`GooglePubSub::batch_wait`] for a partial batch.

## Settlement

- [Acknowledgement][c-settlement]: what `ack`, `retry` and `drop` do to a [`PubSubMessage`],
  exactly-once subscriptions included.
- [retry_after][c-retry-after]: a delivery held under its lease, then nacked.
- [Dead letters][c-dead-letters]: `max_attempts` and `dead_letter` as the subscription's own
  dead-letter policy.
- [Attempts][c-attempts]: [`DELIVERY_ATTEMPT_HEADER`], the attempt number a delivery reports.

## Publishing

- [PubSubPublish][c-publishing]: [`PubSubPublish`], the publish policy, and its live
  [`PubSubPublisher`].
- [Headers][c-headers]: headers as Pub/Sub attributes.
- [Ordering][c-ordering]: messages that share an ordering key, in the order they were published.
- [ordering_key][c-ordering-key]: [`PubSubOrdering::ordering_key`], the key of one publish, written
  into [`PubSubPublishOptions`].
- [Policy][c-policy]: [`PubSubPublish::ordering_key`], one key for a whole mount site.
- [PARTITION_KEY_HEADER][c-partition-key]: [`PARTITION_KEY_HEADER`], the portable spelling of the
  key.
- [Transform][c-transform]: a reply transform that sets the key per delivery.

## Tooling

- [Bindings][c-asyncapi]: the `googlepubsub` bindings of the generated document (feature
  `asyncapi`): the [server][c-asyncapi-server], the [messages][c-asyncapi-messages] and the
  [channels][c-asyncapi-channels].
- [Testing][c-testing]: the in-process Pub/Sub
  [`TestApp`](https://docs.rs/ruststream/latest/ruststream/testing/struct.TestApp.html) connects
  the broker to (feature `testing`), and [Live][c-live] runs the same test against the emulator.

# Cargo features

`testing` adds the in-process mode `TestApp::start` connects the broker through, and `asyncapi`
contributes the `googlepubsub` bindings to the generated document. The codecs are the
framework's features, on the `ruststream` dependency.

[guides]: https://powersemmi.github.io/ruststream-gcp-pubsub/latest/
[g-getting-started]: https://powersemmi.github.io/ruststream-gcp-pubsub/latest/guides/getting-started/
[g-topics-and-subscriptions]: https://powersemmi.github.io/ruststream-gcp-pubsub/latest/guides/topics-and-subscriptions/
[g-publishing]: https://powersemmi.github.io/ruststream-gcp-pubsub/latest/guides/publishing/
[g-retries]: https://powersemmi.github.io/ruststream-gcp-pubsub/latest/guides/retries/
[g-flow-control]: https://powersemmi.github.io/ruststream-gcp-pubsub/latest/guides/flow-control/
[g-ordering]: https://powersemmi.github.io/ruststream-gcp-pubsub/latest/guides/ordering/
[g-batches]: https://powersemmi.github.io/ruststream-gcp-pubsub/latest/guides/batches/
[g-production]: https://powersemmi.github.io/ruststream-gcp-pubsub/latest/guides/production/
[g-testing]: https://powersemmi.github.io/ruststream-gcp-pubsub/latest/guides/testing/
[architecture]: https://powersemmi.github.io/ruststream-gcp-pubsub/latest/architecture/
[benchmarks]: https://powersemmi.github.io/ruststream-gcp-pubsub/latest/architecture/benchmarks/
[c-broker]: https://powersemmi.github.io/ruststream-gcp-pubsub/latest/components/broker/
[c-emulator]: https://powersemmi.github.io/ruststream-gcp-pubsub/latest/components/broker/emulator/
[c-credentials]: https://powersemmi.github.io/ruststream-gcp-pubsub/latest/components/broker/credentials/
[c-endpoint]: https://powersemmi.github.io/ruststream-gcp-pubsub/latest/components/broker/endpoint/
[c-startup]: https://powersemmi.github.io/ruststream-gcp-pubsub/latest/components/broker/startup/
[c-shutdown]: https://powersemmi.github.io/ruststream-gcp-pubsub/latest/components/broker/shutdown/
[c-errors]: https://powersemmi.github.io/ruststream-gcp-pubsub/latest/components/broker/errors/
[c-prelude]: https://powersemmi.github.io/ruststream-gcp-pubsub/latest/components/broker/prelude/
[c-subscriptions]: https://powersemmi.github.io/ruststream-gcp-pubsub/latest/components/subscriptions/
[c-google-pubsub]: https://powersemmi.github.io/ruststream-gcp-pubsub/latest/components/subscriptions/google-pubsub/
[c-name]: https://powersemmi.github.io/ruststream-gcp-pubsub/latest/components/subscriptions/name/
[c-create-with-topic]: https://powersemmi.github.io/ruststream-gcp-pubsub/latest/components/subscriptions/create-with-topic/
[c-max-outstanding]: https://powersemmi.github.io/ruststream-gcp-pubsub/latest/components/subscriptions/max-outstanding/
[c-ack-extension]: https://powersemmi.github.io/ruststream-gcp-pubsub/latest/components/subscriptions/ack-extension/
[c-max-lease]: https://powersemmi.github.io/ruststream-gcp-pubsub/latest/components/subscriptions/max-lease/
[c-batches]: https://powersemmi.github.io/ruststream-gcp-pubsub/latest/components/subscriptions/batches/
[c-settlement]: https://powersemmi.github.io/ruststream-gcp-pubsub/latest/components/settlement/
[c-retry-after]: https://powersemmi.github.io/ruststream-gcp-pubsub/latest/components/settlement/retry-after/
[c-dead-letters]: https://powersemmi.github.io/ruststream-gcp-pubsub/latest/components/settlement/dead-letters/
[c-attempts]: https://powersemmi.github.io/ruststream-gcp-pubsub/latest/components/settlement/attempts/
[c-publishing]: https://powersemmi.github.io/ruststream-gcp-pubsub/latest/components/publishing/
[c-headers]: https://powersemmi.github.io/ruststream-gcp-pubsub/latest/components/publishing/headers/
[c-ordering]: https://powersemmi.github.io/ruststream-gcp-pubsub/latest/components/publishing/ordering/
[c-ordering-key]: https://powersemmi.github.io/ruststream-gcp-pubsub/latest/components/publishing/ordering/ordering-key/
[c-policy]: https://powersemmi.github.io/ruststream-gcp-pubsub/latest/components/publishing/ordering/policy/
[c-partition-key]: https://powersemmi.github.io/ruststream-gcp-pubsub/latest/components/publishing/ordering/partition-key/
[c-transform]: https://powersemmi.github.io/ruststream-gcp-pubsub/latest/components/publishing/ordering/transform/
[c-asyncapi]: https://powersemmi.github.io/ruststream-gcp-pubsub/latest/components/asyncapi/
[c-asyncapi-server]: https://powersemmi.github.io/ruststream-gcp-pubsub/latest/components/asyncapi/server/
[c-asyncapi-messages]: https://powersemmi.github.io/ruststream-gcp-pubsub/latest/components/asyncapi/messages/
[c-asyncapi-channels]: https://powersemmi.github.io/ruststream-gcp-pubsub/latest/components/asyncapi/channels/
[c-testing]: https://powersemmi.github.io/ruststream-gcp-pubsub/latest/components/testing/
[c-live]: https://powersemmi.github.io/ruststream-gcp-pubsub/latest/components/testing/live/
