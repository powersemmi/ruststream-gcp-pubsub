<h1 align="center">ruststream-gcp-pubsub</h1>

<p align="center">
  <i>The Google Cloud Pub/Sub broker for the <a href="https://github.com/powersemmi/ruststream">RustStream</a> messaging framework: streaming pull as a stream, native ack/nack, and subscription-level dead-lettering.</i>
</p>

<p align="center">
  <a href="https://github.com/powersemmi/ruststream-gcp-pubsub/actions/workflows/ci.yml"><img src="https://github.com/powersemmi/ruststream-gcp-pubsub/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://crates.io/crates/ruststream-gcp-pubsub"><img src="https://img.shields.io/crates/v/ruststream-gcp-pubsub.svg" alt="crates.io"></a>
  <a href="https://crates.io/crates/ruststream-gcp-pubsub"><img src="https://img.shields.io/crates/dr/ruststream-gcp-pubsub" alt="Recent downloads"></a>
  <a href="https://docs.rs/ruststream-gcp-pubsub"><img src="https://img.shields.io/docsrs/ruststream-gcp-pubsub" alt="docs.rs"></a>
  <img src="https://img.shields.io/badge/MSRV-1.88-blue.svg" alt="MSRV 1.88">
  <img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="License">
  <a href="https://t.me/ruststream_community"><img src="https://img.shields.io/badge/-Telegram-blue?logo=telegram&label=News" alt="Telegram news channel"></a>
  <a href="https://t.me/ruststream_communuty_ru_chat"><img src="https://img.shields.io/badge/-Telegram-blue?logo=telegram&label=RU" alt="Telegram RU chat"></a>
</p>

<p align="center">
  <b><a href="https://powersemmi.github.io/ruststream-gcp-pubsub/">Documentation</a></b>
</p>

---

`ruststream-gcp-pubsub` implements the RustStream broker contract over the official [`google-cloud-pubsub`](https://crates.io/crates/google-cloud-pubsub) client. Handlers, routers, codecs, and middleware come from the framework; this crate supplies the transport - and nothing broker-specific leaks back into the framework.

## Features

- **Lazy startup contract.** `PubSubBroker::new(project)` is synchronous and does no I/O (Application Default Credentials by default; explicit `credentials`, a regional `endpoint`, or a local `emulator` as builder options); the runtime connects once at startup, so the broker composes with `#[ruststream::app]`.
- **Streaming pull as the message stream.** Each subscription is a `Stream` of deliveries; the client extends ack deadlines in the background while a handler runs, so a slow handler does not cause redelivery.
- **Batches for slice handlers.** A `&[T]` handler names its batch size at the mount site (`.batch(nonzero!(50))`) like on any broker; the pull hands over one delivery at a time, so the batches are assembled on the client, with `GooglePubSub::batch_wait` closing a partial one.
- **Native acknowledgement.** `HandlerOutcome::ack()` and `retry()` map onto the product directly (with the confirmed forms on exactly-once subscriptions). `drop()` acknowledges: Pub/Sub has no drop-without-redelivery verb - poison routing belongs to the subscription's dead-letter policy, and the delivery-attempt count is surfaced as a header.
- **Ordering keys as the partition key.** A publish names its key with `with_ordering_key`; the key travels as the `partition-key` header, under the publish's own headers, and comes back as the same header (feeding `Partitioned`) on delivery.
- **Attributes carry headers directly** - no envelope format is invented, and a `#[derive(Serialized)]` payload leaves as its own bytes with no codec in the way, so non-Rust peers see plain Pub/Sub messages.
- **Emulator as a supported target.** `PubSubBroker::new(p).emulator("localhost:8085")` wires the plaintext endpoint and anonymous credentials (the client does not honour `PUBSUB_EMULATOR_HOST` on its own), and `GooglePubSub::create_with_topic` creates the resources on subscribe for local development.
- **In-process test broker** (feature `testing`). `PubSubTestBroker` reproduces this crate's routing with no server, a service mounts on it and runs under the `TestApp` harness, and it answers the way Pub/Sub does, which the crate's own tests hold it to.

## Install

```toml
[dependencies]
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-gcp-pubsub = "0.7"
serde = { version = "1", features = ["derive"] }

[dev-dependencies]
ruststream-gcp-pubsub = { version = "0.7", features = ["testing"] }
```

## Write a service

```rust
use ruststream_gcp_pubsub::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Outgoing, Serialize)]
struct Order {
    id: u64,
}

#[derive(Debug, Deserialize, PartialEq, Serialize, Outgoing)]
struct Confirmation {
    order_id: u64,
}

#[subscriber("orders-workers")]
async fn handle(order: &Order, Out(out): Out<impl Publisher>) -> HandlerOutcome {
    if out
        .message(&Confirmation { order_id: order.id })
        .to("confirmations")
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        PubSubBroker::new("my-project"),
        |b| {
            b.include(handle).out(DefaultSlot, Publish).build();
        },
    )
}
```

`ruststream_gcp_pubsub::prelude` is the whole import list: the framework's own prelude plus this crate's surface. `Publish` in it is this crate's publish policy under the uniform mount-site name, so `.out(marker, Publish)` reads the same whichever broker it runs on; the handler body states a capability instead (`Out<impl Publisher>`, or `Out<impl PubSubOrdering>` when it wants the ordering step) and names no broker at all.

A plain name subscribes to a subscription that already exists. `GooglePubSub` goes in the same slot when the subscription needs options - `#[subscriber(GooglePubSub::new("orders-workers").max_outstanding(1_000))]` sets flow control, `ack_extension` the deadline reach, `batch_wait` how long a partial batch waits, and `create_with_topic("orders")` creates the subscription (and topic) on subscribe, which the emulator workflow needs.

## Test it

The `testing` feature runs handlers against an in-process Pub/Sub stand-in - no server, same routing, same ladder. Swapping the broker at the mount site is the whole change: the declaration keeps its descriptor and `Publish` pairs against the stand-in as it pairs against Pub/Sub. The `TestApp` harness then drives the service through the dispatch path production uses:

```rust
use ruststream::testing::TestApp;
use ruststream_gcp_pubsub::testing::PubSubTestBroker;

let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
    PubSubTestBroker::new(),
    |b| {
        b.include(handle).out(DefaultSlot, Publish).build();
    },
);
let tb = TestApp::start(app).await?;

// Inject an order as an external producer would; the harness drives the handler to rest.
tb.broker::<PubSubTestBroker>()
    .message(&Order { id: 42 })
    .to("orders-workers")
    .publish()
    .await?;
tb.settle().await?;

// The handler published the matching confirmation through its slot.
tb.out::<DefaultSlot>()
    .assert_called_once()
    .decoded_as::<Confirmation>()
    .with(&Confirmation { order_id: 42 });
```

The harness puts an `Order` on the wire and reads a `Confirmation` back, so each model carries two derives more than the service alone needs: `Outgoing` and `Serialize` on the injected type, `Deserialize` and `PartialEq` on the asserted one.

The stand-in routes by one address, the subscription name, so a test injects there rather than to a topic: it holds no topics, and the topic-to-subscription binding is the product's. Product behaviour (deadline extension, redelivery timing, ordered delivery) is not modelled either. Both are covered by the env-gated live suite instead: `just test-brokers` starts the emulator and runs the integration tests plus the framework conformance lifecycle against it.

## Layout

```
ruststream-gcp-pubsub/
├── crates/
│   └── ruststream-gcp-pubsub/  the published crate
│       └── examples/           runnable pubsub_* examples (docs-site snippet sources)
├── docs/                       the documentation site (properdocs + Material)
├── docker-compose.test.yml     the Pub/Sub emulator for the live suite
├── properdocs.yml              docs site config
└── Cargo.toml                  workspace
```

The Pub/Sub guide, including the acknowledgement, ordering-key, emulator, and capability coverage, lives at [powersemmi.github.io/ruststream-gcp-pubsub](https://powersemmi.github.io/ruststream-gcp-pubsub/). Framework concepts (subscribers, routing, codecs, middleware, the CLI) live in the [RustStream docs](https://powersemmi.github.io/ruststream/).

## Contributing

```bash
just check          # fmt, clippy, feature checks
just test           # handler-stub tests, no server
just test-brokers   # live integration + conformance against the emulator
```

## License

Licensed under the [Apache-2.0](./LICENSE) license.
