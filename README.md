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
- **Native acknowledgement.** `ack` and `nack(requeue = true)` map onto the product directly (with the confirmed forms on exactly-once subscriptions). `nack(requeue = false)` acknowledges: Pub/Sub has no drop-without-redelivery verb - poison routing belongs to the subscription's dead-letter policy, and the delivery-attempt count is surfaced as a header.
- **Ordering keys as the partition key.** A publish names its key with `with_ordering_key`; the key travels as the `partition-key` header, under the publish's own headers, and comes back as the same header (feeding `Partitioned`) on delivery.
- **Attributes carry headers directly** - no envelope format is invented; non-Rust peers see plain Pub/Sub messages.
- **Emulator as a supported target.** `PubSubBroker::new(p).emulator("localhost:8085")` wires the plaintext endpoint and anonymous credentials (the client does not honour `PUBSUB_EMULATOR_HOST` on its own), and `PubSubSubscription::create_with_topic` creates the resources on subscribe for local development.
- **In-process test broker** (feature `testing`). `PubSubTestBroker` reproduces core routing with no server, implements `ruststream::testing::TestableBroker`, and passes the framework's conformance suite in process.

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
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

#[subscriber(PubSubSubscription::new("orders-workers").max_outstanding(1_000))]
async fn handle(order: &Order) -> HandlerResult {
    println!("got order {}", order.id);
    HandlerResult::Ack
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .with_broker(PubSubBroker::new("my-project"), |b| b.include(handle))
}
```

`ruststream_gcp_pubsub::prelude` is the whole import list: the framework's own prelude plus this crate's surface.

The descriptor names an existing subscription; `create_with_topic("orders")` opts into creating the subscription (and topic) on subscribe, which the emulator workflow needs.

## Test it

The `testing` feature runs handlers against an in-process Pub/Sub stand-in - no server, same routing, same ladder. Inject a message as an external producer would with `TestableBroker::inject`, then assert on what a handler published with the free `expect_published`:

```rust
use ruststream::{Broker, OutgoingMessage};
use ruststream::testing::{TestableBroker, expect_published};
use ruststream_gcp_pubsub::testing::PubSubTestBroker;

let broker = PubSubTestBroker::new().connect().await?;
broker.inject(OutgoingMessage::new("orders-workers", br#"{"id":1}"#));
let confirmations =
    expect_published(&broker, "confirmations", 1, std::time::Duration::from_secs(1)).await;
```

Product behaviour (deadline extension, redelivery, ordered delivery) is covered by the env-gated live suite instead: `just test-brokers` starts the emulator and runs the integration tests plus the framework conformance lifecycle against it.

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
