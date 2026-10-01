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

`ruststream-gcp-pubsub` connects a RustStream service to Google Cloud Pub/Sub over the official
[`google-cloud-pubsub`](https://crates.io/crates/google-cloud-pubsub) client. Handlers, routing,
codecs and middleware come from the framework; this crate is the transport.

## Features

- **Streaming pull** with ack deadlines extended in the background while a handler runs.
- **Native acknowledgement,** with the confirmed forms on exactly-once subscriptions.
- **Retry caps as the subscription's own dead-letter policy,** and delayed retries held under the
  delivery's lease.
- **Ordering keys** as a per-message setting.
- **Plain Pub/Sub messages:** headers ride attributes, with no envelope.
- **Batches** assembled on the client.
- **The emulator as a target,** with topics and subscriptions created on subscribe for local
  development.
- **AsyncAPI** under the `googlepubsub` protocol, behind the `asyncapi` feature.
- **Tests on the production app:** `TestApp` runs it with `PubSubBroker` connected in process.

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

#[subscriber(GooglePubSub::new("orders-workers").create_with_topic("orders"))]
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
            b.include(handle).out(DefaultSlot, Publish::default()).build();
        },
    )
}
```

`#[ruststream::app]` generates `main`, so the binary understands `run` and `asyncapi gen`. A plain
name subscribes to a subscription that already exists; `GooglePubSub` describes one with options.

## Test it

`TestApp` runs the app `main` runs with `PubSubBroker` connected in process, with no server.

```rust
use ruststream::testing::TestApp;

let tb = TestApp::start(app()).await?;

tb.broker::<PubSubBroker>()
    .message(&Order { id: 42 })
    .to("orders")
    .publish()
    .await?;

tb.out::<DefaultSlot>()
    .assert_called_once()
    .decoded_as::<Confirmation>()
    .with(&Confirmation { order_id: 42 });
```

## Documentation

- This crate: <https://docs.rs/ruststream-gcp-pubsub>
- The framework: <https://powersemmi.github.io/ruststream/latest>

## Minimum supported Rust version

The MSRV is **1.88**, edition 2024.

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md).

## License

Licensed under the [Apache-2.0](./LICENSE) license.
