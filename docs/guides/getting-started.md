---
sidebar_position: 1
description: "Install ruststream-gcp-pubsub, start the Pub/Sub emulator, run a first subscriber against it and test it without the emulator."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Getting started

This guide installs `ruststream-gcp-pubsub`, starts the Pub/Sub emulator and runs a first service against it.

## Installation

```toml title="Cargo.toml"
[dependencies]
ruststream = { version = "0.7", features = ["macros", "json", "logging"] }
ruststream-gcp-pubsub = "0.7"
serde = { version = "1", features = ["derive"] }
tracing = "0.1"

[dev-dependencies]
ruststream-gcp-pubsub = { version = "0.7", features = ["testing"] }
tokio = { version = "1", features = ["macros", "rt"] }
```

`ruststream` is the framework: `macros` gives the attributes, `json` the default codec, and `logging` prints the service's log to the terminal.
`ruststream-gcp-pubsub` is the Pub/Sub broker.
Its `testing` feature adds the [in-process Pub/Sub](../components/testing/index.md) that tests run on.

The broker crate has two features, both off by default:

| Feature | What it gives |
|---|---|
| `testing` | the in-process Pub/Sub behind `TestApp` |
| `asyncapi` | the [Pub/Sub bindings](../components/asyncapi/index.md) in the generated AsyncAPI document |

The client underneath is [google-cloud-pubsub](https://docs.rs/google-cloud-pubsub), written in Rust, so the build needs no system libraries.
The crate needs Rust 1.95 or newer.

## A local emulator

Google ships a Pub/Sub emulator in the Cloud SDK image:

```yaml title="compose.yaml"
services:
  pubsub:
    image: gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators
    command: gcloud beta emulators pubsub start --host-port=0.0.0.0:8085 --project=my-project
    ports:
      - "8085:8085"
```

```bash
docker compose up -d
```

The emulator listens on `localhost:8085`.
It starts empty and accepts any project id.

## The first service

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/getting_started.rs#service"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/getting_started.rs#service"
```

</TabItem>
</Tabs>

`take_order` reads JSON orders from the subscription `orders-workers`.
It takes an order with an amount and drops one without.
A Pub/Sub service reads a subscription, never a topic.
[`GooglePubSub`](../components/subscriptions/google-pubsub.md) describes the subscription, and [`create_with_topic`](../components/subscriptions/create-with-topic.md) creates it at startup on the topic `orders`, with the topic itself.
[`PubSubBroker::new`](../components/broker/index.md) takes the project id and only records it.
[`emulator`](../components/broker/emulator.md) points the broker at the local emulator, because the Rust client does not read `PUBSUB_EMULATOR_HOST`.
The connection opens when the service starts.
`#[ruststream::app]` turns `app` into `main`; in the manual form, [`run_main`](api:ruststream::runtime::cli::run_main) does the same.

Run the service:

```bash
cargo run -- run
```

Publish an order from another terminal, through the emulator's REST API:

```bash
curl -X POST localhost:8085/v1/projects/my-project/topics/orders:publish \
  -H 'content-type: application/json' \
  -d "{\"messages\": [{\"data\": \"$(printf '{"id":1,"amount_cents":1250}' | base64)\"}]}"
```

Pub/Sub carries the message body as base64 in its REST API, so the order is encoded first.
The service logs `order taken` with the order's id.
<kbd>Ctrl</kbd>+<kbd>C</kbd> stops it.

## The first test

The example file also holds two tests.
Each starts the same `app` with `TestApp::start`, publishes an order and checks how `take_order` settled it.
`PubSubBroker` connects to an in-process Pub/Sub there, so the tests need no emulator:

```bash
cargo test
```

[Testing a Pub/Sub service](testing.md) walks through such tests and what the in-process Pub/Sub models.

## Coming from google-cloud-pubsub

A service on google-cloud-pubsub builds its clients, pulls in a loop and publishes by hand.
Here is where each part goes in RustStream:

| google-cloud-pubsub | RustStream |
|---|---|
| `Subscriber::builder()` with `with_endpoint` and `with_credentials` | [`PubSubBroker::new`](../components/broker/index.md), with [`endpoint`](../components/broker/endpoint.md), [`credentials`](../components/broker/credentials.md) or [`emulator`](../components/broker/emulator.md) |
| `subscriber.subscribe(name)` and a loop over the message stream | a `#[subscriber(..)]` handler |
| `set_max_outstanding_messages`, `set_max_lease_extension`, `set_max_lease` | [`max_outstanding`](../components/subscriptions/max-outstanding.md), [`ack_extension`](../components/subscriptions/ack-extension.md), [`max_lease`](../components/subscriptions/max-lease.md) on the descriptor ([Flow control and leases](flow-control.md)) |
| `create_topic` and `create_subscription` on the admin clients | [`create_with_topic`](../components/subscriptions/create-with-topic.md) ([Topics and subscriptions](topics-and-subscriptions.md)) |
| `serde_json::from_slice` over the message data | the handler's parameter type, decoded by the codec |
| `handler.ack()`, `handler.nack()` | the handler's outcome: `ack`, `retry`, `retry_after`, `drop` ([Acknowledgement and retries](retries.md)) |
| `Publisher::publish` | a reply or an `Out` parameter, sent through the [`Publish`](../components/publishing/index.md) policy ([Publishing and replies](publishing.md)) |
| `Message::set_ordering_key` | the [`ordering_key`](../components/publishing/ordering/ordering-key.md) step ([Ordering keys](ordering.md)) |
