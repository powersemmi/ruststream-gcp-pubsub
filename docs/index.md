# ruststream-gcp-pubsub

**`ruststream-gcp-pubsub`** runs a [RustStream](https://powersemmi.github.io/ruststream/) service on
Google Cloud Pub/Sub, over the official
[`google-cloud-pubsub`](https://docs.rs/google-cloud-pubsub) client.

A streaming pull subscription is the message stream a handler reads. Acknowledgement is native and
per message. On an exactly-once subscription acknowledgement takes the confirmed form. An ordering
key is the framework's partition key.

```toml
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-gcp-pubsub = "0.7"
serde = { version = "1", features = ["derive"] }
```

The crate is published on crates.io and tracks the `ruststream` 0.7 line. Its MSRV is 1.88, set by
the official client.

You assemble a Pub/Sub service in the app function:

```rust
--8<-- "crates/ruststream-gcp-pubsub/examples/pubsub_service.rs:app"
```

## What the crate gives you

One [subscription descriptor](https://docs.rs/ruststream-gcp-pubsub/latest/ruststream_gcp_pubsub/index.html#subscribing)
carries every setting a subscription has: flow control, how far the ack deadline is extended, how
long a partial batch waits, and how long one delivery may stay unsettled. A retry cap and a
dead-letter topic named at the mount site become the subscription's own dead-letter policy, so the
service publishes no retry copies.
[Publishing](https://docs.rs/ruststream-gcp-pubsub/latest/ruststream_gcp_pubsub/index.html#publishing)
adds one per-message setting, the ordering key, which a call site, a partition key header or the
mount site may name. With the
[`testing` feature](https://docs.rs/ruststream-gcp-pubsub/latest/ruststream_gcp_pubsub/index.html#testing),
a test runs the app `main` runs, with `PubSubBroker` connected in process and no server.

## Where to go next

<div class="grid cards" markdown>

- :material-language-rust: **[Crate reference](https://docs.rs/ruststream-gcp-pubsub)** - subscribing, publishing, the prelude, the `AsyncAPI` document, testing, operations.
- :material-book-open-variant: **[RustStream docs](https://powersemmi.github.io/ruststream/)** - installation, the tutorial, the list of brokers.
- :material-transit-connection-variant: **[Framework reference](https://docs.rs/ruststream/latest/ruststream/runtime/index.html)** - writing subscribers, routing, codecs, middleware, the CLI.

</div>

## How this site relates to the RustStream docs

This site is the entry page of the Pub/Sub broker. Everything the broker itself offers is on
[docs.rs](https://docs.rs/ruststream-gcp-pubsub), and the framework is documented with its own
crate and on the [RustStream site](https://powersemmi.github.io/ruststream/).
