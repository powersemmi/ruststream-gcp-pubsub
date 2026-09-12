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

## Where to go next

<div class="grid cards" markdown>

- :material-transit-connection-variant: **[Pub/Sub guide](pubsub.md)** - subscriptions, acknowledgement, ordering keys, the emulator, and testing.
- :material-book-open-variant: **[RustStream docs](https://powersemmi.github.io/ruststream/)** - the framework itself: subscribers, routing, codecs, middleware, the CLI.
- :material-language-rust: **[API reference](https://docs.rs/ruststream-gcp-pubsub)** - the crate's rustdoc on docs.rs.

</div>

## How this site relates to the RustStream docs

This site documents the Pub/Sub broker only. Writing subscribers, publishing, routing, codecs,
middleware, observability and the CLI live in the
[RustStream documentation](https://powersemmi.github.io/ruststream/).
