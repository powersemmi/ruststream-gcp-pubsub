# ruststream-gcp-pubsub

**`ruststream-gcp-pubsub`** is the Google Cloud Pub/Sub broker for the
[RustStream](https://powersemmi.github.io/ruststream/) messaging framework, built on the official
[`google-cloud-pubsub`](https://docs.rs/google-cloud-pubsub) client. A streaming pull subscription
becomes the framework's message stream, acknowledgement is native per message (with the confirmed
forms on exactly-once subscriptions), and ordering keys map onto the framework's partition key.

Handlers, routers, codecs, and middleware come from the framework; this crate supplies the
transport, and nothing broker-specific leaks back into the framework.

```toml
ruststream = { version = "0.6", features = ["macros", "json"] }
ruststream-gcp-pubsub = { git = "https://github.com/powersemmi/ruststream-gcp-pubsub" }
serde = { version = "1", features = ["derive"] }
```

The crate is built on the `ruststream` 0.6 line from crates.io and is not published to crates.io
itself yet, so it is used as a git dependency until the first release. Its MSRV is 1.88, tracking
the official client; the framework core stays at 1.85.

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

This site documents the Pub/Sub broker only. Framework concepts that apply to every broker (writing
subscribers, publishing, routing, codecs, middleware, observability, the CLI) live in the
[RustStream documentation](https://powersemmi.github.io/ruststream/). The pages here cover what is
specific to Pub/Sub and link back to the framework docs where the two meet.
