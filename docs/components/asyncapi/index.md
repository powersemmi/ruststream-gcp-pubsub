---
title: Bindings
description: "With the asyncapi feature the generated document speaks googlepubsub: the server's host and the ordering key a mount site fixed."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Bindings

With the `asyncapi` feature the generated document speaks `googlepubsub`: the server's host and the ordering key a mount site fixed.

The core builds the AsyncAPI document from the app, before anything connects.
This crate adds what it knows about Pub/Sub to the parts the core writes.
A [server](./server.md) reports the host clients dial, under the protocol `googlepubsub`.
A [message](./messages.md) carries the specification's `googlepubsub` binding, version `0.2.0`, where its mount site fixed an ordering key.
The [channels](./channels.md) are the subscriptions the service reads and the topics it publishes to.

Everything comes from the service's own declarations: the broker and the publish policies.
The document is published and shared, so no part of it carries a credential.
The binary `#[ruststream::app]` writes prints the document with `asyncapi gen`.

## Example

The dependency line:

```toml
ruststream-gcp-pubsub = { version = "0.7", features = ["asyncapi"] }
```

The service registers its broker under the label `pubsub`, the key of its server in the document.
`cargo run --example pubsub_asyncapi --features asyncapi -- asyncapi gen` prints the document.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pubsub_asyncapi.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pubsub_asyncapi.rs#app"
```

</TabItem>
</Tabs>
