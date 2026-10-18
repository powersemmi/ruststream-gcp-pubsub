---
sidebar_position: 7
title: Prelude
description: "One glob, ruststream_gcp_pubsub::prelude, brings the core prelude, the broker, the descriptor, the publish policy and the ordering step."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Prelude

One glob, [`ruststream_gcp_pubsub::prelude`](api:ruststream_gcp_pubsub::prelude), brings the core prelude, the broker, the descriptor, the publish policy and the ordering step.

`use ruststream_gcp_pubsub::prelude::*;` is the one import a Pub/Sub service writes.
It carries the core's own prelude, so `#[subscriber]`, `RustStream`, `HandlerOutcome` and `Out` come with it.
It adds [`PubSubBroker`](./index.md), the descriptor [`GooglePubSub`](../subscriptions/google-pubsub.md) and the live [`PubSubPublisher`](api:ruststream_gcp_pubsub::PubSubPublisher).

The publish policy comes under the name every broker crate uses: `Publish` is [`PubSubPublish`](../publishing/index.md).
So a mount site reads the same on every broker.
Two broker preludes in one file collide on this name; the prefixed type at the crate root tells them apart.

A handler body bounds its slots with capability traits, such as `Out<impl Publisher>`, and names no broker type.
The one exception is a body that names an ordering key on one publish.
The [`ordering_key`](../publishing/ordering/ordering-key.md) step needs [`PubSubOrdering`](api:ruststream_gcp_pubsub::PubSubOrdering) in scope and the slot bounded `Out<impl Publisher<Options = PubSubPublishOptions>>`, both from this prelude.
The constants [`PARTITION_KEY_HEADER`](../publishing/ordering/partition-key.md) and [`DELIVERY_ATTEMPT_HEADER`](../settlement/attempts.md) stay at the crate root.

## Example

The descriptor, the broker and the reply policy all come from the one glob.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pubsub_broker_prelude.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pubsub_broker_prelude.rs#app"
```

</TabItem>
</Tabs>
