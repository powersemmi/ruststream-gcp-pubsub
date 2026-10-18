---
title: Subscriptions
description: "A name in #[subscriber] reads that existing Pub/Sub subscription; the GooglePubSub descriptor carries every other setting of one."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Subscriptions

A name in `#[subscriber]` reads that existing Pub/Sub subscription; the [`GooglePubSub`](./google-pubsub.md) descriptor carries every other setting of one.

Pub/Sub keeps topics and subscriptions apart.
A publisher sends to a topic, and a subscription attached to the topic keeps every message published after it was created.
Each subscription of a topic receives every message, and the consumers of one subscription share its messages.
A service reads a subscription, never a topic.

`#[subscriber("orders-workers")]` reads the subscription `orders-workers`, which the deployment created on its topic.
The name is a short id in the broker's project or a full resource name, `projects/{project}/subscriptions/{id}`.
Nothing is created at startup, and a subscription that does not exist fails on its stream with [`PubSubError::Receive`](api:ruststream_gcp_pubsub::PubSubError::Receive).
A [dead-letter declaration](../settlement/dead-letters.md) at the mount site reaches a subscription named this way too.
Every other setting belongs to the descriptor, which goes where the name goes, as the attribute's first argument.
The manual form passes the name to [`subscriber(name, body)`](api:ruststream::runtime::subscriber), and a descriptor goes in the same place, `subscriber(descriptor, body)`.
Two handlers on one subscription are two consumers of it: each message goes to one of them.

## Example

The subscription `orders-workers` of the project `my-project`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pubsub_subscriptions.rs#name"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pubsub_subscriptions.rs#name"
```

</TabItem>
</Tabs>
