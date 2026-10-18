---
sidebar_position: 4
title: Startup
description: "At startup the broker builds its clients, then each subscription creates or updates its topology and opens a streaming pull."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Startup

At startup the broker builds its clients, then each subscription creates or updates its topology and opens a streaming pull.

The runtime connects the broker once, after the synchronous builder has run.
The connect builds four clients of the Google client library: a subscriber, a publisher, and the topic and subscription admin clients.
The clients open their connections on first use.

Then each subscription opens in turn.
A descriptor the service cannot express fails the startup before any call: an empty name, or a [dead-letter declaration](../settlement/dead-letters.md) with one half missing or a cap outside 5 to 100.
A descriptor with [`create_with_topic`](../subscriptions/create-with-topic.md) creates its topic, its dead-letter topic and the subscription where they are missing.
A subscription that already exists and carries a dead-letter declaration gets the declared policy written onto it.
Each of these calls needs the matching Pub/Sub permission, and a refusal fails the startup with [`PubSubError::Admin`](api:ruststream_gcp_pubsub::PubSubError::Admin).
Last, the subscription opens its streaming pull.
A subscription that does not exist fails there, on its stream, with [`PubSubError::Receive`](api:ruststream_gcp_pubsub::PubSubError::Receive).

## Example

The subscription `orders-workers` creates itself on the topic `orders` with a dead-letter policy of five attempts.
Its test also starts the same service with a cap of three, which the startup refuses.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pubsub_broker_startup.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pubsub_broker_startup.rs#app"
```

</TabItem>
</Tabs>
