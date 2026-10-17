---
sidebar_position: 3
title: Channels
description: "A subscription is a channel the service receives on, and a topic a channel it sends to; neither carries a googlepubsub binding."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Channels

A subscription is a channel the service receives on, and a topic a channel it sends to; neither carries a `googlepubsub` binding.

A channel the service receives on is named after the subscription.
The topic behind a subscription is a fact of the service's infrastructure, which the document, built before anything connects, does not see.
A channel the service publishes to reports the topic the mount site resolved as its address: a reply type's `#[outgoing(name)]`, a `reply("dest")` clause, or a slot's destination.
The channel binding of `googlepubsub` describes a topic's configuration: labels, retention, storage policy and schema.
No descriptor or publish policy holds those, so channels carry no binding.

## Example

The subscription `orders-workers` is a channel the service receives on, and the topic `receipts` one it sends to.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pubsub_asyncapi.rs#channels"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pubsub_asyncapi.rs#channels"
```

</TabItem>
</Tabs>
