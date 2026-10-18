---
sidebar_position: 1
title: GooglePubSub
description: "GooglePubSub names one subscription by short id or full resource name and carries its settings: topology, flow control, leases and batches."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# GooglePubSub

[`GooglePubSub`](api:ruststream_gcp_pubsub::GooglePubSub) names one subscription by short id or full resource name and carries its settings: topology, flow control, leases and batches.

[`GooglePubSub::new`](api:ruststream_gcp_pubsub::GooglePubSub::new) takes the subscription's name, and builder steps add the rest.
[`create_with_topic`](./create-with-topic.md) creates the subscription and its topic at startup.
[`max_outstanding`](./max-outstanding.md) caps the deliveries that may be unsettled at once.
[`ack_extension`](./ack-extension.md) and [`max_lease`](./max-lease.md) shape how the client keeps a delivery leased.
`batch_wait` bounds how long a partial [batch](./batches.md) waits for the rest.
A setting the descriptor leaves out keeps the client library's default.

The descriptor is plain data and holds no connection.
It opens against the connected broker at startup, so the declaration a service ships is the one its tests run.

## Example

The subscription `orders-workers` on the topic `orders`, with at most 100 orders unsettled at once.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pubsub_subscriptions_descriptor.rs#descriptor"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pubsub_subscriptions_descriptor.rs#descriptor"
```

</TabItem>
</Tabs>
