---
sidebar_position: 3
title: endpoint
description: "endpoint names another Pub/Sub endpoint, such as a locational endpoint that keeps every request of the service in one region."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# endpoint

[`endpoint`](api:ruststream_gcp_pubsub::PubSubBroker::endpoint) names another Pub/Sub endpoint, such as a locational endpoint that keeps every request of the service in one region.

By default the broker reaches the global endpoint, `https://pubsub.googleapis.com`.
A locational endpoint, such as `https://us-east1-pubsub.googleapis.com`, routes every request to that region.
Two Pub/Sub guarantees hold only inside one region.
Messages that share an [ordering key](../publishing/ordering/index.md) arrive in order when they are published to the same region.
An exactly-once subscription guarantees one delivery to subscribers that connect in the region of the subscription.
The endpoint applies to every client of the broker: subscriptions, publishers and the topology calls at startup.
The [AsyncAPI server](../asyncapi/server.md) reports the endpoint's host.

## Example

Every request of the service goes to `us-east1`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pubsub_broker_config.rs#endpoint"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pubsub_broker_config.rs#endpoint"
```

</TabItem>
</Tabs>
