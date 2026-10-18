---
sidebar_position: 5
title: Shutdown
description: "Shutdown flushes every message the service still buffers; a publisher used afterwards reports PubSubError::NotConnected."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Shutdown

Shutdown flushes every message the service still buffers; a publisher used afterwards reports [`PubSubError::NotConnected`](api:ruststream_gcp_pubsub::PubSubError::NotConnected).

The client library sends published messages in batches, so a publish may wait in a buffer for a few milliseconds.
At shutdown the runtime stops the subscriptions and closes the connection, then flushes the buffer of every topic the service published to.
A message published before the shutdown reaches the topic before the service exits.
A subscription that stops ends its streaming pull; the deliveries the service has not settled come back to the subscription once their leases run out.

A publisher taken before the shutdown keeps pointing at the closed connection.
A publish through it returns `PubSubError::NotConnected` and never succeeds against the dead connection.

## Example

The handler replies with a shipment, and the shipment reaches its topic before the service exits.
Its test publishes through a publisher taken before the start and gets `NotConnected` after the shutdown.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pubsub_broker_shutdown.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pubsub_broker_shutdown.rs#app"
```

</TabItem>
</Tabs>
