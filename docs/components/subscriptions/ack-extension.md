---
sidebar_position: 5
title: ack_extension
description: "ack_extension sets how far each background extension of a delivery's lease reaches, from 10 seconds to 10 minutes, 60 seconds by default."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# ack_extension

[`ack_extension`](api:ruststream_gcp_pubsub::GooglePubSub::ack_extension) sets how far each background extension of a delivery's lease reaches, from 10 seconds to 10 minutes, 60 seconds by default.

Pub/Sub redelivers a message whose ack deadline passes before it is settled.
While a handler works, the client library extends that deadline in the background, each time by this much.
A longer extension means fewer calls for a slow handler.
A shorter one brings a delivery back sooner when the process dies, because the service redelivers once the last extension runs out.
The client clamps the value to the 10 seconds to 10 minutes Pub/Sub accepts.
It is independent of the ack deadline configured on the subscription.
How long the client keeps extending one delivery is [`max_lease`](./max-lease.md).

## Example

A report takes minutes to render, so each extension reaches five minutes.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pubsub_subscriptions_flow.rs#ack-extension"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pubsub_subscriptions_flow.rs#ack-extension"
```

</TabItem>
</Tabs>
