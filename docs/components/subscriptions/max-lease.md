---
sidebar_position: 6
title: max_lease
description: "max_lease is how long the client keeps extending the lease of one unsettled delivery, an hour by default, and the budget of retry_after."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# max_lease

[`max_lease`](api:ruststream_gcp_pubsub::GooglePubSub::max_lease) is how long the client keeps extending the lease of one unsettled delivery, an hour by default, and the budget of `retry_after`.

After a delivery has been held this long, the client stops extending its lease.
The lease then runs out, and Pub/Sub redelivers the message, possibly to another consumer.
A handler that may run longer than an hour needs a longer lease.

[`retry_after(delay)`](../settlement/retry-after.md) holds the delivery in the process for the delay, under this same lease.
A delay longer than `max_lease` is refused with [`PubSubError::DelayBeyondLease`](api:ruststream_gcp_pubsub::PubSubError::DelayBeyondLease), and the delivery comes back at once.
The refusal keeps the delivery from coming back before the delay is over, from a lease that ran out.

## Example

An invoice that is not ready comes back in ninety minutes, inside a lease of two hours.
The tests advance the clock past the ninety minutes, and show that a delay of three hours comes back at once.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pubsub_subscriptions_flow.rs#max-lease"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pubsub_subscriptions_flow.rs#max-lease"
```

</TabItem>
</Tabs>
