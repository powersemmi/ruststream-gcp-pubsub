---
sidebar_position: 1
title: retry_after
description: "retry_after holds the delivery under its lease for the delay, then nacks it, so the subscription redelivers it once the delay is over."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# retry_after

[`retry_after(delay)`](api:ruststream::runtime::HandlerOutcome::retry_after) holds the delivery under its lease for the delay, then nacks it, so the subscription redelivers it once the delay is over.

Pub/Sub has no delayed nack, so the crate keeps the delivery in the process for the delay.
The client goes on extending the delivery's lease while it waits, so the service does not redeliver it early.
The wait runs on a task of its own, and the subscription keeps delivering meanwhile.
The held delivery counts toward [`max_outstanding`](../subscriptions/max-outstanding.md) until it comes back, and its redelivery counts as an attempt toward a [dead-letter policy](./dead-letters.md).
Nothing is published: the message itself comes back, with its payload and its attributes, and with the next [attempt](./attempts.md) number where the subscription counts attempts.

The delay must fit inside the subscription's [`max_lease`](../subscriptions/max-lease.md), an hour by default.
A longer one is refused with [`PubSubError::DelayBeyondLease`](api:ruststream_gcp_pubsub::PubSubError::DelayBeyondLease), and the delivery comes back at once.
A process that stops while it holds a delivery stops extending its lease, and the service redelivers the message when the lease runs out: the delay is lost, the message is not.

## Example

A payment the bank has not settled yet comes back in thirty seconds.
The test advances the paused clock and sees the second delivery only once the thirty seconds are over.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pubsub_settlement_retry_after.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pubsub_settlement_retry_after.rs#handler"
```

</TabItem>
</Tabs>
