---
sidebar_position: 7
title: Batches
description: "A slice handler takes batches assembled on the client: batch(n) bounds their size, and batch_wait the wait for a partial one."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Batches

A slice handler takes batches assembled on the client: [`batch(n)`](api:ruststream::runtime::SubscriberSettings::batch) bounds their size, and [`batch_wait`](api:ruststream_gcp_pubsub::GooglePubSub::batch_wait) the wait for a partial one.

A handler over `&[T]` is a batch handler, and `batch(n)` at the mount site is its largest batch.
The streaming pull hands over one delivery at a time, so the subscription gathers deliveries into a batch itself.
A batch goes to the handler when it holds `n` deliveries, or when `batch_wait` has passed since its first one, 50 milliseconds by default.
So a quiet subscription yields small batches, and a busy one full ones.
One outcome settles every delivery of the batch; a `Vec<HandlerOutcome>` settles delivery `i` with outcome `i`.
Every delivery of a batch is outstanding while the batch waits, so [`max_outstanding`](./max-outstanding.md) below `n` keeps batches short of `n`.
In the manual form the body is an `impl Handle<[T]>`, where `Ok(())` acks the batch and `Err(outcomes)` settles delivery by delivery.

## Example

Three orders published together arrive in one call.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pubsub_subscriptions_batches.rs#batches"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pubsub_subscriptions_batches.rs#batches"
```

</TabItem>
</Tabs>
