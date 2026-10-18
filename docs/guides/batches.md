---
sidebar_position: 7
description: "Handle Pub/Sub messages in batches: a slice handler, the batch size and wait, and settling each message of a batch on its own."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Batch processing

This guide hands a handler many messages at once, for work that is cheaper per batch than per message, such as one write to a warehouse.

Pub/Sub delivers messages one by one and settles each on its own.
The service collects the deliveries of one subscription into a batch on its side, calls the handler once for the batch, and then settles every message of it.

The example loads page clicks into an analytics warehouse.

## The batch handler

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/batches.rs#load"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/batches.rs#load"
```

</TabItem>
</Tabs>

A slice parameter, `&[Click]`, makes `load` a [batch handler](../components/subscriptions/batches.md).
[`batch_wait`](api:ruststream_gcp_pubsub::GooglePubSub::batch_wait) on the descriptor is how long a batch short of its size waits for more messages, half a second here.
A batch is handed over as soon as it is full, or when the wait is over with what it has.

`load` returns one outcome per click, in the order of the batch.
A click from a user the warehouse does not know yet is retried: Pub/Sub delivers that click again, and it arrives on its own or in a later batch.
A click without a page is dropped, and the rest are acknowledged.
A single `HandlerOutcome` settles the whole batch the same way.
In the manual form `Ok(())` acknowledges the batch, and `Err` carries the outcomes click by click.

## The mount site

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/batches.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/batches.rs#app"
```

</TabItem>
</Tabs>

`batch(nonzero!(100))` is the batch size, and a batch handler does not compile without it.
Every message of a batch is leased until the batch is settled, so the batch size stays under the subscription's [`max_outstanding`](../components/subscriptions/max-outstanding.md), 1000 by default.
The dead-letter declaration counts each click on its own: after five deliveries a click goes to `clicks-dead`, however many batches it rode in.
[Acknowledgement and retries](retries.md) covers the dead-letter topic.

## Testing

The tests publish clicks together with `tokio::join!`, so they meet in one batch, and read the batches back with `assert_batch_sizes` and `batches`.
A retried click shows up as a batch of one: the batch of two, then the stranger's click alone four more times, and then it is on `clicks-dead`.
[Testing a Pub/Sub service](testing.md) covers the harness.
