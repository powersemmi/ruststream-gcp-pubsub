---
title: Ordering
description: "On a subscription with message ordering, messages that share an ordering key reach a subscriber in the order they were published."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Ordering

On a subscription with message ordering, messages that share an ordering key reach a subscriber in the order they were published.

An ordering key is a string a publish carries, up to 1 KB.
A subscription orders deliveries only when it was created with message ordering on, a setting that cannot change afterwards.
A subscription [`create_with_topic`](../../subscriptions/create-with-topic.md) creates has it on; one created elsewhere keeps its own setting.
The order holds for messages of one key published to one region, so publishers of one key use the same locational [endpoint](../../broker/endpoint.md).
When a message of a key is redelivered, the messages published after it under that key come again too, acknowledged ones included.
With a dead-letter policy on the subscription, the order may not hold.

A publish names its key in one of three places, and the first one present wins.
The [`ordering_key`](./ordering-key.md) step, or a [transform](./transform.md), writes it into the message's settings.
The [`partition-key`](./partition-key.md) header is the portable spelling.
The policy's [default key](./policy.md) applies where neither named one.
A delivery reports the key it arrived under in the `partition-key` header.

## Example

Three events of one order, published under its key, arrive in that order.
The test runs the same body in process and against the emulator.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pubsub_publishing_ordering.rs#ordered"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pubsub_publishing_ordering.rs#ordered"
```

</TabItem>
</Tabs>
