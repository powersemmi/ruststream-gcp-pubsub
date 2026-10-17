---
title: Acknowledgement
description: "ack acknowledges a delivery, retry nacks it so Pub/Sub redelivers it, and drop acknowledges one that is not worth another attempt."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Acknowledgement

[`ack`](api:ruststream::runtime::HandlerOutcome::ack) acknowledges a delivery, [`retry`](api:ruststream::runtime::HandlerOutcome::retry) nacks it so Pub/Sub redelivers it, and [`drop`](api:ruststream::runtime::HandlerOutcome::drop) acknowledges one that is not worth another attempt.

Pub/Sub settles each message on its own, by acknowledging it or by giving it back.
`ack` acknowledges, and the subscription does not deliver the message again.
`retry` nacks, and the subscription redelivers the message, possibly to another consumer.
A subscription with no retry policy redelivers at once; one with an exponential backoff policy waits first.
`drop` acknowledges too: Pub/Sub has no verb that discards a message without settling it.
The one exception is the last delivery a [dead-letter policy](./dead-letters.md) allows: there `drop` nacks, so the service moves the message to the dead-letter topic as the mount site declared.
A delayed retry is [`retry_after`](./retry-after.md).

### Exactly-once subscriptions

A subscription with exactly-once delivery on guarantees that an acknowledged message is not delivered again.
It is a property of the subscription's configuration, and the crate reads it off each delivery.
On such a subscription an ack and a nack wait for the service to confirm them.
An acknowledgement the service rejects, such as one whose lease has expired, fails the settlement with an `AckError` that carries the cause, and the message may come back.
On other subscriptions an ack is sent in the background and returns at once.
The guarantee holds for subscribers in the subscription's region, which a locational [endpoint](../broker/endpoint.md) pins.

## Example

A reservation acknowledges an order it reserved, drops an order with nothing to reserve, and asks for another delivery while the stock is out.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pubsub_settlement.rs#outcomes"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pubsub_settlement.rs#outcomes"
```

</TabItem>
</Tabs>
