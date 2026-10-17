---
sidebar_position: 2
title: Dead letters
description: "max_attempts and dead_letter at the mount site become the subscription's own dead-letter policy, written at startup."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Dead letters

[`max_attempts(n)`](api:ruststream::runtime::Mounting::max_attempts) and [`dead_letter(topic)`](api:ruststream::runtime::Mounting::dead_letter) at the mount site become the subscription's own dead-letter policy, written at startup.

A Pub/Sub subscription moves a spent message itself.
After `n` deliveries the service stops redelivering the message and publishes it to the dead-letter topic, with its payload and its attributes.
The crate publishes nothing: it writes the declaration onto the subscription as its dead-letter policy.
A subscription that [`create_with_topic`](../subscriptions/create-with-topic.md) creates carries the policy from the start, and the dead-letter topic is created with it.
A subscription that already exists gets the policy written onto it at [startup](../broker/startup.md), whether the handler names it by string or by descriptor.

The policy is one field of the subscription with both halves, so the mount site declares both.
A cap without a topic, a topic without a cap, or a cap outside the 5 to 100 Pub/Sub accepts fails the startup.
Two registrations on one subscription that declare different policies fail the startup too.
`.out_retry(..)` does not compile on this broker, because no retry copy is ever published.

The dead-letter topic is an ordinary topic, read through a subscription of its own.
A message published to a topic with no subscription is lost, so the reader's subscription exists before the first message is spent.
The Pub/Sub service account of the project needs permission to publish to the dead-letter topic and to subscribe to the source subscription.
Pub/Sub counts deliveries on a best-effort basis, so a message may get a few more or fewer than `n`.

## Example

Five deliveries of a declined payment; after the fifth, Pub/Sub publishes it to `payments-dead`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pubsub_settlement_dead_letters.rs#declaration"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pubsub_settlement_dead_letters.rs#declaration"
```

</TabItem>
</Tabs>

The dead-letter topic is read by an ordinary subscription:

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pubsub_settlement_dead_letters.rs#reader"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pubsub_settlement_dead_letters.rs#reader"
```

</TabItem>
</Tabs>
