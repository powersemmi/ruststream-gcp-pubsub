---
sidebar_position: 3
title: Attempts
description: "On a subscription with a dead-letter policy a delivery reports its attempt number in the pubsub-delivery-attempt header."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Attempts

On a subscription with a dead-letter policy a delivery reports its attempt number in the [`pubsub-delivery-attempt`](api:ruststream_gcp_pubsub::DELIVERY_ATTEMPT_HEADER) header.

Pub/Sub counts the deliveries of a message only where its subscription has a [dead-letter policy](./dead-letters.md).
There each delivery carries the count, the first delivery being attempt 1, and the crate puts it in the header `pubsub-delivery-attempt`.
A subscription without a policy reports no count, and the header is absent.
A handler reads it with `ctx.headers().get_str(DELIVERY_ATTEMPT_HEADER)` and branches on it, for example to give a message up before the cap.
The count is the service's estimate: it may miss a delivery, and it may start over from zero.
The crate also reads it to tell the last allowed delivery, where `drop` nacks instead of acknowledging.

## Example

An order without stock waits through two redeliveries and is given up on the third attempt, before the cap of five.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pubsub_settlement_attempts.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pubsub_settlement_attempts.rs#handler"
```

</TabItem>
</Tabs>
