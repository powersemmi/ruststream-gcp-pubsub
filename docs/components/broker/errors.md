---
sidebar_position: 6
title: Errors
description: "PubSubError is the one error type of the crate: connect, admin, receive, publish, not connected, invalid descriptor and a delay past the lease."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Errors

[`PubSubError`](api:ruststream_gcp_pubsub::PubSubError) is the one error type of the crate: connect, admin, receive, publish, not connected, invalid descriptor and a delay past the lease.

Each variant names the step that failed.
The client library's error behind it is its `source`, so the library's types stay out of the crate's API.

| Variant | When |
|---|---|
| `Connect` | a client could not be built |
| `Admin` | a topic or subscription call at startup failed, such as a create without permission |
| `Receive` | a streaming pull failed for good, such as a subscription that does not exist |
| `Publish` | the service refused a message, or the crate refused one it cannot send, such as an attribute that is not UTF-8 |
| `NotConnected` | a publisher was used after the broker shut down |
| `InvalidDescriptor` | a subscription the crate cannot open: an empty name, half a dead-letter declaration, a cap outside 5 to 100 |
| `DelayBeyondLease` | a [`retry_after`](../settlement/retry-after.md) asked for more than the subscription's [`max_lease`](../subscriptions/max-lease.md) |

A service meets the error in three places.
A failed startup carries it as the source of the runtime's error, and the service exits before it reads anything.
A publish from a handler returns it inside the core's [`PublishError`](api:ruststream::runtime::PublishError), and the handler decides how the delivery settles.
A settlement the subscription refuses, such as a delay past the lease, reaches the runtime as the source of an `AckError`.
The enum is `#[non_exhaustive]`, so a `match` on it ends with a wildcard arm.

## Example

A shipment whose note exceeds the 1024 bytes an attribute holds is refused; the handler logs the cause and drops the order.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pubsub_broker_errors.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pubsub_broker_errors.rs#handler"
```

</TabItem>
</Tabs>
