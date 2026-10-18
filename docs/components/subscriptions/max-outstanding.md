---
sidebar_position: 4
title: max_outstanding
description: "max_outstanding caps how many deliveries may be unsettled at once; Pub/Sub stops sending at the cap and resumes below it."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# max_outstanding

[`max_outstanding`](api:ruststream_gcp_pubsub::GooglePubSub::max_outstanding) caps how many deliveries may be unsettled at once; Pub/Sub stops sending at the cap and resumes below it.

A delivery is outstanding from the moment the service receives it until a handler settles it.
The limit travels with the streaming pull, and the service holds the stream to it.
It applies to one stream, not to the subscription as a whole: two instances of the service each hold their own share.
The default is the client library's 1000 messages; a value of zero or less sets no limit.
A delivery held by [`retry_after`](../settlement/retry-after.md) stays outstanding until it comes back.
The client also caps outstanding bytes at 100 MiB, a limit the descriptor leaves at its default.

## Example

At most 50 orders are unsettled at once.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pubsub_subscriptions_flow.rs#max-outstanding"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pubsub_subscriptions_flow.rs#max-outstanding"
```

</TabItem>
</Tabs>
