---
sidebar_position: 2
title: name
description: "#[subscriber(GooglePubSub)] fixes the kind of subscription, and name at the mount site leaves its name to the deployment."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# name

`#[subscriber(GooglePubSub)]` fixes the kind of subscription, and [`name`](api:ruststream::runtime::SubscriberSettings::name) at the mount site leaves its name to the deployment.

A handler written with the bare kind has no subscription yet.
`.name("orders-workers")` where it is mounted builds the descriptor, as [`GooglePubSub::new`](./google-pubsub.md) would.
A definition mounted without a name does not compile.
The descriptor built this way keeps every default; a subscription with settings of its own is a descriptor in the attribute.
The same handler serves several deployments that each read a subscription of their own, such as one per region.
The manual form starts from `Unnamed::<GooglePubSub>::new()` and takes the same `.name(..)` step.

## Example

Each deployment names its subscription in `ORDERS_SUBSCRIPTION`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pubsub_subscriptions_name.rs#name"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pubsub_subscriptions_name.rs#name"
```

</TabItem>
</Tabs>
