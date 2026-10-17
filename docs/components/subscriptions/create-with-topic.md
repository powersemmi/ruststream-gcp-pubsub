---
sidebar_position: 3
title: create_with_topic
description: "create_with_topic creates the subscription and its topic at startup where they are missing, with message ordering on."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# create_with_topic

[`create_with_topic`](api:ruststream_gcp_pubsub::GooglePubSub::create_with_topic) creates the subscription and its topic at startup where they are missing, with message ordering on.

At [startup](../broker/startup.md) the subscription looks the topic up and creates it when it is missing.
It does the same for the declared [dead-letter topic](../settlement/dead-letters.md), and then for the subscription itself, attached to the topic.
A subscription it creates has [message ordering](../publishing/ordering/index.md) on and carries the declared dead-letter policy.
A subscription that already exists keeps its own configuration; the crate changes only its dead-letter policy, where the mount site declared one.
The calls need the permissions to create topics and subscriptions, which a production service often lacks.
So the step suits the [emulator](../broker/emulator.md), tests and services that own their topology; elsewhere the topology is infrastructure and the descriptor only names it.
Several subscriptions created on one topic each receive every message published to it.

## Example

Two subscriptions on the topic `orders`: each receives every order.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pubsub_subscriptions_descriptor.rs#create-with-topic"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pubsub_subscriptions_descriptor.rs#create-with-topic"
```

</TabItem>
</Tabs>
