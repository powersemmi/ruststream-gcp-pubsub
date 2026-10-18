---
title: PubSubBroker
description: "PubSubBroker connects a service to Google Cloud Pub/Sub; new only records the project, and the runtime connects it at startup."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# PubSubBroker

[`PubSubBroker`](api:ruststream_gcp_pubsub::PubSubBroker) connects a service to Google Cloud Pub/Sub; `new` only records the project, and the runtime connects it at startup.

[`PubSubBroker::new`](api:ruststream_gcp_pubsub::PubSubBroker::new) takes the project id that short topic and subscription names resolve in.
It does no I/O, so the whole service fits the synchronous `#[ruststream::app]` builder.
The settings that follow it are builder steps too: a local [emulator](./emulator.md), explicit [credentials](./credentials.md) and another service [endpoint](./endpoint.md).
Without them the broker reaches `pubsub.googleapis.com` with Application Default Credentials.

At [startup](./startup.md) the runtime connects the broker and receives a [`ConnectedPubSubBroker`](api:ruststream_gcp_pubsub::ConnectedPubSubBroker).
Every subscription and every publisher of the service opens against that connected form.
At [shutdown](./shutdown.md) the runtime flushes what the service published and closes the connection.

`#[subscriber("orders-workers")]` reads the existing subscription `orders-workers` of the project.
A name is a short id or a full resource name, `projects/{project}/subscriptions/{id}`.
Every other setting of a subscription belongs to the [`GooglePubSub`](../subscriptions/google-pubsub.md) descriptor.

## Example

The service reads the subscription `orders-workers` of the project `my-project`.
`cargo run --example pubsub_broker --features testing -- run` starts it against Pub/Sub.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pubsub_broker.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pubsub_broker.rs#app"
```

</TabItem>
</Tabs>
