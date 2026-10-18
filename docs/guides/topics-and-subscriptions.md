---
sidebar_position: 2
description: "Read Pub/Sub topics through subscriptions: one per consumer for fan-out, shared by replicas, created by the service or by operations."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Topics and subscriptions

This guide reads one topic from several parts of a service, through subscriptions the service creates and subscriptions operations own.

A publisher sends to a topic.
A subscription is a resource of its own, attached to one topic, and it keeps every message published to the topic after it was created.
Each subscription of a topic receives every message.
The consumers of one subscription share its messages.
A service always reads a subscription.

The example is an orders service: billing, shipping, an audit and a report each read the topic `orders`.

## One subscription per consumer

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/topics_and_subscriptions.rs#fan-out"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/topics_and_subscriptions.rs#fan-out"
```

</TabItem>
</Tabs>

`bill` and `ship` each have a subscription of their own on `orders`, so each receives every order.
A new consumer of the topic is a new subscription; nothing changes for the publisher.
A subscription receives only what is published after it exists, so a consumer added later does not see the orders before it.

[`GooglePubSub`](../components/subscriptions/google-pubsub.md) names the subscription by its short id in the broker's project, or by its full resource name, `projects/{project}/subscriptions/{id}`.
[`create_with_topic`](../components/subscriptions/create-with-topic.md) makes the service own its topology: at startup it creates the topic and the subscription where they are missing, and leaves them alone where they exist.
A subscription it creates has message ordering on, which [Ordering keys](ordering.md) uses.

## Subscriptions operations own

In production the topology is often infrastructure, created by Terraform or `gcloud` before the service runs.
The service then only names what it reads.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/topics_and_subscriptions.rs#existing"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/topics_and_subscriptions.rs#existing"
```

</TabItem>
</Tabs>

`#[subscriber("orders-audit")]` reads the [subscription](../components/subscriptions/index.md) `orders-audit` as it is.
Nothing is created at startup, and a subscription that is not there fails on its stream with [`PubSubError::Receive`](api:ruststream_gcp_pubsub::PubSubError::Receive).
`#[subscriber(GooglePubSub)]` fixes the kind and leaves the name open.
The mount site names the subscription with [`.name(..)`](../components/subscriptions/name.md), so the deployment decides it, here from `REPORTS_SUBSCRIPTION`.
In the manual form the descriptor is built where the handler is mounted, so it takes the name directly.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/topics_and_subscriptions.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/topics_and_subscriptions.rs#app"
```

</TabItem>
</Tabs>

Against the emulator, create the two subscriptions before the service starts, the way operations would:

```bash
for subscription in orders-audit orders-reports; do
  curl -X PUT localhost:8085/v1/projects/my-project/subscriptions/$subscription \
    -H 'content-type: application/json' \
    -d '{"topic": "projects/my-project/topics/orders"}'
done
```

The topic `orders` exists once the service has started at least once, because billing and shipping create it.

## Several replicas of the service

Every replica of the service opens a streaming pull on the same subscriptions.
Pub/Sub hands each message of a subscription to one of them, so the replicas share the work.
Scaling out is starting more replicas; no setting changes.
A message one replica gives back with `retry` may come back to another.

Inside one replica, a subscription handles one message at a time.
`workers(n)` on the `#[subscriber]` attribute handles up to `n` at once; [Ordering keys](ordering.md) shows the keyed form.

## Testing

The tests in the example file publish an order to `orders` and check that billing and shipping both handled it.
In process, a subscription the service only names reads the topic of its own name, so the test of `orders-audit` and `orders-reports` publishes there.
[Testing a Pub/Sub service](testing.md) covers what the in-process Pub/Sub models.
