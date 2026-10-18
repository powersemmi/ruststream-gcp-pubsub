---
sidebar_position: 3
description: "Publish to Pub/Sub topics from a service: replies whose type names the topic, Out slots in a handler, and a message published at startup."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Publishing and replies

This guide publishes to Pub/Sub topics in the three ways a service does: a reply to a message, a publish from inside a handler, and a message once the service is up.

Every publish goes through a publish policy.
[`Publish`](../components/publishing/index.md), the prelude's name for [`PubSubPublish`](api:ruststream_gcp_pubsub::PubSubPublish), publishes to a topic and waits for the service to accept the message.
A topic that does not exist fails the publish: Pub/Sub keeps no message for a topic it does not have.

The example is an orders service that answers each order with a receipt, bills it, and announces that it is up.

## A reply

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/publishing.rs#reply"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/publishing.rs#reply"
```

</TabItem>
</Tabs>

The `reply` clause makes the return value a message to publish.
Where it goes is a property of its type: `#[outgoing(name = "receipts")]` sends every `Receipt` to the topic `receipts`.
The receipt is published after the handler returns and before the order is acknowledged, so a receipt that fails to publish leaves the order unacknowledged, and it comes back.

## A publish from a handler

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/publishing.rs#slot"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/publishing.rs#slot"
```

</TabItem>
</Tabs>

An `Out` parameter is a publisher the handler calls itself.
`bill` names the topic on the call with `.to("invoices")` and decides what a failed publish means for the order.
Here it asks for the order again with `retry`, so the invoice is not lost.
A handler may publish any number of messages, to any topics, before it settles.

## The mount site

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/publishing.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/publishing.rs#app"
```

</TabItem>
</Tabs>

`.out_reply(Publish::default())` gives the reply its policy, and `.out(DefaultSlot, Publish::default())` gives the `Out` parameter its own.
A policy holds settings for every message of its position: [`Publish::default().ordering_key(..)`](../components/publishing/ordering/policy.md) is one.
The broker creates one client publisher per topic, on the first publish to it, and every later publish to that topic reuses it.

`after_startup` runs once every subscription is open.
It receives a live publisher of the policy it names, here to announce the service on `services`.
A hook that fails stops the startup, so a service that cannot announce itself does not run.

## Running it

The topics the service publishes to belong to whoever reads them, so they exist before the service starts.
Against the emulator, create them through its REST API:

```bash
for topic in receipts invoices services; do
  curl -X PUT localhost:8085/v1/projects/my-project/topics/$topic
done
cargo run -- run
```

Without `services`, the announcement fails and the service does not start.

## Testing

The tests in the example file publish an order and assert on what reached `receipts` and `invoices` with `published`.
A second test starts the app and finds the announcement on `services`.
In process, every publish is recorded whether or not a subscription reads its topic.
[Testing a Pub/Sub service](testing.md) covers the harness.
