---
title: PubSubPublish
description: "PubSubPublish, Publish in the prelude, is the publish policy; every publish waits for the message id Pub/Sub assigns."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# PubSubPublish

[`PubSubPublish`](api:ruststream_gcp_pubsub::PubSubPublish), `Publish` in the prelude, is the publish policy; every publish waits for the message id Pub/Sub assigns.

A mount site names it wherever a registration publishes: `.out_reply(Publish::default())` for a reply, `.out(Slot, Publish::default())` for a slot.
The policy holds no connection; the runtime pairs it with the connected broker at startup into a [`PubSubPublisher`](api:ruststream_gcp_pubsub::PubSubPublisher).
A registration that names no policy for its reply publishes through this one.
The destination is a topic, a short id in the broker's project or a full resource name.
A reply goes to the topic its type names, and a slot publish to the topic the call or the slot names.

The encoded message becomes the Pub/Sub message's data without a copy, and [headers](./headers.md) become its attributes.
The client library keeps one publisher per topic, created on first use, which sends messages in batches of up to 100 messages, 1 MB or 10 milliseconds.
A publish returns once the service has accepted the message and assigned its id, or with the error it reported as [`PubSubError::Publish`](api:ruststream_gcp_pubsub::PubSubError::Publish).
The policy's one setting is a default [ordering key](./ordering/policy.md).

## Example

A reply goes to the topic of its type, and an invoice to the topic the call names.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pubsub_publishing.rs#reply"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pubsub_publishing.rs#reply"
```

</TabItem>
</Tabs>

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pubsub_publishing.rs#slot"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pubsub_publishing.rs#slot"
```

</TabItem>
</Tabs>

The mount site names the policy for both:

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pubsub_publishing.rs#mount"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pubsub_publishing.rs#mount"
```

</TabItem>
</Tabs>
