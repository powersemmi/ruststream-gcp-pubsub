---
sidebar_position: 1
title: Headers
description: "Headers travel as Pub/Sub attributes, with no envelope of their own; an attribute that is not UTF-8 fails the publish."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Headers

Headers travel as Pub/Sub attributes, with no envelope of their own; an attribute that is not UTF-8 fails the publish.

Every header of an outgoing message becomes an attribute of the Pub/Sub message, name and value unchanged.
Each attribute of a delivery becomes a header, so a handler reads it with `ctx.headers()`.
Attributes are text, so a header value that is not UTF-8 fails the publish with [`PubSubError::Publish`](api:ruststream_gcp_pubsub::PubSubError::Publish) instead of being rewritten.
Pub/Sub accepts at most 100 attributes per message, names of up to 256 bytes and values of up to 1024 bytes, and refuses a message past them.

Two headers have a meaning of their own.
[`partition-key`](./ordering/partition-key.md) is the message's ordering key: it never travels as an attribute, and a delivery reports its ordering key under it.
[`pubsub-delivery-attempt`](../settlement/attempts.md) is the attempt number a delivery reports where the subscription counts attempts.

## Example

The tenant arrives as an attribute of the order and leaves as an attribute of the shipment.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pubsub_publishing_headers.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pubsub_publishing_headers.rs#handler"
```

</TabItem>
</Tabs>
