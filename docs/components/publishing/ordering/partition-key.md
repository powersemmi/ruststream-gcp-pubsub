---
sidebar_position: 3
title: PARTITION_KEY_HEADER
description: "The partition-key header is the portable spelling of the ordering key, read on publish and written on every keyed delivery."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# PARTITION_KEY_HEADER

The [`partition-key`](api:ruststream_gcp_pubsub::PARTITION_KEY_HEADER) header is the portable spelling of the ordering key, read on publish and written on every keyed delivery.

A body that names no broker type sets the key through this header.
On publish the header becomes the message's ordering key and does not travel as an attribute.
The header names the same key as the in-memory broker's `partition-key`, so a service keeps its headers when it moves between the two.
A key from the [`ordering_key`](./ordering-key.md) step or a transform wins over the header, and the header wins over the [policy's key](./policy.md).
A value that is not UTF-8 fails the publish.
On a delivery the header holds the key the message was published under, and it is absent on a message without one.

## Example

The handler sets the header, and the shipment goes out under the order's key.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pubsub_publishing_ordering_header.rs#partition-key"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pubsub_publishing_ordering_header.rs#partition-key"
```

</TabItem>
</Tabs>
