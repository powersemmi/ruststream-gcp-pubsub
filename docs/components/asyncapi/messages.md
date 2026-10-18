---
sidebar_position: 2
title: Messages
description: "A publish policy with a fixed ordering key writes it into the googlepubsub binding of the message the mount site sends."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Messages

A publish policy with a fixed ordering key writes it into the `googlepubsub` binding of the message the mount site sends.

The message binding of `googlepubsub` describes what a message carries beyond its payload and attributes.
This crate fills `orderingKey` where the mount site's [policy](../publishing/ordering/policy.md) fixed one.
A key named per message, by the [`ordering_key`](../publishing/ordering/ordering-key.md) step or the `partition-key` header, is named at a call site, which the document does not see.
A message whose mount site fixed no key carries no binding.

## Example

Every receipt goes out under the key `receipts`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pubsub_asyncapi.rs#messages"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pubsub_asyncapi.rs#messages"
```

</TabItem>
</Tabs>

The message `Receipt` in the document:

```json
{
  "googlepubsub": {
    "orderingKey": "receipts",
    "bindingVersion": "0.2.0"
  }
}
```
