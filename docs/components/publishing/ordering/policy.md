---
sidebar_position: 2
title: Policy
description: "Publish::default().ordering_key(..) orders every message of one mount site under one key, a reply included."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Policy

[`Publish::default().ordering_key(..)`](api:ruststream_gcp_pubsub::PubSubPublish::ordering_key) orders every message of one mount site under one key, a reply included.

The key belongs to the policy, so it applies to every publish of that slot or reply.
A reply has no call site, so the policy is where a fixed key for replies goes.
It suits a slot that belongs to one entity: one ledger, one device.
A key named by the call or by the `partition-key` header wins over it.
The [AsyncAPI document](../../asyncapi/messages.md) reports the key in the message's binding.

## Example

Every ledger entry goes out under the key `ledger`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pubsub_publishing_ordering_policy.rs#policy"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pubsub_publishing_ordering_policy.rs#policy"
```

</TabItem>
</Tabs>
