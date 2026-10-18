---
sidebar_position: 1
title: ordering_key
description: "The ordering_key step names the ordering key of one publish, on the framework's publish builder."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# ordering_key

The [`ordering_key`](api:ruststream_gcp_pubsub::PubSubOrdering::ordering_key) step names the ordering key of one publish, on the framework's publish builder.

The step sits between `message(..)` and `publish()`, so the publish stays the mount site's: its codec, its transforms and its slot hold.
It exists only on a builder over a Pub/Sub publisher.
The handler bounds its slot `Out<impl Publisher<Options = PubSubPublishOptions>>` and imports the [prelude](../../broker/prelude.md), which brings the [`PubSubOrdering`](api:ruststream_gcp_pubsub::PubSubOrdering) trait.
The key reaches the client as the message's own ordering key and never as an attribute.
A key named by the call wins over the `partition-key` header and over the policy's key.
A test reads the setting back with `tb.out::<Slot>().with_options(..)` and a [`PubSubPublishOptions`](api:ruststream_gcp_pubsub::PubSubPublishOptions) value.

## Example

Each confirmation goes out under its order's key.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pubsub_publishing_ordering_key.rs#ordering-key"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pubsub_publishing_ordering_key.rs#ordering-key"
```

</TabItem>
</Tabs>
