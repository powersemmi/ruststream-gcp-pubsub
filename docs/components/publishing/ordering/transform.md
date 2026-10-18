---
sidebar_position: 4
title: Transform
description: "A reply transform over PubSubPublishOptions sets the reply's ordering key per delivery, read off the request."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Transform

A reply transform over [`PubSubPublishOptions`](api:ruststream_gcp_pubsub::PubSubPublishOptions) sets the reply's ordering key per delivery, read off the request.

A reply has no call site, so a key that differs per reply is a transform's to write.
The transform implements [`PublishTransform<ForReply<C>, PubSubPublishOptions>`](api:ruststream::runtime::PublishTransform) and reads the delivery being answered.
It writes the key into the message's settings, where the [`ordering_key`](./ordering-key.md) step would.
Naming the settings type ties the transform to Pub/Sub publishers: it mounts on no other broker.
The mount site adds it after the reply policy with `.transform(..)`.

## Example

Each receipt goes out under the key of the order it answers.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pubsub_publishing_ordering_transform.rs#transform"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pubsub_publishing_ordering_transform.rs#transform"
```

</TabItem>
</Tabs>
