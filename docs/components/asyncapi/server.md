---
sidebar_position: 1
title: Server
description: "The server reports the host and port clients dial under the protocol googlepubsub, never a scheme, a path or credentials."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Server

The server reports the host and port clients dial under the protocol `googlepubsub`, never a scheme, a path or credentials.

A broker registered with `with_broker_labeled` is a server of the document, under its label.
Its `host` is the [emulator](../broker/emulator.md) address where one is set, the [endpoint](../broker/endpoint.md) where one is set, and `pubsub.googleapis.com` otherwise.
A scheme, a path and any user info the endpoint carries stay out of it.
The `protocol` is `googlepubsub`.
`protocolVersion` stays out, and the server has no binding: the specification's `googlepubsub` binding describes no server field.

## Example

The broker reaches the regional endpoint, and its server in the document is `us-east1-pubsub.googleapis.com`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pubsub_asyncapi.rs#server"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pubsub_asyncapi.rs#server"
```

</TabItem>
</Tabs>
