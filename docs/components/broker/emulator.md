---
sidebar_position: 1
title: emulator
description: "emulator points every client of the broker at a local Pub/Sub emulator, with anonymous credentials."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# emulator

[`emulator(address)`](api:ruststream_gcp_pubsub::PubSubBroker::emulator) points every client of the broker at a local Pub/Sub emulator, with anonymous credentials.

The address is either a `host:port`, which the broker dials over plaintext, or a URL such as `http://localhost:8085`, which it dials as written.
The Google client libraries for Rust do not read `PUBSUB_EMULATOR_HOST`, so the address is a setting of the broker.
With an emulator set, the [endpoint](./endpoint.md) and the [credentials](./credentials.md) steps have no effect.

The emulator accepts any project id and starts empty.
A subscription that creates itself with [`create_with_topic`](../subscriptions/create-with-topic.md) is the usual form against it.
The emulator keeps its topics and subscriptions until it stops, so a second run finds what the first one created.
The repository's stand is `docker-compose.test.yml`, which `just brokers-up` starts on port 8085.

## Example

The service runs against the emulator at `localhost:8085`.
Its test runs the same service in process, and against the emulator when `PUBSUB_TEST_HOST` names one.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pubsub_broker_emulator.rs#broker"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pubsub_broker_emulator.rs#broker"
```

</TabItem>
</Tabs>
