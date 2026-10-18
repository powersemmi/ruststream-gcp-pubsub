---
title: Testing
description: "TestApp::start connects PubSubBroker in process: topics, subscriptions, redelivery and dead letters, with no emulator."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Testing

[`TestApp::start`](api:ruststream::testing::TestApp::start) connects [`PubSubBroker`](../broker/index.md) in process: topics, subscriptions, redelivery and dead letters, with no emulator.

A test hands the harness the app `main` runs, unchanged, and addresses the broker by its production type, `tb.broker::<PubSubBroker>()`.
The `testing` feature in `[dev-dependencies]` adds the in-process mode, and turns on the core's harness with it.
Nothing is dialled and no credentials are read, so a test runs where the service could not authenticate.

The in-process mode keeps Pub/Sub's model.
A topic and a subscription are separate, and a subscription receives every message published to its topic after it was created, whether or not a consumer is open.
A subscription the service only names is taken to exist, attached to the topic of its own name; [`create_with_topic`](../subscriptions/create-with-topic.md) attaches one to the topic it names.
The consumers of one subscription share its messages in turn.
A nack redelivers, a [`retry_after`](../settlement/retry-after.md) waits on the test's clock, and a [dead-letter policy](../settlement/dead-letters.md) counts attempts and moves a spent message.
It refuses what the service refuses: a resource name it does not accept, an empty message, an attribute or an ordering key past its limits or not UTF-8.

A publish through the harness returns once every handler it woke has settled.
With `tokio::time::pause()`, or `#[tokio::test(start_paused = true)]`, `tb.advance(d)` moves the clock and runs what fell due.
Leases, flow control, ordered delivery by key and exactly-once delivery belong to the service; a [live test](./live.md) checks them against the emulator.

## Example

The dependency line:

```toml
[dev-dependencies]
ruststream-gcp-pubsub = { version = "0.7", features = ["testing"] }
```

The first test publishes a payment and reads how the handler settled it.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pubsub_testing.rs#testapp"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pubsub_testing.rs#testapp"
```

</TabItem>
</Tabs>

A payment the bank never settles comes back every thirty seconds and is dead-lettered after its fifth attempt.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pubsub_testing.rs#advance"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pubsub_testing.rs#advance"
```

</TabItem>
</Tabs>
