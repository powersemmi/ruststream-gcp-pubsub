---
sidebar_position: 1
title: Live
description: "TestApp::start_live runs the same test body against the Pub/Sub emulator, for leases, flow control and ordered delivery."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Live

[`TestApp::start_live`](api:ruststream::testing::TestApp::start_live) runs the same test body against the Pub/Sub emulator, for leases, flow control and ordered delivery.

It connects the broker through its ordinary connect, so every subscription opens a real streaming pull.
The body is the one an in-process test runs; only the start call differs.
A publish, `settle` and `advance` wait on the real clock until every subscription has handled what was published to it.
The wait lasts ten seconds at most, and [`start_live_within`](api:ruststream::testing::TestApp::start_live_within) sets another bound.

The live app is the production one on a broker pointed at the [emulator](../broker/emulator.md).
The emulator starts empty, so its subscriptions create themselves with [`create_with_topic`](../subscriptions/create-with-topic.md).
It keeps its subscriptions until it stops, so a run that failed can leave messages behind for the next one.
The examples delete their subscriptions before each live run, which `live::fresh` does with the client library's admin API.

In the example, `live::host` returns the emulator's address from `PUBSUB_TEST_HOST`, and the test returns early without one, so `cargo test` passes on a machine with no emulator.
A job that stood an emulator up sets `RUSTSTREAM_REQUIRE_LIVE=1`, and a missing address then fails the test instead of skipping it.
The repository's stand is `docker-compose.test.yml`, which `just brokers-up` starts.

## Example

The two tests share one body: one in process, one against the emulator at `PUBSUB_TEST_HOST`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pubsub_testing_live.rs#start-live"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pubsub_testing_live.rs#start-live"
```

</TabItem>
</Tabs>
