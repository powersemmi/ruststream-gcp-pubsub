---
sidebar_position: 9
description: "Test a Pub/Sub service with TestApp: the production app in process, replies, drops, delays and dead letters, and the same test against the emulator."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Testing a Pub/Sub service

A test runs the service's own app with [`TestApp`](api:ruststream::testing::TestApp), against an [in-process Pub/Sub](../components/testing/index.md) or against the emulator.

The service under test sends refunds.
A refund arrives on `refunds`, and the handler answers it with a `Refunded` message on `refunds-done`.
A refund without an amount is dropped.
A refund above the approval limit waits a minute for approval with [`retry_after`](../components/settlement/retry-after.md).
After five deliveries a refund goes to `refunds-review`, the [dead-letter topic](../components/settlement/dead-letters.md).
The app is built by `service(broker)`, so a test chooses the broker and `main` uses the one for the emulator.
The tests need the `testing` feature of `ruststream-gcp-pubsub` in `[dev-dependencies]`.

## A refund answered

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/testing.rs#answered"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#answered"
```

</TabItem>
</Tabs>

[`TestApp::start`](api:ruststream::testing::TestApp::start) connects [`PubSubBroker`](../components/broker/index.md) to an in-process Pub/Sub.
Nothing is dialled and no credentials are read.
The test addresses the broker by its production type, `tb.broker::<PubSubBroker>()`.
`Refund` names its topic, so `message(..).publish()` needs no destination.
`publish` returns once the handler and its reply have settled.
So the next lines assert on finished state: how the subscriber settled and what reached `refunds-done`.
The body takes the started app as an argument, so the live test below runs it too.

## A refund dropped

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/testing.rs#dropped"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#dropped"
```

</TabItem>
</Tabs>

`settled` checks the outcome the handler returned.
`assert_not_called` checks that no answer was published.

## A delay, without waiting for it

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/testing.rs#held"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#held"
```

</TabItem>
</Tabs>

The test starts on a paused clock, and [`advance`](api:ruststream::testing::TestApp::advance) moves it forward.
A minute of `APPROVAL_WAIT` costs nothing.
Each wait ends in a redelivery of the same message, and `assert_called` counts them.
The fifth delivery waits too, and when its wait ends Pub/Sub moves the refund to `refunds-review` as it arrived.
`published` reads what reached a topic, whether the service or Pub/Sub put it there.

## What the in-process Pub/Sub models

The in-process Pub/Sub reads the production broker's project and every descriptor the service mounts.
A publish, a subscription or a declaration Pub/Sub refuses fails the test the same way.
It models Pub/Sub rather than a queue:

- Topics and subscriptions are separate, and each subscription of a topic receives every message published after it was created.
- The consumers of one subscription share its messages.
- `retry` delivers a message again, `retry_after` waits on the test's clock first, and `drop` acknowledges.
- A dead-letter policy counts the deliveries and moves a message that ran out of them.

The topology the service expects to find is taken to exist.
A subscription the service only names reads the topic of its own name, and a topic the service publishes to accepts the message.
Leases, flow control, ordered delivery by key and exactly-once delivery need the emulator or Pub/Sub itself.

## The same test against the emulator

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/testing.rs#live"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#live"
```

</TabItem>
</Tabs>

[`TestApp::start_live`](../components/testing/live.md) connects the same app to the emulator.
A publish then waits up to ten seconds for every subscription it reaches to handle it; `start_live_within` sets another deadline.
The test reads the emulator's address from `PUBSUB_TEST_HOST` with `live::host`, a helper of the example's own, and skips when it is unset.
The emulator keeps its subscriptions between runs, so `live::fresh` deletes `refunds-workers` first, and the run reads only what it publishes.
`create_topic` creates `refunds-done` the way operations would, because a reply to a topic that is not there fails.
The assertions are the in-process ones, unchanged.

```bash
cargo test
PUBSUB_TEST_HOST=localhost:8085 cargo test -- --test-threads=1
```
