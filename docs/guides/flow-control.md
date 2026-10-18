---
sidebar_position: 5
description: "Bound how many Pub/Sub messages a service holds, how far each lease extension reaches and how long a slow message stays leased."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Flow control and leases

This guide bounds how many messages a subscription holds at once and keeps a slow message leased while the service works on it or waits for it.

Pub/Sub leases each message it delivers on a streaming pull.
While the lease lasts, the subscription does not deliver the message to anyone else.
The client extends the lease in the background until the message is settled, up to a limit.
A lease that runs out sends the message to another consumer, even if the first one is still working on it.

The example is a reports service: rendering a report takes a while, and sometimes its data is not there yet.

## The subscription

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/flow_control.rs#render"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/flow_control.rs#render"
```

</TabItem>
</Tabs>

All three settings are steps of the [`GooglePubSub`](../components/subscriptions/google-pubsub.md) descriptor.

[`max_outstanding(10)`](../components/subscriptions/max-outstanding.md) caps the messages the service holds unsettled at once.
At the cap Pub/Sub stops sending, and it resumes as messages are settled.
A message held by `retry_after` counts toward the cap until it comes back.
The client's default is 1000; a small cap keeps slow work from piling up in one replica while others are idle.

[`ack_extension`](../components/subscriptions/ack-extension.md) is how far each extension of a lease reaches, a minute here.
The client keeps it between 10 seconds and 10 minutes.
A longer extension means fewer calls to Pub/Sub; a shorter one hands a message to another consumer sooner after a replica dies.

[`max_lease`](../components/subscriptions/max-lease.md) is how long the client goes on extending one lease, 30 minutes here and an hour by default.
It is also the longest delay a `retry_after` may ask for: the delay is spent under the lease.

## Waiting under the lease

`render` asks for a report whose data is late again in 20 seconds, with [`retry_after`](../components/settlement/retry-after.md).
The service keeps the message for the delay and the client extends its lease, so no other consumer gets it meanwhile.
When the delay is over the service nacks the message, and Pub/Sub delivers it again.
A delay longer than `max_lease` would outlive the lease, so the service refuses it and the message comes back at once.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/flow_control.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/flow_control.rs#app"
```

</TabItem>
</Tabs>

The app is built by `service(broker)`, so a test runs the same service in process and against the emulator.

## Testing

The test publishes a report whose data is late and checks that it comes back once, after the 20 seconds.
In process the delay passes on a paused clock.
The in-process Pub/Sub has no leases and no flow control, so the same test also runs against the emulator, where the 20 seconds are real.
There the wait is twice the subscription's 10-second acknowledgement deadline, and the report comes back only once: the lease held it.

```bash
docker compose up -d
PUBSUB_TEST_HOST=localhost:8085 cargo test -- --test-threads=1
```

The live test reads the emulator's address from `PUBSUB_TEST_HOST` and skips when it is unset.
[Testing a Pub/Sub service](testing.md) shows how one test body runs in both modes.
