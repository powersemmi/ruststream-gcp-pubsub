---
sidebar_position: 4
description: "Settle Pub/Sub deliveries: acknowledge, retry now or after a delay, drop, cap the deliveries and move what keeps failing to a dead-letter topic."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Acknowledgement and retries

This guide settles each Pub/Sub delivery by the handler's outcome, retries now or after a delay, and caps the deliveries with a [dead-letter topic](../components/settlement/dead-letters.md).

Pub/Sub settles every message on its own.
An acknowledged message is done; a message given back is delivered again, possibly to another consumer.
The subscription counts the deliveries and moves a message that ran out of them to its dead-letter topic.
The crate maps the handler's outcome onto these verbs, and the mount site's cap onto the subscription's dead-letter policy.

The example is a payments service that charges cards through a bank.

## The handler

The bank answers a charge in four ways, and each needs a different outcome.
`Charged` is done: `ack`.
`Held` means the bank reviews the payment, which takes a while: [`retry_after`](../components/settlement/retry-after.md) asks again in 30 seconds.
`Unreachable` means the bank did not answer: `retry` asks again at once.
`Declined` means the card is closed, and another try gets the same answer, so the payment is dropped.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/retries.rs#charge"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#charge"
```

</TabItem>
</Tabs>

On Pub/Sub the outcomes become these calls:

| Outcome | What Pub/Sub does |
|---|---|
| `ack` | acknowledges; the message is not delivered again |
| `retry` | nacks; the subscription delivers the message again |
| `retry_after(delay)` | the service holds the message under its lease for the delay, then nacks it |
| `drop` | acknowledges; on the last delivery the cap allows, nacks, so the message goes to the dead-letter topic |

A subscription with no retry policy delivers a nacked message again at once.
A subscription with an exponential backoff policy, set by operations, waits first, and then `retry` waits too.

Pub/Sub has no delayed nack, so [`retry_after`](../components/settlement/retry-after.md) keeps the message in the service.
The client goes on extending its lease for the delay, and nacks it when the delay is over.
The delay must fit inside the subscription's [`max_lease`](../components/subscriptions/max-lease.md), an hour by default; a longer one comes back at once.
A service that stops during the delay stops extending the lease, and the message comes back when the lease runs out.
[Flow control and leases](flow-control.md) covers the lease.

[`DELIVERY_ATTEMPT_HEADER`](../components/settlement/attempts.md) is the delivery's attempt number, the first delivery being 1.
Pub/Sub counts attempts only on a subscription with a dead-letter policy, and its count is an estimate that may miss a delivery.

## The cap and the dead-letter topic

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/retries.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#app"
```

</TabItem>
</Tabs>

[`max_attempts`](api:ruststream::runtime::Mounting::max_attempts) and [`dead_letter`](api:ruststream::runtime::Mounting::dead_letter) become the subscription's own dead-letter policy, written at startup.
After the fifth delivery Pub/Sub stops delivering the payment and publishes it to `payments-dead`, payload and attributes as they arrived.
The service publishes nothing itself, so a crash cannot lose the move.
Pub/Sub accepts a cap from 5 to 100; anything else, or one half of the pair alone, fails the startup.
A dropped payment is acknowledged and goes nowhere; the dead-letter topic holds only the payments that ran out of deliveries.

The subscription `payments-workers` is created with the policy, and the topic `payments-dead` with it.
A subscription operations created gets the policy written onto it at startup instead.
In production the Pub/Sub service account of the project needs permission to publish to the dead-letter topic and to subscribe to the source subscription; [Running in production](production.md) lists the roles.

## Reading the dead-letter topic

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/retries.rs#review"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#review"
```

</TabItem>
</Tabs>

The dead-letter topic is an ordinary topic, and `review` reads it through a subscription of its own.
A message published to a topic with no subscription is lost, so the subscription exists before the first payment runs out of deliveries.
Here `review` creates it at startup.

## Exactly-once subscriptions

A subscription with exactly-once delivery on guarantees that an acknowledged message is not delivered again.
It is a setting of the subscription, made when operations create it; the service needs no setting of its own.
The crate reads it off each delivery, and on such a subscription `ack` and `retry` wait for Pub/Sub to confirm them.
An acknowledgement Pub/Sub rejects, for example after the lease ran out, fails the settlement with an error that names the cause, and the message may come back.
The guarantee holds for subscribers in the subscription's region, so the service connects through that region's [endpoint](../components/broker/endpoint.md).

## Testing

The tests in the example file publish one payment per answer.
A held payment comes back only once 30 seconds have passed on the paused clock.
An unreachable bank costs five deliveries, and then `review` receives the payment.
A declined payment is handled once and never reaches the review.
[Testing a Pub/Sub service](testing.md) covers the harness.
