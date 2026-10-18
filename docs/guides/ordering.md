---
sidebar_position: 6
description: "Keep the messages of one key in order on Pub/Sub: ordered subscriptions, keyed lanes in the service, and the key on what it publishes."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Ordering keys

This guide keeps the messages of one key in order: on the way in, through the service's own concurrency, and on the way out.

Pub/Sub delivers messages in no particular order by default.
A message published with an ordering key, to a subscription with message ordering on, arrives after every earlier message of the same key.
Messages of different keys still arrive in any order, and that is what lets them be handled side by side.
The guarantee holds for messages published to one region.

The example is a ledger service: an entry changes the balance of one account, and the entries of one account must apply in order.
The account service publishes each entry with the account's id as its ordering key.

## Ordered delivery, handled in lanes

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/ordering.rs#post"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/ordering.rs#post"
```

</TabItem>
</Tabs>

[`create_with_topic`](../components/subscriptions/create-with-topic.md) creates `ledger-posting` with message ordering on.
Pub/Sub fixes that setting when the subscription is created, so a subscription operations own has it only if they turned it on.

`workers(8, by_key)` handles up to eight entries at once in eight lanes.
Every entry of one account goes to the same lane, and a lane handles its entries one after another.
So the accounts are posted side by side, and the entries of one account keep their order.
A delivery reports its ordering key in the [`partition-key`](../components/publishing/ordering/partition-key.md) header, which is the key the lanes read.
In the manual form the lanes are the `workers_by_key` step of the chain.

When a delivery of one key is redelivered, Pub/Sub delivers the later messages of that key again too, so a handler of an ordered subscription is idempotent.

## The key on what the service publishes

`post` publishes the posted entry to `postings` under the account's key, named on the call with the [`ordering_key`](../components/publishing/ordering/ordering-key.md) step.
The step comes with the prelude, and it needs the `Out` parameter to name the crate's settings type, `Publisher<Options = PubSubPublishOptions>`.
A publish without a key carries no ordering.

A reply has no call to name a key on.
A [publish transform](../components/publishing/ordering/transform.md) sets it per delivery:

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/ordering.rs#notify"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/ordering.rs#notify"
```

</TabItem>
</Tabs>

`UnderTheEntrysKey` reads the key the entry arrived with and gives it to the statement, so the statements of one account stay in order too.
A key that is the same for every message of a mount site goes on the policy instead: [`Publish::default().ordering_key(..)`](../components/publishing/ordering/policy.md).
Where several name one, the call wins over the `partition-key` header, and the header over the policy.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/ordering.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/ordering.rs#app"
```

</TabItem>
</Tabs>

A failed publish with a key pauses that key in the client; the crate resumes it, so the next publish of the key goes out, and the failure reaches the handler as an error.
Against the emulator, create the topics `postings` and `statements` before the service starts, as in [Publishing and replies](publishing.md#running-it).

## Testing

The tests publish an entry with the account's key in the `partition-key` header, the way the account service's key reaches a delivery.
They check the key on the posting with `tb.out::<DefaultSlot>().with_options(..)`, and on the statement with `published(..).with_options(..)`.
The in-process Pub/Sub does not model ordered delivery; the [ordering component page](../components/publishing/ordering/index.md) holds a test of it against the emulator.
