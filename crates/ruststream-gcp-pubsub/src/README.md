Google Cloud Pub/Sub broker for the [`RustStream`](https://github.com/powersemmi/ruststream)
messaging framework.

Handlers, routers, codecs and middleware come from the framework; this crate supplies the
transport, over the official [`google-cloud-pubsub`](https://docs.rs/google-cloud-pubsub) client.
Pub/Sub keeps its two resources apart: a service publishes to a topic, and a handler consumes a
subscription, which queues that topic's messages for it. One streaming pull subscription is one
handler's message stream, and acknowledgement on it is native and per message.

The framework's half of a service - what a handler may take, routing, codecs, middleware, the
generated document - is documented with the core:
[runtime](https://docs.rs/ruststream/latest/ruststream/runtime/index.html) and
[codec](https://docs.rs/ruststream/latest/ruststream/codec/index.html). This page is the Pub/Sub
half: the descriptor, the publish policy, the settings and the losses.

# A service

A handler names the subscription it consumes, and the application object mounts it on the broker:

```
# mod demo {
use ruststream_gcp_pubsub::prelude::*;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

#[subscriber(GooglePubSub::new("orders-workers").create_with_topic("orders"))]
async fn handle(order: &Order) -> HandlerOutcome {
    println!("got order {}", order.id);
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        PubSubBroker::new("my-project").emulator("localhost:8085"),
        |b| {
            b.include(handle);
        },
    )
}
# }
# fn main() {}
```

`cargo run -- run` starts it. [`PubSubBroker::new`] records the project id and does no I/O, so the
synchronous builder composes; the runtime authenticates and connects once at startup. Without
[`emulator`](PubSubBroker::emulator) the broker dials `pubsub.googleapis.com` with Application
Default Credentials, and `create_with_topic` is dropped: a production subscription is managed as
infrastructure, not by the service that consumes it.

More of both halves in [`examples/`](https://github.com/powersemmi/ruststream-gcp-pubsub/tree/main/crates/ruststream-gcp-pubsub/examples).

# Subscribing

[`GooglePubSub`] is the one subscription form this crate has, and it carries every setting a
subscription takes. It names an existing subscription by short id or by full
`projects/{project}/subscriptions/{name}` resource name.

| Step | What it decides | Default |
| --- | --- | --- |
| [`create_with_topic`](GooglePubSub::create_with_topic) | creates the subscription and its topic on subscribe | off |
| [`max_outstanding`](GooglePubSub::max_outstanding) | how many deliveries may be unsettled at once - the real prefetch | the client's 1000 |
| [`ack_extension`](GooglePubSub::ack_extension) | how far one background extension of the ack deadline reaches | 60s, clamped by the protocol to 10s..=600s |
| [`batch_wait`](GooglePubSub::batch_wait) | how long a partial batch waits for the rest of it | 50ms |
| [`max_lease`](GooglePubSub::max_lease) | how long one delivery may stay unsettled before the client stops extending it | 1 hour |

`#[subscriber("orders-workers")]` is the by-name form, and it opens the same descriptor with every
default, so the subscription has to exist already. `#[subscriber(GooglePubSub)]` with
`.name("orders-workers")` at the mount site is that descriptor with its name left to the
deployment. An empty subscription or topic name is refused before any I/O, as
[`PubSubError::InvalidDescriptor`].

Both forms answer `Copies = BrokerMoves`: a spent delivery is moved by Pub/Sub itself, and the
service publishes no retry copies. `.out_retry(policy)` over either one therefore does not
compile, and the error names the descriptor.

## Capping the retries

`.max_attempts(n).dead_letter(name)` at the mount site becomes the subscription's own dead-letter
policy - `n` is its `maxDeliveryAttempts` and `name` its dead-letter topic - written when the
service starts. A descriptor with `create_with_topic` creates the dead-letter topic beside its own
and opens the subscription carrying the policy; a subscription that already exists receives it as
an update.

Pub/Sub bounds the count at 5..=100, and a cap outside that range refuses to start. So does half a
declaration: the policy is one field of the subscription resource, and it cannot carry a cap
without a destination or a destination without a cap.

A handler that names its subscription with a plain string declares its retries the same way: the
broker takes the declaration for that name and opens the subscription with the policy. What a bare
name does not do is create topology, so the subscription and the dead-letter topic have to exist
already; `GooglePubSub::new("orders-workers").create_with_topic("orders")` is the declaration that
creates them.

One subscription carries one dead-letter policy, so two handlers on the same subscription declare
the same retries. A second declaration that differs refuses to start, rather than leaving whichever
registration mounted last in charge of the policy.

```
# mod demo {
use std::time::Duration;

use ruststream_gcp_pubsub::prelude::*;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Payment {
    id: u64,
    settled: bool,
}

/// A slice parameter is what makes this a batch handler; the mount site names how large a batch
/// may be, and the descriptor how long a partial one waits.
#[subscriber(GooglePubSub::new("payments-workers").batch_wait(Duration::from_millis(200)))]
async fn reconcile(payments: &[Payment]) -> HandlerOutcome {
    if payments.iter().any(|payment| !payment.settled) {
        return HandlerOutcome::retry_after(Duration::from_secs(5));
    }
    println!("settled {} payments", payments.len());
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(
        PubSubBroker::new("my-project"),
        |b| {
            // Pub/Sub gives one payment five deliveries, then publishes it to `payments-dead`.
            b.include(reconcile.batch(nonzero!(50)))
                .max_attempts(nonzero!(5u32))
                .dead_letter("payments-dead");
        },
    )
}
# }
# fn main() {}
```

## Settling a delivery

| Handler outcome | The call | The effect |
| --- | --- | --- |
| `HandlerOutcome::ack()` | acknowledge | the delivery is done |
| `HandlerOutcome::retry()` | nack | the message becomes available again |
| `HandlerOutcome::drop()` | acknowledge | the message is not redelivered |
| `HandlerOutcome::retry_after(d)` | the delivery is held, then nacked | it comes back after `d` |

Pub/Sub has no drop-without-redelivery verb, which is why `drop()` acknowledges. The one exception
is the last delivery a dead-letter policy allows: there the rejection is what carries the message
to the dead-letter topic, so it is rejected rather than acknowledged, and a message the
declaration asked to keep is kept.

Pub/Sub has no delayed nack either, so `retry_after(delay)` is carried by the process: the crate
holds the delivery for `delay` and then rejects it. The client goes on extending the ack deadline
of a delivery nothing has settled, so a held delivery is leased rather than lost, and it still
counts against `max_outstanding` while it waits. The budget is
[`max_lease`](GooglePubSub::max_lease), and a longer delay is refused at the call with
[`PubSubError::DelayBeyondLease`]. If the process dies while a delivery is held, the lease stops
being extended and the subscription redelivers on its own: the delay is lost there, not the
message.

On an exactly-once subscription `ack` takes the confirmed form, so `Ok` means the service accepted
it and a refused acknowledgement surfaces as `AckError::Broker` instead of passing as success.
Exactly-once is a setting on the subscription resource, so the same handler code runs against
both kinds.

## Batches

The streaming pull hands over one delivery at a time, so batches are assembled on the client, over
the framework's own buffer. A batch holds at most the size `.batch(n)` named and closes early on
[`batch_wait`](GooglePubSub::batch_wait), whose 50ms default is about one streaming-pull burst.
`max_outstanding` bounds the pull, not the batch.

## What a delivery reports

The crate adds no per-delivery context field: everything a delivery says arrives through the
framework's own surface. `message.partition_key()` and the [`PARTITION_KEY_HEADER`] header both
report the ordering key the message arrived under. `message.redelivery_count()` and the
[`DELIVERY_ATTEMPT_HEADER`] header report which attempt this delivery is, counting from one -
Pub/Sub sends the count only where the subscription has a dead-letter policy, and reports nothing
without one. Nothing in the process counts alongside it and nothing in the process applies the cap:
the subscription's policy is what ends the message, whether the handler asked for the next attempt
at once or after a delay, and the count is there for a handler to read. Every other attribute is a
header, in both directions, with no envelope around it.

There is no log to seek in: a Pub/Sub subscription has no client-addressable position, so this
crate implements neither `Seekable` nor `Positioned` and `.start_at(..)` does not compile.
Repositioning a whole subscription to a timestamp or a snapshot is the admin `seek` call, an
operator's action rather than a mount-site setting.

# Publishing

[`PubSubPublish`] is the publish policy, and the prelude carries it under the uniform mount-site
name `Publish`. It is pure declaration, constructible anywhere; the runtime pairs it with the
connected broker to produce the live [`PubSubPublisher`]. It is also this broker's default policy,
so a replying handler mounted without an `.out_reply(..)` call publishes through it.

A publish addresses a topic, by id or by full resource name. The client publisher for a topic is
created on first use and cached on the broker, so `shutdown` flushes every batch it has buffered.
A publisher handed out before `shutdown` and used after it reports
[`PubSubError::NotConnected`] rather than accepting a message nothing will send.

Pub/Sub gives a message exactly one setting beyond its payload and attributes, its ordering key,
so that is the whole of [`PubSubPublishOptions`]. Messages sharing a key reach one subscriber in
publish order. Three places can name the key of one publish, and the most specific wins:

1. the message's own settings, written by the [`ordering_key`](PubSubOrdering::ordering_key) step
   at a call site or by a publish transform on the position the message leaves through;
2. the [`PARTITION_KEY_HEADER`] header, the portable spelling of the same key, which is how a
   service that names no broker still orders its messages;
3. the key [`PubSubPublish::ordering_key`] fixed for the whole mount site.

A publish that reaches none of the three is unordered, which is Pub/Sub's own default. The key
travels as the message's own field and never as an attribute.

```
# mod demo {
use ruststream_gcp_pubsub::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

/// The reply type names its destination, so the mount site does not.
#[derive(Serialize, Outgoing)]
#[outgoing(name = "receipts")]
struct Receipt {
    order_id: u64,
}

/// Each confirmation goes out under its own order's key, so the body names the step - and with
/// it the broker it runs on.
#[subscriber("orders-workers")]
async fn confirm(
    order: &Order,
    Out(out): Out<impl Publisher<Options = PubSubPublishOptions>>,
) -> HandlerOutcome {
    if out
        .message(order)
        .to("confirmations")
        .ordering_key(format!("order-{}", order.id))
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

#[subscriber("orders-receipts", publish)]
async fn receipt(order: &Order) -> Receipt {
    Receipt { order_id: order.id }
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        PubSubBroker::new("my-project"),
        |b| {
            b.include(confirm).out(DefaultSlot, Publish::default()).build();
            // A reply has no call site, so its key is the policy's.
            b.include(receipt).out_reply(Publish::default().ordering_key("receipts"));
        },
    )
}
# }
# fn main() {}
```

[`ordering_key`](PubSubOrdering::ordering_key) is a position on the framework's publish builder,
between `message(..)` and `publish()`, so the publish it finishes is still the mount site's: the
codec that entry named, its transforms and its slot attribution all hold. It resolves only on a
builder over a Pub/Sub publisher. Where a reply needs a key that differs per delivery, a
`PublishTransform<ForReply<C>, PubSubPublishOptions>` reads the delivery and writes the setting;
it mounts with the ordinary `.transform(..)` step, and naming the options type is what keeps it off
another broker's publisher.

An error on an ordered key pauses that key inside the client. This crate resumes the key and
returns the error, so one failure does not wedge every later publish under it.

Three framework capabilities are absent here, and the calls that need them do not compile:
`TransactionalPublisher` and `OwnedTransactions` (Pub/Sub has no publish transaction; an ordering
key is what groups a run of messages) and `RequestReply` (a reply topic and a correlation
attribute are yours to wire).

# The prelude

`use ruststream_gcp_pubsub::prelude::*;` is one glob: the framework's own prelude, this crate's
broker and descriptor, [`PubSubPublish`] under the name `Publish`, the live publisher, the
[`PubSubOrdering`] step and the [`PubSubPublishOptions`] it writes into.

The two sides of a service import different things, and that is what keeps the vocabularies apart.
A file of handler bodies imports the framework's prelude alone and bounds a slot with a capability
(`Out(out): Out<impl Publisher>`), so it names no broker and moves between brokers untouched. A
routes file imports this glob and attaches policies under their uniform names, so moving a service
changes the one import rather than every mount site.

The one exception is a body that adjusts a per-message setting: it names the
[`ordering_key`](PubSubOrdering::ordering_key) step, imports this glob and bounds its slot
`Out<impl Publisher<Options = PubSubPublishOptions>>`. Its signature then says which broker the
handler is tied to, which is the cost of the setting.

`Partitioned` is deliberately left out of the glob. The framework's prelude already carries
`partition_key` as a defaulted method on `IncomingMessage`, and this crate's deliveries override
it, so a glob holding both traits would make `message.partition_key()` ambiguous on a concrete
delivery.

# The `AsyncAPI` document

The `asyncapi` feature turns on the framework's
[document generation](https://docs.rs/ruststream/latest/ruststream/asyncapi/index.html) for this
broker. The server carries the host clients dial and the protocol key `googlepubsub`, and nothing
else: a scheme, a path or credentials written into an endpoint are cut off before the description
is built, because the document is shared.

What the crate adds is the `googlepubsub` message binding, carrying the `orderingKey` a mount site
fixed. Everything else that binding can describe - labels, message retention, the storage policy,
the schema settings - belongs to the topic resource, which no publish policy configures, and a key
named per message is named at a call site, which is not in the document.

A channel here is a subscription and the binding describes a topic, so a subscription's channel
carries no binding at all: the topic behind a subscription is a connection-time value, and the
document is built before anything connects. A reply's channel is a topic and the mount site names
it, so the document reports that name as the channel's address; the channel binding stays empty
there for the same reason, since its fields configure the topic resource and a publish policy holds
none of them. For the same reason the crate answers no redelivery
address, which costs nothing, since a Pub/Sub subscription moves a spent delivery itself and the
framework's republishing fallback is unreachable from here.

# Testing

The `testing` feature ships an in-process transport, `PubSubTestBroker`, that a service mounts on
as it ships: the same [`GooglePubSub`] opens an in-process subscription, and the same `Publish`
policy pairs with it. The framework's
[`TestApp`](https://docs.rs/ruststream/latest/ruststream/testing/index.html) harness then drives
the real dispatch path. What the stand-in reproduces and what it leaves to the emulator is in the
[`testing`](crate::testing) module overview.

# Operations

Authentication is Application Default Credentials by default;
[`credentials`](PubSubBroker::credentials) takes a `google_cloud_auth` credentials value instead,
and transport security is the client's. [`endpoint`](PubSubBroker::endpoint) names another service
endpoint - a regional one is what keeps a single ordering key in order across publishers in one
region. [`emulator`](PubSubBroker::emulator) points every client at a local Pub/Sub emulator with a
plaintext endpoint and anonymous credentials in one call; the client does not read
`PUBSUB_EMULATOR_HOST` on its own, so the host is named here.

Ordered delivery needs message ordering enabled on the subscription resource; the publish side
alone does not turn it on. Flow control, deadline extension and the lease budget are the three
descriptor settings above, and the client owns their defaults.

Known gaps, each a line: batches are assembled on the client, never on the wire; the retry cap
lives in a range Pub/Sub chose (5..=100) and cannot be finer; a delayed retry cannot outlive the
lease budget; there is no position to seek to; and a bare subscription name carries no descriptor
settings, so the declaration steps over it are silently inert.
