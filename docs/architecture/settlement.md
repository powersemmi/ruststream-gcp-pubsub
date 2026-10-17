---
title: Settlement
sidebar_position: 5
description: "What ack, retry, retry_after and drop become on Pub/Sub, which acknowledges, rejects or keeps leasing each message."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "Each handler outcome and the call the crate makes for it on Pub/Sub",
  "text": "The diagram as text",
  "handler": "Handler",
  "onPubSub": "On Pub/Sub",
  "ack": ["ack()"],
  "acked": ["Acknowledged:", "removed from the subscription"],
  "retry": ["retry()"],
  "nacked": ["Nacked: delivered again", "after the retry policy's", "wait, or dead-lettered"],
  "retryAfter": ["retry_after(d)"],
  "held": ["Held for d while the", "client extends the lease,", "then nacked; refused if", "d exceeds max_lease"],
  "drop": ["drop()"],
  "dropped": ["Acknowledged:", "never delivered again"],
  "dropAtCap": ["drop() on the", "last attempt"],
  "deadLettered": ["Nacked, so the service", "moves it to the", "dead-letter topic"],
  "exactlyOnce": ["On an exactly-once subscription the ack and the nack", "wait for the service to confirm them"]
};

# Settlement

What `ack`, `retry`, [`retry_after`](https://powersemmi.github.io/ruststream/latest/components/subscribers/messages/retry-after/)
and `drop` become on Pub/Sub, which acknowledges, rejects or keeps leasing each message.

<Diagram name="settlement" words={words} />

Pub/Sub has no verb for "discard without redelivery" other than the ack, so `drop()` acknowledges.
The exception is the last delivery a declared dead-letter policy allows. There a nack is what
moves the message to the dead-letter topic, so `drop()` nacks. The crate reads the attempt from
the delivery and the cap from the registration's
[`max_attempts(n)`](api:ruststream::runtime::Mounting::max_attempts). A cap set on the subscription
outside the service's code is unknown to it, and `drop()` acknowledges there. The attempt count is
approximate, so a count that reset makes `drop()` acknowledge too.

A decode failure drops the message by default
([`FailurePolicy`](api:ruststream::runtime::FailurePolicy)), so on Pub/Sub it is acknowledged.

The subscription counts attempts and moves spent messages itself. `retry()` under a declared
`max_attempts` is a nack like any other, and the service applies the cap. The runtime publishes no
retry copy on this broker.

`retry_after(d)` keeps the delivery in the process. A task on the runtime the broker connected on
waits for `d` and then nacks. Meanwhile the client goes on extending the lease, and the message
counts against [`max_outstanding`](api:ruststream_gcp_pubsub::GooglePubSub::max_outstanding). A
`d` longer than [`max_lease`](api:ruststream_gcp_pubsub::GooglePubSub::max_lease) is refused at
the call with `PubSubError::DelayBeyondLease`, because the lease would end first. If the process
dies during the wait, the lease ends and the service delivers the message again: the delay is
lost, the message is not.

On an exactly-once subscription the crate uses the client's confirmed ack and nack. An ack that
returns has been accepted by the service, and a refused one is logged as a failed ack. On any
other subscription an ack is best effort, and the message may still come back.
