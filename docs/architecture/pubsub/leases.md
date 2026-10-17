---
title: Leases
sidebar_position: 1
description: "A streaming pull leases each message: the client extends its ack deadline while the handler works, and flow control caps how many it holds."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "A streaming pull: messages arrive under a lease the client extends while the application holds them, and acks go back in batches",
  "text": "The diagram as text",
  "subscription": ["Pub/Sub subscription", "an ack deadline per message"],
  "client": "google-cloud-pubsub",
  "pull": ["Streaming pull", "up to max_outstanding"],
  "lease": ["Lease set", "checked every 3 s"],
  "flush": ["Acks and", "nacks, sent", "in batches", "every 100 ms"],
  "app": "Application",
  "handler": ["Handler", "holds the message"],
  "settle": ["Settles it:", "ack or nack"],
  "expired": ["Past max_lease the client stops extending:", "the deadline runs out and the message comes back"],
  "messages": "messages",
  "extends": "extends",
  "settled": "settled",
  "answers": "ack, nack"
};

# Leases

A streaming pull leases each message: the client extends its ack deadline while the handler works,
and flow control caps how many it holds.

<Diagram name="leases" words={words} />

A streaming pull is one long-lived gRPC stream per subscription. The service sends messages down
it as they arrive, and each one comes with an ack deadline. A message that is neither acknowledged
nor extended before its deadline is delivered again, possibly to another subscriber.

The client library holds that deadline off. It sends `modifyAckDeadline` for every message the
application still holds, pushing each deadline at most
[`ack_extension`](api:ruststream_gcp_pubsub::GooglePubSub::ack_extension) ahead (60 seconds by
default, clamped to 10 to 600). It does so for at most
[`max_lease`](api:ruststream_gcp_pubsub::GooglePubSub::max_lease), one hour by default. A process
that dies sends no extensions, so its messages return to the subscription once their deadlines
pass.

Flow control is on the stream. The service stops sending while
[`max_outstanding`](api:ruststream_gcp_pubsub::GooglePubSub::max_outstanding) messages (1000 by
default) or 100 MiB are unsettled on it, and resumes when the count drops below. A slow handler
therefore slows its own stream down, and the messages it has not received wait in the
subscription.

An ack removes the message from the subscription. A nack is a `modifyAckDeadline` with a deadline
of zero: the message leaves the lease set, and the service delivers it again, as
[Redelivery](redelivery.md) draws.

The figures are the `google-cloud-pubsub` 1.3 client's
([`Subscribe`](https://docs.rs/google-cloud-pubsub/1.3.0/google_cloud_pubsub/builder/subscriber/struct.Subscribe.html))
and Google's
([lease management](https://docs.cloud.google.com/pubsub/docs/lease-management)).
