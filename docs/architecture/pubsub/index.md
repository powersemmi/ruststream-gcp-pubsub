---
title: Topics
sidebar_position: 0
description: "A Pub/Sub topic hands every message to each subscription attached to it, and the subscribers of one subscription share its messages."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "A publisher publishes to one topic; two subscriptions each receive every message, and the two subscribers of one share them",
  "text": "The diagram as text",
  "publisher": "Publisher",
  "topic": "Topic orders",
  "billing": "Subscription billing",
  "billing1": ["Subscriber 1:", "messages 1 and 3"],
  "billing2": ["Subscriber 2:", "message 2"],
  "audit": "Subscription audit",
  "audit1": ["One subscriber:", "messages 1, 2, 3"],
  "retained": ["A subscription keeps a message until it is acknowledged", "or its retention runs out, with or without a subscriber"],
  "publishes": "publishes",
  "copy": "every message"
};

# Topics

A Pub/Sub topic hands every message to each subscription attached to it, and the subscribers of
one subscription share its messages.

<Diagram name="topics" words={words} />

A publisher names a topic and nothing else. A subscription is a resource of its own, attached to
exactly one topic. A topic may have many subscriptions.

A subscription receives the messages published to its topic after it was created. Each
subscription gets its own copy of every message, so billing and audit never take messages from
each other.

The subscribers of one subscription share its messages: each gets a part, and no two get the same
part. Adding a subscriber to a subscription spreads its load. Adding a subscription adds a reader
of everything.

In `ruststream-gcp-pubsub`, a handler names the subscription it reads, through
[`GooglePubSub`](api:ruststream_gcp_pubsub::GooglePubSub) or a bare name, and a publisher names the
topic it writes to. The model is Google's
([subscription overview](https://docs.cloud.google.com/pubsub/docs/subscription-overview),
[Pub/Sub basics](https://docs.cloud.google.com/pubsub/docs/pubsub-basics)). How a subscriber
holds a message while it works is on [Leases](leases.md).
