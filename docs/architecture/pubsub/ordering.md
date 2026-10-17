---
title: Ordering
sidebar_position: 2
description: "Messages that share an ordering key, published in one region, reach a subscription with message ordering in the order they were published."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "Two ordering keys from one publisher: each key is published one batch at a time and delivered in its own order",
  "text": "The diagram as text",
  "publisher": ["Publisher", "one batch in flight per key"],
  "keyA": ["Key order-1:", "#1, #2, #3"],
  "keyB": ["Key order-2:", "#1, #2"],
  "topic": ["Topic", "published to in one region"],
  "subscription": "Subscription with message ordering",
  "deliveredA": ["order-1:", "#1, then #2,", "then #3"],
  "deliveredB": ["order-2:", "#1, then #2"],
  "redelivery": ["A redelivery of order-1 #2 brings #3 back too,", "even when #3 was acknowledged"],
  "inOrder": "in key order"
};

# Ordering

Messages that share an ordering key, published in one region, reach a subscription with message
ordering in the order they were published.

<Diagram name="ordering" words={words} />

Order holds per key and nowhere else. Messages of different keys, and messages without a key,
arrive in any order. Ordering is a property of the subscription, set when it is created and fixed
after that. A subscription without it delivers keyed messages in any order.

The client publisher keeps one batch in flight per key, so a key's messages reach the service in
the order the application published them. Messages without a key are sent concurrently. A failed
publish pauses its key: the client refuses that key's later messages until the application
resumes it.

The service orders what it receives in one region. Publishers of one key in several regions get no
order between them, and a regional endpoint
([`endpoint`](api:ruststream_gcp_pubsub::PubSubBroker::endpoint)) keeps them in one. Subscribers
may connect from anywhere.

Messages of one key usually go to the same subscriber of the subscription, but the service may move
a key to another subscriber. A key is at most 1 KB, and one key publishes at most 1 MBps. With a
dead-letter policy on the subscription, the order may not hold, because dead-lettering is best
effort.

## In this crate

A publish takes its key from the call's
[`ordering_key`](api:ruststream_gcp_pubsub::PubSubOrdering::ordering_key) step first, then from a
`partition-key` header, then from the key the
[`PubSubPublish`](api:ruststream_gcp_pubsub::PubSubPublish::ordering_key) policy fixed at the mount
site. The key travels as the message's ordering key, never as an attribute. When a keyed publish
fails, the crate resumes the key and returns the error, so the next publish under that key is
sent.

A delivery reports its key as the `partition-key` header. Keyed worker lanes
([`workers(n, by_key)`](https://powersemmi.github.io/ruststream/latest/components/subscribers/dispatch/workers/))
hash it, so one key keeps its order inside the service too. The subscription the crate creates through
[`create_with_topic`](api:ruststream_gcp_pubsub::GooglePubSub::create_with_topic) has message
ordering on. A subscription it finds keeps its own setting.

The rules are Google's ([ordering messages](https://docs.cloud.google.com/pubsub/docs/ordering));
the publisher's are the `google-cloud-pubsub` 1.3 client's
([`Publisher`](https://docs.rs/google-cloud-pubsub/1.3.0/google_cloud_pubsub/client/struct.Publisher.html)).
