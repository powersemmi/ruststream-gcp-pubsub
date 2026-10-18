---
title: Lifecycle
sidebar_position: 3
description: "PubSubBroker::new records configuration, connect builds the clients, startup opens each subscription, and shutdown flushes the publishers."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "The broker's three states as types, and what each transition does on Pub/Sub",
  "text": "The diagram as text",
  "states": "Broker",
  "pubsub": "On Pub/Sub",
  "unconnected": ["PubSubBroker::new", "(project)"],
  "connected": ["ConnectedPubSubBroker"],
  "closed": ["Closed: ()"],
  "config": ["Configuration only,", "no I/O"],
  "clients": ["Four clients built", "for the connection"],
  "subscriptions": ["Each subscription:", "topology created", "where declared, then", "its stream opened"],
  "publisher": ["A topic's first publish", "starts its publisher"],
  "flush": ["Closed, then every", "topic's publisher", "flushed"],
  "afterShutdown": ["A publisher used before connect or after shutdown", "answers NotConnected, never a silent success"],
  "connect": "connect(self)",
  "shutdown": "shutdown(self)"
};

# Lifecycle

[`PubSubBroker::new`](api:ruststream_gcp_pubsub::PubSubBroker::new) records configuration,
connect builds the clients, startup opens each subscription, and shutdown flushes the publishers.

<Diagram name="lifecycle" words={words} />

`new` does no I/O, so the service's builder stays synchronous. A publisher taken from the broker
before connect shares the connection the runtime fills in later.

Each subscription opens once, at startup. Its descriptor is checked first: an empty name, a
dead-letter cap outside 5 to 100, or half a declaration fails the start before any call to the
service. A descriptor with
[`create_with_topic`](api:ruststream_gcp_pubsub::GooglePubSub::create_with_topic) then creates
the topic, the dead-letter topic and the subscription where they are missing, the subscription
with message ordering on. Against Pub/Sub that needs the permissions to create topics and
subscriptions. A subscription that already exists and has a declared dead-letter policy gets that
policy written onto it. Then the streaming pull opens.

A registration mounted by a bare name declares its retries the same way. Two registrations of one
subscription with different declarations fail the start, because a subscription carries one
dead-letter policy.

Shutdown marks the connection closed, then flushes every topic's publisher so batched messages
reach the service. The closed state carries nothing. A subscription stops when the runtime drops
it: the client stops reading and keeps extending the leases of messages still held, until they
settle.
