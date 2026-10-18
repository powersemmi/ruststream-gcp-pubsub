---
title: Design
sidebar_position: 2
description: "Where ruststream-gcp-pubsub sits: between the RustStream runtime and the google-cloud-pubsub clients, a streaming pull per subscription, a publisher per topic."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "The service, the RustStream runtime, this crate, the google-cloud-pubsub clients and Pub/Sub, top to bottom",
  "text": "The diagram as text",
  "service": "Your service",
  "handlers": ["#[subscriber]", "handlers"],
  "app": ["RustStream::new(..)", ".with_broker(..)"],
  "core": "RustStream core",
  "runtime": "Runtime: dispatch, workers, settlement",
  "crate": "ruststream-gcp-pubsub",
  "subscriber": ["PubSubSubscriber", "a pump task per", "subscription"],
  "publishers": ["PubSubPublisher", "a client publisher", "per topic"],
  "message": ["PubSubMessage", "holds the lease"],
  "broker": ["PubSubBroker", "configuration"],
  "descriptor": ["GooglePubSub", "the subscription"],
  "inProcess": ["In-process transport", "under testing"],
  "client": "google-cloud-pubsub",
  "pull": ["Subscriber", "streaming pull"],
  "clients": ["Publisher,", "TopicAdmin, ..."],
  "pubsub": "Pub/Sub, or the emulator",
  "mounted": "mounted",
  "polls": "polls the stream"
};

# Design

Where `ruststream-gcp-pubsub` sits: between the RustStream runtime and the `google-cloud-pubsub`
clients, a streaming pull per subscription, a publisher per topic.

<Diagram name="design" words={words} />

The crate implements the core's broker traits over Google's
[`google-cloud-pubsub`](https://docs.rs/google-cloud-pubsub/1.3.0/google_cloud_pubsub/) client.
The runtime above it is the same for every broker. Everything Pub/Sub-specific is in this crate:
the [subscription descriptor](api:ruststream_gcp_pubsub::GooglePubSub), what each handler outcome
does to a lease, and how a publish carries its ordering key.

Connect builds four clients and shares them for the life of the connection: a `Subscriber`, a
`BasePublisher`, a `TopicAdmin` and a `SubscriptionAdmin`. The admins create the topology a
descriptor declares and write the dead-letter policy a registration declares.
[`emulator`](api:ruststream_gcp_pubsub::PubSubBroker::emulator) points every client at a local
emulator with anonymous credentials. Otherwise they use
[`endpoint`](api:ruststream_gcp_pubsub::PubSubBroker::endpoint) and
[`credentials`](api:ruststream_gcp_pubsub::PubSubBroker::credentials) where given, and Application
Default Credentials where not.

Each subscription runs a pump task. It reads the client's stream and forwards each delivery into a
channel of 16, which the runtime polls. Batches are assembled on that channel. The client's lease
travels inside each [`PubSubMessage`](api:ruststream_gcp_pubsub::PubSubMessage), so a settlement
goes straight to the client.

The [publishers](api:ruststream_gcp_pubsub::PubSubPublisher) of a connection share one client
publisher per topic, created on its first publish. The client batches what they send.

Under the `testing` feature, [`TestApp`](api:ruststream::testing::TestApp) connects the same broker
to an in-process transport instead of the clients. It models topics and subscriptions, competing
subscribers, acknowledgement and redelivery, attempt counting and dead-lettering. A build without
the feature carries none of it.

How the pieces start and stop is on [Lifecycle](lifecycle.md), and the way of one message on
[Path](path.md).
