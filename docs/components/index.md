---
sidebar_position: 0
description: "The parts of ruststream-gcp-pubsub a Pub/Sub service uses: the broker, its subscription, its publish policy and its testing transport."
---

# Components

The parts of `ruststream-gcp-pubsub` a Pub/Sub service uses: the broker
([`PubSubBroker`](api:ruststream_gcp_pubsub::PubSubBroker)), its subscription descriptor
([`GooglePubSub`](api:ruststream_gcp_pubsub::GooglePubSub)), its publish policy
([`PubSubPublish`](api:ruststream_gcp_pubsub::PubSubPublish)) and the in-process transport a
[`TestApp`](api:ruststream::testing::TestApp) test runs on.
