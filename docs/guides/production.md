---
sidebar_position: 8
description: "Run a Pub/Sub service on Google Cloud: credentials, a regional endpoint, topology owned by operations, IAM roles and exactly-once in one region."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Running in production

This guide takes a service from the emulator to Google Cloud: how it signs in, which endpoint it reaches, who owns the topology and which roles it needs.

The example is a payments service configured from its environment.

## Credentials and the endpoint

{/* single-form: the broker's configuration is the same code with and without the macros */}
```rust title="src/main.rs" reference="examples/guide/production.rs#config"
```

Without [`emulator`](../components/broker/emulator.md), the broker reaches Google Cloud.
It signs in with Application Default Credentials, so the code names no credentials.
On GKE, Cloud Run and Compute Engine they come from the metadata server: the service runs as the service account of its workload.
On a developer machine `gcloud auth application-default login` writes them, and `GOOGLE_APPLICATION_CREDENTIALS` names a key file anywhere else.
A service that must sign in as a particular account builds a credentials value with `google-cloud-auth` and passes it to [`credentials`](../components/broker/credentials.md).
The clients ask for a token on their first call to Pub/Sub, so wrong credentials show up as the error of that call: a topology call at startup, a subscription's stream or a publish.

The global endpoint routes each request to a nearby region.
[`endpoint`](../components/broker/endpoint.md) pins the service to one region, here from `PUBSUB_ENDPOINT`.
Two guarantees need that: messages of one [ordering key](ordering.md) arrive in order only when they are published to one region, and an exactly-once subscription delivers once only to subscribers in its own region.
The endpoint applies to every client of the broker, the startup's topology calls included.

## The service

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/production.rs#service"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/production.rs#service"
```

</TabItem>
</Tabs>

In production the topics and subscriptions are usually infrastructure, created by Terraform or `gcloud` with their retention, filters and retry policy.
The service then names its subscription, `#[subscriber("payments-workers")]`, and creates nothing.
The dead-letter declaration still applies: at startup the service writes it onto the subscription as its dead-letter policy.
To keep the policy with the infrastructure as well, operations set the same cap and topic, and the update at startup changes nothing.
[`create_with_topic`](../components/subscriptions/create-with-topic.md) suits a service that owns its topology, and the emulator.

Startup stops at the first topology call Pub/Sub refuses, with [`PubSubError::Admin`](api:ruststream_gcp_pubsub::PubSubError::Admin) naming the resource.
A subscription that is not there fails on its stream with [`PubSubError::Receive`](api:ruststream_gcp_pubsub::PubSubError::Receive).

## Roles

The service account of the service needs these permissions:

| The service | Permissions | Predefined role |
|---|---|---|
| reads a subscription | `pubsub.subscriptions.consume` on it | `roles/pubsub.subscriber` |
| publishes to a topic | `pubsub.topics.publish` on it | `roles/pubsub.publisher` |
| declares a dead-letter policy on a subscription | `pubsub.subscriptions.update` on it | `roles/pubsub.editor` |
| uses `create_with_topic` | `pubsub.topics.get`, `pubsub.topics.create`, `pubsub.topics.attachSubscription`, `pubsub.subscriptions.get`, `pubsub.subscriptions.create` in the project | `roles/pubsub.editor` |

A dead-letter policy also needs the Pub/Sub service agent of the project, `service-{project-number}@gcp-sa-pubsub.iam.gserviceaccount.com`.
It publishes to the dead-letter topic, so it needs `roles/pubsub.publisher` there, and it acknowledges the moved message, so it needs `roles/pubsub.subscriber` on the source subscription.

## Exactly-once delivery

Exactly-once is a setting of the subscription, made by operations when they create it.
The crate needs no setting: on such a subscription each acknowledgement waits for Pub/Sub to confirm it, and one Pub/Sub rejects fails with an error that names the cause.
The service connects through the regional endpoint of the subscription's region, because the guarantee holds there only.
[Acknowledgement and retries](retries.md#exactly-once-subscriptions) covers what changes for a handler.

## Testing

The tests start the same service with a `Config` of their own, in process, where nothing is dialled and no credentials are read.
`app` reads the environment, so `main` fails at once without `GOOGLE_CLOUD_PROJECT`, before any connection.
