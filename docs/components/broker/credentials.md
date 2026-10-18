---
sidebar_position: 2
title: credentials
description: "credentials replaces Application Default Credentials with a credentials value built by google-cloud-auth."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# credentials

[`credentials`](api:ruststream_gcp_pubsub::PubSubBroker::credentials) replaces Application Default Credentials with a credentials value built by `google-cloud-auth`.

By default every client of the broker authenticates with Application Default Credentials.
They come from `GOOGLE_APPLICATION_CREDENTIALS`, the file `gcloud auth application-default login` writes, or the metadata server of the machine the service runs on.
`credentials` takes a [`Credentials`](https://docs.rs/google-cloud-auth/latest/google_cloud_auth/credentials/struct.Credentials.html) value instead.
The `google-cloud-auth` crate builds one from a service account key, an impersonated service account, or an external account.
The value only records how to obtain a token; the clients ask for one when they call the service.
Startup does not read the credentials, so wrong credentials surface on the first call, as an error of the subscription or the publish.

## Example

The deployment mounts a service account key as a secret, and the broker signs in with it.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pubsub_broker_config.rs#credentials"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pubsub_broker_config.rs#credentials"
```

</TabItem>
</Tabs>
