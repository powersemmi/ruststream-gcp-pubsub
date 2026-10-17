---
title: Path
sidebar_position: 4
description: "The way of one Pub/Sub message through a RustStream service: the streaming pull, the delivery, the handler, the reply and the ack."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "The path of one Pub/Sub message that is answered, from the streaming pull to its ack",
  "text": "The diagram as text",
  "pull": ["Streaming pull", "the client leases the message"],
  "pump": ["Pump task", "forwards into a channel of 16"],
  "message": ["PubSubMessage", "attributes become headers, uncopied"],
  "dispatch": ["Dispatch", "one loop, or workers"],
  "decode": ["Decode", "from the message's data"],
  "body": ["Handler", "async fn under #[subscriber]"],
  "encode": ["Reply", "encoded into a buffer it hands over"],
  "publisher": ["PubSubPublisher", "awaits the message id"],
  "settle": ["Settlement", "ack, nack or a held retry"],
  "ack": ["Ack", "sent with the client's next batch"]
};

# Path

The way of one Pub/Sub message through a RustStream service: the streaming pull, the delivery,
the handler, the reply and the ack.

<Diagram name="path" words={words} />

The client takes the message off the stream and starts its lease. The pump task wraps it in a
[`PubSubMessage`](api:ruststream_gcp_pubsub::PubSubMessage) without copying: the data stays the
client's buffer, and the attributes move into the headers. The ordering key becomes the
`partition-key` header, and the attempt count `pubsub-delivery-attempt` where the subscription
has a dead-letter policy.

The handler's reply is encoded into a buffer that becomes the outgoing message's data as it is,
because the client keeps the bytes it publishes. The headers become attributes; a header that is
not UTF-8 fails the publish, because an attribute is text. A key named for the publish becomes the
message's ordering key, as [Ordering](pubsub/ordering.md) draws.

The publish returns once the service has assigned the message an id. A reply that fails to
publish settles the delivery with a retry, so the message comes back.

The delivery settles after the reply. An ack leaves with the client's next batch of
acknowledgements, within about 100 milliseconds. What each outcome becomes is on
[Settlement](settlement.md).
