---
title: Redelivery
sidebar_position: 3
description: "Pub/Sub delivers a message again after a nack or an expired deadline, until a dead-letter policy moves it to a dead-letter topic."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "One delivery on Pub/Sub: an ack removes the message, a nack or an expired deadline brings it back until the dead-letter cap",
  "text": "The diagram as text",
  "delivery": ["A delivery of one message", "delivery_attempt: n"],
  "ack": ["ack:", "removed from", "the subscription"],
  "nack": ["nack, or the", "deadline expires"],
  "policy": ["Dead-letter policy:", "is n below", "maxDeliveryAttempts?"],
  "deadLetter": ["At the cap:", "published to the", "dead-letter topic"],
  "redeliver": ["Below the cap, or", "no policy: the retry", "policy waits, then", "it comes back"],
  "approximate": ["The attempt count is approximate", "and can reset to zero"],
  "below": "below",
  "atCap": "at the cap"
};

# Redelivery

Pub/Sub delivers a message again after a nack or an expired deadline, until a dead-letter policy
moves it to a dead-letter topic.

<Diagram name="redelivery" words={words} />

A subscription redelivers at once by default. Its retry policy can add an exponential backoff
instead, between 0 and 600 seconds (10 and 600 by default). The backoff is best effort: under many
nacks a message may wait less.

A dead-letter policy is a property of the subscription: a dead-letter topic and
`maxDeliveryAttempts`, from 5 to 100 (5 by default). Only a subscription with one counts attempts,
and only then does a delivery carry `delivery_attempt`. Because forwarding is best effort, a
message may be delivered a few more times than the cap.

The service moves a message by publishing it to the dead-letter topic and acknowledging it in the
subscription. Its service agent needs the publisher role on that topic and the subscriber role on
the subscription. A dead-letter topic with no subscription of its own loses what it receives.

## Exactly-once delivery

Exactly-once delivery is another property of the subscription, for pull and streaming pull. An ack
the service confirms is final: the message is not delivered again. The client must ask for that
confirmation, and the guarantee holds only for subscribers that connect in the same region, which a
regional endpoint ensures. Its deliveries take noticeably longer than on a standard subscription.

## In this crate

A delivery reports `delivery_attempt` as the `pubsub-delivery-attempt` header. A registration's
[`max_attempts(n)`](api:ruststream::runtime::Mounting::max_attempts) and
[`dead_letter(topic)`](api:ruststream::runtime::Mounting::dead_letter) become the subscription's
dead-letter policy, written when the subscription opens. A cap outside 5 to 100, or one of the two
declared without the other, fails the start before any call to the service. How each handler
outcome reaches this picture is on [Settlement](../settlement.md).

The rules are Google's
([retry policy](https://docs.cloud.google.com/pubsub/docs/subscription-retry-policy),
[dead-letter topics](https://docs.cloud.google.com/pubsub/docs/handling-failures),
[exactly-once delivery](https://docs.cloud.google.com/pubsub/docs/exactly-once-delivery)).
