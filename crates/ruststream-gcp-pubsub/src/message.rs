//! [`PubSubMessage`] and the mapping between `RustStream` headers and Pub/Sub attributes.
//!
//! Message attributes carry headers directly - no envelope format is invented - and the framework's
//! partition key is the message's own ordering key, which a delivery reports back.

use bytes::Bytes;
use google_cloud_pubsub::model::Message as GcpMessage;
use google_cloud_pubsub::subscriber::handler::Handler;
use ruststream::{AckError, HeaderMap, IncomingMessage, OutgoingMessage, Partitioned};

/// Header a delivery reports its ordering key under, which is the framework's partition key.
///
/// Mirrors the in-memory broker's convention, so a handler reading keys works on either broker.
/// It is a report, not an instruction: the key of an outgoing message is a per-message setting
/// ([`PubSubPublishOptions`](crate::PubSubPublishOptions)), and writing this header on one orders
/// nothing.
pub const PARTITION_KEY_HEADER: &str = "partition-key";

/// Header exposing the delivery attempt count on received messages, present when the
/// subscription has a dead-letter policy.
pub const DELIVERY_ATTEMPT_HEADER: &str = "pubsub-delivery-attempt";

/// A message delivered by a [`PubSubSubscriber`](crate::PubSubSubscriber).
///
/// `ack` and `nack(requeue = true)` are native. `nack(requeue = false)` acknowledges: Pub/Sub
/// has no "drop without redelivery" beyond acknowledgement - dead-lettering is the
/// subscription's redrive policy, driven by repeated nacks and expired deadlines, not a
/// per-message verb. On an exactly-once subscription the confirmed forms are used, so `Ok`
/// from `ack` means the broker accepted it.
pub struct PubSubMessage {
    payload: Bytes,
    headers: HeaderMap,
    handler: Handler,
}

impl std::fmt::Debug for PubSubMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PubSubMessage")
            .field("payload_len", &self.payload.len())
            .finish_non_exhaustive()
    }
}

impl PubSubMessage {
    pub(crate) fn new(message: GcpMessage, handler: Handler) -> Self {
        let mut headers = HeaderMap::with_capacity(message.attributes.len() + 2);
        for (name, value) in &message.attributes {
            headers.insert(name.clone(), value.clone());
        }
        if !message.ordering_key.is_empty() {
            headers.insert(PARTITION_KEY_HEADER, message.ordering_key.clone());
        }
        if let Some(attempt) = handler.delivery_attempt() {
            headers.insert(DELIVERY_ATTEMPT_HEADER, attempt.to_string());
        }
        Self {
            payload: message.data,
            headers,
            handler,
        }
    }
}

impl Partitioned for PubSubMessage {
    fn partition_key(&self) -> Option<&[u8]> {
        self.headers.get(PARTITION_KEY_HEADER)
    }
}

impl IncomingMessage for PubSubMessage {
    fn payload(&self) -> &[u8] {
        &self.payload
    }

    fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    async fn ack(self) -> Result<(), AckError> {
        match self.handler {
            // Only the confirmed form guarantees no redelivery on an exactly-once
            // subscription; the plain form is fire-and-forget.
            Handler::ExactlyOnce(handler) => handler
                .confirmed_ack()
                .await
                .map_err(|e| AckError::Broker(Box::new(e))),
            handler => {
                handler.ack();
                Ok(())
            }
        }
    }

    async fn nack(self, requeue: bool) -> Result<(), AckError> {
        if requeue {
            match self.handler {
                Handler::ExactlyOnce(handler) => handler
                    .confirmed_nack()
                    .await
                    .map_err(|e| AckError::Broker(Box::new(e))),
                handler => {
                    handler.nack();
                    Ok(())
                }
            }
        } else {
            // Dropping without redelivery IS an acknowledge in Pub/Sub; the dead-letter
            // policy on the subscription owns poison-message routing.
            self.ack().await
        }
    }

    fn partition_key(&self) -> Option<&[u8]> {
        Partitioned::partition_key(self)
    }
}

/// Builds the Pub/Sub message for an outgoing publish under `ordering_key`, the key the publisher
/// resolved for it.
///
/// The `partition-key` header never travels as an attribute: the name belongs to the message's own
/// ordering key, which a delivery reports under it, so a header copied off one delivery cannot be
/// mistaken downstream for the key of a message that carries none.
pub(crate) fn to_gcp_message(msg: &OutgoingMessage<'_>, ordering_key: Option<&str>) -> GcpMessage {
    let headers = msg.headers();
    let mut attributes: Vec<(String, String)> = Vec::with_capacity(headers.len());
    for (name, value) in headers.iter() {
        if name == PARTITION_KEY_HEADER {
            continue;
        }
        attributes.push((name.to_owned(), String::from_utf8_lossy(value).into_owned()));
    }

    let mut message = GcpMessage::new().set_data(Bytes::copy_from_slice(msg.payload()));
    if !attributes.is_empty() {
        message = message.set_attributes(attributes);
    }
    if let Some(key) = ordering_key {
        message = message.set_ordering_key(key);
    }
    message
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_resolved_key_becomes_the_messages_ordering_key() {
        let mut headers = HeaderMap::new();
        // What a handler forwarding a delivery's headers carries along. It is a report of the
        // delivery's key, not an instruction, so it says nothing about this publish.
        headers.insert(PARTITION_KEY_HEADER, "the-delivery-this-answers");
        headers.insert("x-tenant", "acme");
        let outgoing = OutgoingMessage::new("orders", b"{}".as_slice()).with_headers(headers);

        let message = to_gcp_message(&outgoing, Some("user-42"));
        assert_eq!(message.ordering_key, "user-42");
        assert_eq!(
            message.attributes.get("x-tenant").map(String::as_str),
            Some("acme")
        );
        // The name belongs to the message's own field, so a copied header must not reach the
        // attributes and be read back downstream as this message's key.
        assert!(!message.attributes.contains_key(PARTITION_KEY_HEADER));
    }

    #[test]
    fn plain_messages_carry_no_ordering_key() {
        let outgoing = OutgoingMessage::new("orders", b"{}".as_slice());
        let message = to_gcp_message(&outgoing, None);
        assert!(message.ordering_key.is_empty());
    }
}
