//! [`PubSubMessage`] and the mapping between `RustStream` headers and Pub/Sub attributes.
//!
//! Message attributes carry headers directly - no envelope format is invented - and the
//! partition key rides the message's ordering key in both directions.

use bytes::Bytes;
use google_cloud_pubsub::model::Message as GcpMessage;
use google_cloud_pubsub::subscriber::handler::Handler;
use ruststream::{AckError, Headers, IncomingMessage, OutgoingMessage, Partitioned};

/// Header carrying the partition key, mapped onto the message's ordering key.
///
/// Mirrors the in-memory broker's convention, so services can switch brokers without changing
/// their headers.
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
    headers: Headers,
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
        let mut headers = Headers::with_capacity(message.attributes.len() + 2);
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

    fn headers(&self) -> &Headers {
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

/// Builds the Pub/Sub message for an outgoing publish. Returns the message and its ordering
/// key (empty when unordered), which the publisher needs for the resume-after-error path.
pub(crate) fn to_gcp_message(msg: &OutgoingMessage<'_>) -> (GcpMessage, String) {
    let headers = msg.headers();
    let mut ordering_key = String::new();
    let mut attributes: Vec<(String, String)> = Vec::with_capacity(headers.len());
    for (name, value) in headers.iter() {
        let text = String::from_utf8_lossy(value).into_owned();
        if name == PARTITION_KEY_HEADER {
            ordering_key = text;
        } else {
            attributes.push((name.to_owned(), text));
        }
    }

    let mut message = GcpMessage::new().set_data(Bytes::copy_from_slice(msg.payload()));
    if !attributes.is_empty() {
        message = message.set_attributes(attributes);
    }
    if !ordering_key.is_empty() {
        message = message.set_ordering_key(ordering_key.clone());
    }
    (message, ordering_key)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_key_header_becomes_the_ordering_key() {
        let mut headers = Headers::new();
        headers.insert(PARTITION_KEY_HEADER, "user-42");
        headers.insert("x-tenant", "acme");
        let outgoing = OutgoingMessage::new("orders", b"{}".as_slice()).with_headers(headers);

        let (message, key) = to_gcp_message(&outgoing);
        assert_eq!(key, "user-42");
        assert_eq!(message.ordering_key, "user-42");
        assert_eq!(
            message.attributes.get("x-tenant").map(String::as_str),
            Some("acme")
        );
        assert!(!message.attributes.contains_key(PARTITION_KEY_HEADER));
    }

    #[test]
    fn plain_messages_carry_no_ordering_key() {
        let outgoing = OutgoingMessage::new("orders", b"{}".as_slice());
        let (message, key) = to_gcp_message(&outgoing);
        assert!(key.is_empty());
        assert!(message.ordering_key.is_empty());
    }
}
