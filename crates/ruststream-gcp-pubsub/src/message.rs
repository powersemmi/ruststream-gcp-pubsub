//! [`PubSubMessage`] and the mapping between `RustStream` headers and Pub/Sub attributes.
//!
//! Message attributes carry headers directly - no envelope format is invented - and the partition
//! key rides the message's ordering key in both directions.

use bytes::Bytes;
use google_cloud_pubsub::model::Message as GcpMessage;
use google_cloud_pubsub::subscriber::handler::Handler;
use ruststream::{AckError, HeaderMap, IncomingMessage, OutgoingMessage, Partitioned};

/// Header carrying the partition key, mapped onto the message's ordering key.
///
/// Mirrors the in-memory broker's convention, so services can switch brokers without changing
/// their headers. It works in both directions: a delivery reports the key it arrived under, and a
/// header on an outgoing message orders that message where nothing named the key natively
/// ([`PubSubPublishOptions`](crate::PubSubPublishOptions)).
pub const PARTITION_KEY_HEADER: &str = "partition-key";

/// Header exposing the delivery attempt count on received messages, present when the
/// subscription has a dead-letter policy.
pub const DELIVERY_ATTEMPT_HEADER: &str = "pubsub-delivery-attempt";

/// A message delivered by a [`PubSubSubscriber`](crate::PubSubSubscriber).
///
/// `ack` and `nack(requeue = true)` are native. `nack(requeue = false)` acknowledges: Pub/Sub
/// has no "drop without redelivery" beyond acknowledgement - dead-lettering is the
/// subscription's dead-letter policy, driven by repeated nacks and expired deadlines, not a
/// per-message verb. The exception is the last delivery that policy allows, where a rejection is
/// exactly how the message reaches the dead-letter topic; a delivery there is rejected rather
/// than acknowledged. On an exactly-once subscription the confirmed forms are used, so `Ok`
/// from `ack` means the broker accepted it.
pub struct PubSubMessage {
    payload: Bytes,
    headers: HeaderMap,
    handler: Handler,
    /// How many deliveries one message gets under the subscription's dead-letter policy, as the
    /// registration declared it. `None` where none was declared.
    max_delivery_attempts: Option<i32>,
}

impl std::fmt::Debug for PubSubMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PubSubMessage")
            .field("payload_len", &self.payload.len())
            .finish_non_exhaustive()
    }
}

impl PubSubMessage {
    pub(crate) fn new(
        message: GcpMessage,
        handler: Handler,
        max_delivery_attempts: Option<i32>,
    ) -> Self {
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
            max_delivery_attempts,
        }
    }

    /// Whether this is the last delivery the subscription's dead-letter policy allows.
    ///
    /// A rejection here is what moves the message to the dead-letter topic, so it is the one
    /// place a settlement meaning "do not redeliver" has to reach the service rather than
    /// acknowledge.
    fn at_delivery_cap(&self) -> bool {
        match (self.max_delivery_attempts, self.handler.delivery_attempt()) {
            (Some(cap), Some(attempt)) => attempt >= cap,
            _ => false,
        }
    }

    /// Rejects the delivery, in the form the subscription's delivery guarantee asks for.
    async fn reject(self) -> Result<(), AckError> {
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

    /// The delivery attempt the service reports, counting this delivery.
    ///
    /// Pub/Sub sends it only where the subscription has a dead-letter policy, so a subscription
    /// without one counts nothing and the framework reads its own header instead.
    fn redelivery_count(&self) -> Option<u64> {
        self.handler
            .delivery_attempt()
            .and_then(|attempt| u64::try_from(attempt).ok())
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
        // At the cap the rejection is what moves the message: the service publishes it to the
        // dead-letter topic instead of redelivering it, so acknowledging here would drop a
        // message the registration asked to keep.
        if requeue || self.at_delivery_cap() {
            return self.reject().await;
        }
        // Dropping without redelivery IS an acknowledge in Pub/Sub; the dead-letter policy on
        // the subscription owns poison-message routing.
        self.ack().await
    }

    fn partition_key(&self) -> Option<&[u8]> {
        Partitioned::partition_key(self)
    }
}

/// Builds the Pub/Sub message for an outgoing publish under `ordering_key`, the key the publisher
/// resolved for it.
///
/// The `partition-key` header never travels as an attribute: it is the portable spelling of the
/// ordering key, the publisher has already read it, and a delivery reports the key back under that
/// same name.
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
        headers.insert(PARTITION_KEY_HEADER, "user-42");
        headers.insert("x-tenant", "acme");
        let outgoing = OutgoingMessage::new("orders", b"{}".as_slice()).with_headers(headers);

        let message = to_gcp_message(&outgoing, Some("user-42"));
        assert_eq!(message.ordering_key, "user-42");
        assert_eq!(
            message.attributes.get("x-tenant").map(String::as_str),
            Some("acme")
        );
        // The key is a field of the message, so it must not be duplicated as an attribute.
        assert!(!message.attributes.contains_key(PARTITION_KEY_HEADER));
    }

    #[test]
    fn plain_messages_carry_no_ordering_key() {
        let outgoing = OutgoingMessage::new("orders", b"{}".as_slice());
        let message = to_gcp_message(&outgoing, None);
        assert!(message.ordering_key.is_empty());
    }
}
