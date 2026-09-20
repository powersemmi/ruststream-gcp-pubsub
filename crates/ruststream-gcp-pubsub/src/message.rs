//! [`PubSubMessage`] and the mapping between `RustStream` headers and Pub/Sub attributes.
//!
//! Message attributes carry headers directly - no envelope format is invented - and the partition
//! key rides the message's ordering key in both directions.

use std::future::{Future, ready};
use std::time::Duration;

use bytes::Bytes;
use google_cloud_pubsub::model::Message as GcpMessage;
use google_cloud_pubsub::subscriber::handler::Handler;
use ruststream::{AckError, HeaderMap, IncomingMessage, OutgoingMessage, Partitioned, Str};
use tokio::time::sleep;

use crate::error::{PubSubError, box_err};
use crate::subscription::DeliveryLimits;

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
    limits: DeliveryLimits,
}

impl std::fmt::Debug for PubSubMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PubSubMessage")
            .field("payload_len", &self.payload.len())
            .finish_non_exhaustive()
    }
}

impl PubSubMessage {
    pub(crate) fn new(message: GcpMessage, handler: Handler, limits: DeliveryLimits) -> Self {
        let mut headers = HeaderMap::with_capacity(message.attributes.len() + 2);
        // The attributes are moved, not copied: a header key and a header value are both shared
        // buffers, and a `String` becomes one without touching its bytes.
        for (name, value) in message.attributes {
            headers.insert(name, value);
        }
        if !message.ordering_key.is_empty() {
            headers.insert(Str::from_static(PARTITION_KEY_HEADER), message.ordering_key);
        }
        if let Some(attempt) = handler.delivery_attempt() {
            headers.insert(
                Str::from_static(DELIVERY_ATTEMPT_HEADER),
                attempt.to_string(),
            );
        }
        Self {
            payload: message.data,
            headers,
            handler,
            limits,
        }
    }

    /// Whether this is the last delivery the subscription's dead-letter policy allows.
    ///
    /// A rejection here is what moves the message to the dead-letter topic, so it is the one
    /// place a settlement meaning "do not redeliver" has to reach the service rather than
    /// acknowledge.
    fn at_delivery_cap(&self) -> bool {
        match (
            self.limits.max_delivery_attempts,
            self.handler.delivery_attempt(),
        ) {
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
    /// without one reports no count. It is the only count a delivery of this crate carries:
    /// nothing here publishes a retry copy, so the framework's own header never appears beside
    /// it, and the cap the count would be read against is the subscription's to apply.
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

    /// Pub/Sub has no delayed nack of its own, so this crate holds the delivery instead. The
    /// client keeps extending the ack deadline of a delivery nothing has settled, which is what
    /// makes holding one a real delay rather than a lost message.
    fn supports_nack_after(&self) -> bool {
        true
    }

    /// Holds the delivery for `delay`, then rejects it so the subscription redelivers.
    ///
    /// The delivery stays leased while it is held: the client extends its ack deadline in the
    /// background for as long as nothing has settled it, up to
    /// [`GooglePubSub::max_lease`](crate::GooglePubSub::max_lease). The wait runs on a task of its
    /// own, so the subscription keeps dispatching; the delivery counts against the subscription's
    /// outstanding messages until it comes back, which is what it does against the service too.
    ///
    /// If the process dies while a delivery is held, the lease stops being extended and the
    /// subscription redelivers on its own once it expires. The delay is lost there, not the
    /// message.
    ///
    /// # Errors
    ///
    /// Returns [`AckError::Broker`] carrying [`PubSubError::DelayBeyondLease`] when `delay`
    /// outlives the subscription's maximum lease, because the delivery would come back before it
    /// elapsed. The check is at the call because a handler names the delay while it runs.
    fn nack_after(self, delay: Duration) -> impl Future<Output = Result<(), AckError>> + Send {
        if delay > self.limits.max_lease {
            return ready(Err(AckError::Broker(box_err(
                PubSubError::DelayBeyondLease {
                    requested: delay,
                    lease: self.limits.max_lease,
                },
            ))));
        }
        // The handle travels into the task, so the client goes on extending the lease for the
        // whole wait. A task that never gets to finish drops the handle, which rejects the
        // delivery at once rather than stranding it.
        tokio::spawn(async move {
            sleep(delay).await;
            let _ = self.reject().await;
        });
        ready(Ok(()))
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
