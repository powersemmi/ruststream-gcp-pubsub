//! [`PubSubSubscriber`]: a stream of deliveries backed by a pump task.
//!
//! The client's `MessageStream` exposes an inherent `next()` that is not documented
//! cancel-safe, so the crate owns a pump task per subscription: it drives `next`, converts
//! deliveries, and forwards them into a bounded channel the subscriber polls. Settlement needs
//! no round trip through the pump - the client's `Handler` travels inside each message and is
//! consumed by `ack`/`nack` directly.

use futures::Stream;

use google_cloud_pubsub::subscriber::{MessageStream, ShutdownToken};
use ruststream::Subscriber;
use tokio::sync::mpsc;

use crate::broker::Core;
use crate::error::{PubSubError, box_err};
use crate::message::PubSubMessage;
use crate::subscription::PubSubSubscription;

/// How many converted deliveries may sit between the pump and the consumer. Real prefetch is
/// the client's own flow control (`max_outstanding`); this only decouples the two loops.
const CHANNEL_CAPACITY: usize = 16;

/// A subscription to one Pub/Sub subscription; yields [`PubSubMessage`]s.
///
/// Dropping the subscriber signals the client's shutdown token, which drains the stream and
/// stops the pump task.
pub struct PubSubSubscriber {
    subscription: String,
    rx: mpsc::Receiver<Result<PubSubMessage, PubSubError>>,
    shutdown: ShutdownToken,
}

impl std::fmt::Debug for PubSubSubscriber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PubSubSubscriber")
            .field("subscription", &self.subscription)
            .finish_non_exhaustive()
    }
}

impl PubSubSubscriber {
    /// The full resource name of the subscription this stream consumes from.
    #[must_use]
    pub fn subscription(&self) -> &str {
        &self.subscription
    }

    /// Opens the stream synchronously (the client connects lazily) and spawns the pump.
    pub(crate) fn open(core: &Core, descriptor: &PubSubSubscription) -> Self {
        let name = core.subscription_name(descriptor.subscription());
        let mut builder = core.subscriber.subscribe(name.clone());
        if let Some(messages) = descriptor.max_outstanding_value() {
            builder = builder.set_max_outstanding_messages(messages);
        }
        if let Some(extension) = descriptor.ack_extension_value() {
            builder = builder.set_max_lease_extension(extension);
        }
        let stream = builder.build();
        let shutdown = stream.shutdown_token();

        let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
        tokio::spawn(pump(stream, tx, name.clone()));

        Self {
            subscription: name,
            rx,
            shutdown,
        }
    }
}

impl Drop for PubSubSubscriber {
    fn drop(&mut self) {
        // `shutdown` is async and destructors are sync; the spawned signal drains the stream,
        // which ends the pump task. Best effort by design: if the runtime is already gone, the
        // pump dies with it.
        let token = self.shutdown.clone();
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                token.shutdown().await;
            });
        }
    }
}

impl Subscriber for PubSubSubscriber {
    type Message = PubSubMessage;
    type Error = PubSubError;

    fn stream(&mut self) -> impl Stream<Item = Result<PubSubMessage, PubSubError>> + Send + '_ {
        // Poll the channel in place rather than wrapping it in an owning stream, so `stream`
        // can be called again after the returned stream is dropped (the runtime and the
        // conformance helpers re-enter it per call).
        futures::stream::poll_fn(move |cx| self.rx.poll_recv(cx))
    }
}

async fn pump(
    mut stream: MessageStream,
    out: mpsc::Sender<Result<PubSubMessage, PubSubError>>,
    subscription: String,
) {
    while let Some(item) = stream.next().await {
        match item {
            Ok((message, handler)) => {
                if out
                    .send(Ok(PubSubMessage::new(message, handler)))
                    .await
                    .is_err()
                {
                    // Subscriber dropped; its Drop has already signalled shutdown.
                    break;
                }
            }
            Err(err) => {
                // The client absorbs transient failures internally; an error here is
                // permanent and closes the stream.
                let _ = out
                    .send(Err(PubSubError::Receive {
                        subscription: subscription.clone(),
                        source: box_err(err),
                    }))
                    .await;
                break;
            }
        }
    }
}
