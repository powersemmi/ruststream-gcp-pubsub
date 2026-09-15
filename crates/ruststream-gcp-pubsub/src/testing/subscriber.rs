//! [`PubSubTestSubscriber`] and [`PubSubTestMessage`].

use std::future::{Future, ready};
use std::num::NonZeroUsize;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use futures::Stream;

use ruststream::{
    AckError, BatchSubscriber, BufferedSubscriber, HeaderMap, IncomingMessage, Partitioned,
    Subscriber, testing::Coordinator,
};

use crate::error::{PubSubError, box_err};
use crate::subscription::DeliveryLimits;
use crate::testing::broker::TestState;
use crate::testing::router::{
    DeadLetter, Delivery, DeliveryReceiver, DeliverySender, SubscriptionId,
};
use crate::{DELIVERY_ATTEMPT_HEADER, PARTITION_KEY_HEADER};

/// What the descriptor said about one stand-in subscription: how long a partial batch waits, the
/// dead-letter policy the registration declared, and the limits one delivery runs under.
#[derive(Debug, Clone)]
pub(crate) struct Declared {
    pub(crate) batch_wait: Duration,
    pub(crate) dead_letter: Option<Arc<DeadLetter>>,
    pub(crate) limits: DeliveryLimits,
}

/// Subscriber returned by [`ConnectedPubSubTestBroker`](crate::testing::ConnectedPubSubTestBroker).
///
/// Dropping it unregisters the subscription, so handlers stop receiving as soon as their task
/// finishes.
pub struct PubSubTestSubscriber {
    state: Arc<TestState>,
    id: SubscriptionId,
    // Named for what it is rather than what it yields, as on the real subscriber.
    buffer: BufferedSubscriber<Deliveries>,
}

impl std::fmt::Debug for PubSubTestSubscriber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PubSubTestSubscriber")
            .finish_non_exhaustive()
    }
}

impl PubSubTestSubscriber {
    pub(crate) fn new(
        state: Arc<TestState>,
        id: SubscriptionId,
        rx: DeliveryReceiver,
        requeue: DeliverySender,
        coordinator: Option<Coordinator>,
        declared: Declared,
    ) -> Self {
        let Declared {
            batch_wait,
            dead_letter,
            limits,
        } = declared;
        Self {
            state: Arc::clone(&state),
            id,
            // The descriptor's own deadline, because it is the same knob on the same buffer
            // here as against the product: batching is on the client either way.
            buffer: BufferedSubscriber::new(Deliveries {
                rx,
                requeue,
                coordinator,
                state,
                dead_letter,
                limits,
            })
            .max_wait(batch_wait),
        }
    }
}

impl Drop for PubSubTestSubscriber {
    fn drop(&mut self) {
        self.state.router.unsubscribe(self.id);
    }
}

impl Subscriber for PubSubTestSubscriber {
    type Message = PubSubTestMessage;
    type Error = PubSubError;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        self.buffer.stream()
    }
}

/// The stand-in batches the way the real subscriber does - on the client, off the same
/// one-at-a-time delivery path, closed by the size the registration named or by the descriptor's
/// [`batch_wait`](crate::GooglePubSub::batch_wait) - so a slice handler runs under
/// `TestApp` exactly as it runs against Pub/Sub.
impl BatchSubscriber for PubSubTestSubscriber {
    type Batch = Vec<PubSubTestMessage>;

    fn batches(
        &mut self,
        size: NonZeroUsize,
    ) -> impl Stream<Item = Result<Self::Batch, PubSubError>> + Send + '_ {
        self.buffer.batches(size)
    }
}

/// The routed side of a stand-in subscription: the router's channel, read one delivery at a
/// time. The buffer above it is what turns those into batches.
struct Deliveries {
    rx: DeliveryReceiver,
    requeue: DeliverySender,
    /// A clone of the broker's harness coordinator, threaded into each yielded message so a
    /// requeue re-counts and a consumed delivery decrements. `None` outside a harness run.
    coordinator: Option<Coordinator>,
    /// The transport a spent delivery leaves through, which is the same router every publish
    /// goes to.
    state: Arc<TestState>,
    /// What the registration declared, `None` where it declared nothing.
    dead_letter: Option<Arc<DeadLetter>>,
    /// The same budget the product's subscription holds a delivery under, so a delay the product
    /// refuses is refused here too.
    limits: DeliveryLimits,
}

impl Subscriber for Deliveries {
    type Message = PubSubTestMessage;
    type Error = PubSubError;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        let requeue = self.requeue.clone();
        let coordinator = self.coordinator.clone();
        let state = Arc::clone(&self.state);
        let dead_letter = self.dead_letter.clone();
        let limits = self.limits;
        // Poll the receiver in place rather than wrapping it in an owning stream, so `stream`
        // can be called again after the returned stream is dropped (the runtime and the
        // conformance helpers re-enter it per call).
        futures::stream::poll_fn(move |cx| {
            self.rx.poll_recv(cx).map(|next| {
                next.map(|delivery| {
                    Ok(PubSubTestMessage::new(
                        delivery,
                        requeue.clone(),
                        coordinator.clone(),
                        Arc::clone(&state),
                        dead_letter.clone(),
                        limits,
                    ))
                })
            })
        })
    }
}

/// Message handed to handlers from an [`PubSubTestSubscriber`].
///
/// `ack` consumes the handle; `nack(requeue = true)` re-queues the delivery on the owning
/// subscription's channel so the next handler invocation sees it again; `nack(requeue = false)`
/// drops it, matching the real subscriber's reject path in effect. On the last delivery a
/// declared dead-letter policy allows, either rejection publishes the message to the declared
/// topic instead, which is what the subscription does against Pub/Sub.
pub struct PubSubTestMessage {
    delivery: Option<Delivery>,
    requeue: DeliverySender,
    /// A clone of the broker's harness coordinator. When set, this delivery is counted in
    /// flight and is decremented exactly once when the message is consumed or dropped.
    coordinator: Option<Coordinator>,
    /// The transport a spent delivery is published through.
    state: Arc<TestState>,
    /// The subscription's declared dead-letter policy, `None` where the registration declared
    /// nothing and a rejection is the end of the message.
    dead_letter: Option<Arc<DeadLetter>>,
    limits: DeliveryLimits,
}

impl Drop for PubSubTestMessage {
    /// Counts this delivery consumed exactly once: on ack, nack, or an unsettled drop. A
    /// requeue re-enqueues a fresh delivery first, so the in-flight count stays balanced.
    fn drop(&mut self) {
        if let Some(coordinator) = &self.coordinator {
            coordinator.consumed();
        }
    }
}

impl std::fmt::Debug for PubSubTestMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PubSubTestMessage").finish_non_exhaustive()
    }
}

impl PubSubTestMessage {
    pub(crate) fn new(
        mut delivery: Delivery,
        requeue: DeliverySender,
        coordinator: Option<Coordinator>,
        state: Arc<TestState>,
        dead_letter: Option<Arc<DeadLetter>>,
        limits: DeliveryLimits,
    ) -> Self {
        // Pub/Sub reports the delivery attempt only under a dead-letter policy, and this crate
        // surfaces it as a header, so the stand-in surfaces the same header under the same
        // condition.
        if dead_letter.is_some() {
            delivery
                .headers
                .insert(DELIVERY_ATTEMPT_HEADER, delivery.attempt.to_string());
        }
        Self {
            delivery: Some(delivery),
            requeue,
            coordinator,
            state,
            dead_letter,
            limits,
        }
    }

    /// Whether this is the last delivery the subscription's dead-letter policy allows.
    fn at_delivery_cap(&self) -> bool {
        match (&self.dead_letter, &self.delivery) {
            (Some(policy), Some(delivery)) => delivery.attempt >= policy.max_attempts,
            _ => false,
        }
    }

    /// Publishes a spent delivery to the declared topic, the way the subscription's dead-letter
    /// policy does. The attempt header goes with the delivery, not with the message, so the copy
    /// leaves without it.
    fn dead_letter(&self, mut delivery: Delivery, topic: &str) {
        delivery.headers.remove(DELIVERY_ATTEMPT_HEADER);
        self.state
            .publish(topic, delivery.payload, delivery.headers);
    }
}

impl Partitioned for PubSubTestMessage {
    fn partition_key(&self) -> Option<&[u8]> {
        self.headers().get(PARTITION_KEY_HEADER)
    }
}

impl IncomingMessage for PubSubTestMessage {
    fn payload(&self) -> &[u8] {
        self.delivery
            .as_ref()
            .map(|d| d.payload.as_ref())
            .unwrap_or_default()
    }

    fn headers(&self) -> &HeaderMap {
        static EMPTY: OnceLock<HeaderMap> = OnceLock::new();
        self.delivery
            .as_ref()
            .map_or_else(|| EMPTY.get_or_init(HeaderMap::new), |d| &d.headers)
    }

    fn ack(mut self) -> impl Future<Output = Result<(), AckError>> {
        self.delivery.take();
        ready(Ok(()))
    }

    /// The stand-in answers as the product does, because the crate holds a delayed retry in the
    /// process on both.
    fn supports_nack_after(&self) -> bool {
        true
    }

    /// Holds the delivery for `delay`, then returns it to the subscription.
    ///
    /// Registered with the harness coordinator rather than slept on directly, so a test fires it
    /// with [`TestApp::advance`](ruststream::testing::TestApp::advance) instead of waiting. The
    /// delay a subscription cannot outlast is refused here as it is against the product.
    ///
    /// # Errors
    ///
    /// Returns [`AckError::Broker`] carrying
    /// [`PubSubError::DelayBeyondLease`](crate::PubSubError::DelayBeyondLease) when `delay`
    /// outlives the subscription's maximum lease.
    fn nack_after(mut self, delay: Duration) -> impl Future<Output = Result<(), AckError>> {
        if delay > self.limits.max_lease {
            return ready(Err(AckError::Broker(box_err(
                PubSubError::DelayBeyondLease {
                    requested: delay,
                    lease: self.limits.max_lease,
                },
            ))));
        }
        let at_cap = self.at_delivery_cap();
        let mut delivery = self
            .delivery
            .take()
            .expect("PubSubTestMessage ack/nack invoked twice");
        delivery.headers.remove(DELIVERY_ATTEMPT_HEADER);
        // A delivery whose attempts are spent is carried away rather than held: the wait would
        // only end in the same move, and the product does not hold one either.
        if let (true, Some(policy)) = (at_cap, self.dead_letter.as_deref()) {
            let topic = policy.topic.clone();
            self.dead_letter(delivery, &topic);
            return ready(Ok(()));
        }
        delivery.attempt += 1;
        let requeue = self.requeue.clone();
        let Some(coordinator) = self.coordinator.clone() else {
            tokio::spawn(async move {
                tokio::time::sleep(delay).await;
                // The subscription may be gone by then; a dropped receiver is not an error.
                let _ = requeue.send(delivery);
            });
            return ready(Ok(()));
        };
        let counter = coordinator.clone();
        coordinator.schedule_redelivery(delay, move || {
            if requeue.send(delivery).is_ok() {
                counter.enqueued();
            }
        });
        ready(Ok(()))
    }

    fn nack(mut self, requeue: bool) -> impl Future<Output = Result<(), AckError>> {
        let at_cap = self.at_delivery_cap();
        let mut delivery = self
            .delivery
            .take()
            .expect("PubSubTestMessage ack/nack invoked twice");
        delivery.headers.remove(DELIVERY_ATTEMPT_HEADER);
        match (at_cap, self.dead_letter.as_deref()) {
            // The attempts are spent, so the message leaves the subscription for the declared
            // topic. The publish counts its own enqueue for whoever is subscribed there.
            (true, Some(policy)) => {
                let topic = policy.topic.clone();
                self.dead_letter(delivery, &topic);
            }
            _ if requeue => {
                delivery.attempt += 1;
                let sent = self.requeue.send(delivery);
                // The requeue bypasses fanout, so count the re-enqueue here to balance this
                // message's `Drop` decrement. The redelivered copy is consumed in turn.
                if sent.is_ok()
                    && let Some(coordinator) = &self.coordinator
                {
                    coordinator.enqueued();
                }
            }
            _ => {}
        }
        ready(Ok(()))
    }

    /// The delivery attempt, counted the way Pub/Sub counts it: only under a dead-letter policy,
    /// and starting at one.
    fn redelivery_count(&self) -> Option<u64> {
        let _policy = self.dead_letter.as_ref()?;
        let delivery = self.delivery.as_ref()?;
        u64::try_from(delivery.attempt).ok()
    }

    fn partition_key(&self) -> Option<&[u8]> {
        Partitioned::partition_key(self)
    }
}
