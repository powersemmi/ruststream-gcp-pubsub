//! The consuming half of the in-process transport: the stream of one consumer, and how a delivery
//! taken from it settles.

use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use super::project::{ConsumerId, HandedReceiver, Pending, Project};
use crate::error::PubSubError;
use crate::message::PubSubMessage;
use crate::subscription::DeliveryLimits;

/// One consumer of a subscription on the in-process transport, one delivery at a time: what the
/// subscriber batches over, as it batches over a streaming pull.
pub(crate) struct Consumer {
    project: Arc<Project>,
    /// The subscription's full resource name.
    subscription: Arc<str>,
    id: ConsumerId,
    receiver: HandedReceiver,
    limits: DeliveryLimits,
}

impl Consumer {
    pub(super) fn new(
        project: Arc<Project>,
        subscription: Arc<str>,
        id: ConsumerId,
        receiver: HandedReceiver,
        limits: DeliveryLimits,
    ) -> Self {
        Self {
            project,
            subscription,
            id,
            receiver,
            limits,
        }
    }

    /// The next delivery, read off the message the subscription handed over with the mapping a
    /// delivery off the service goes through.
    pub(crate) fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<PubSubMessage, PubSubError>>> {
        self.receiver.poll_recv(cx).map(|handed| {
            handed.map(|handed| {
                let reported = self
                    .project
                    .reported_attempt(&self.subscription, handed.pending.attempt);
                let message = handed.pending.message.clone();
                Ok(PubSubMessage::in_process(
                    message,
                    Settlement {
                        project: Arc::clone(&self.project),
                        subscription: Arc::clone(&self.subscription),
                        pending: Some(handed.pending),
                        reported,
                        counted: handed.counted,
                    },
                    self.limits,
                ))
            })
        })
    }
}

impl Drop for Consumer {
    fn drop(&mut self) {
        self.project
            .cancel(&self.subscription, self.id, &mut self.receiver);
    }
}

impl std::fmt::Debug for Consumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Consumer")
            .field("subscription", &self.subscription)
            .finish_non_exhaustive()
    }
}

/// How an in-process delivery settles: against the subscription it came from, with what the
/// service does with a rejected message.
///
/// A delivery dropped without a settlement is rejected, as the client's handler rejects one on
/// drop, and the delivery is released to the harness once, when it goes.
pub(crate) struct Settlement {
    project: Arc<Project>,
    subscription: Arc<str>,
    /// The message and its attempt, until the delivery settles.
    pending: Option<Pending>,
    /// The delivery attempt the subscription reports, present only under a dead-letter policy.
    reported: Option<i32>,
    /// Whether the harness counted this delivery in flight when it was handed over.
    counted: bool,
}

impl Settlement {
    pub(crate) fn delivery_attempt(&self) -> Option<i32> {
        self.reported
    }

    /// Acknowledges the delivery: the subscription is done with the message.
    pub(crate) fn ack(mut self) {
        self.pending = None;
    }

    /// Rejects the delivery now: the service redelivers it, or dead-letters it where its attempts
    /// are spent.
    pub(crate) fn reject(mut self) {
        if let Some(pending) = self.pending.take() {
            self.project.reject(&self.subscription, pending);
        }
    }

    /// Holds the delivery for `delay`, then rejects it, as the crate holds a delivery of the
    /// service. Under the harness the timer belongs to the coordinator, so the test's clock is the
    /// one the delay passes on; the delivery is released to the harness now, and the rejection
    /// counts the redelivery when it fires.
    pub(crate) fn reject_after(mut self, delay: Duration) {
        let Some(pending) = self.pending.take() else {
            return;
        };
        let project = Arc::clone(&self.project);
        let subscription = Arc::clone(&self.subscription);
        let reject = move || project.reject(&subscription, pending);
        match self.project.coordinator() {
            Some(coordinator) => coordinator.schedule_redelivery(delay, reject),
            // On the runtime the broker connected on, not the settling caller's: a handler on a
            // dedicated thread settles from a runtime that may stop before the delay is out.
            None => {
                self.project.runtime().spawn(async move {
                    tokio::time::sleep(delay).await;
                    reject();
                });
            }
        }
    }
}

impl Drop for Settlement {
    fn drop(&mut self) {
        // Rejected before it is released, so the redelivery is counted before this one goes and
        // the harness never reads the reaction as over in between.
        if let Some(pending) = self.pending.take() {
            self.project.reject(&self.subscription, pending);
        }
        if self.counted
            && let Some(coordinator) = self.project.coordinator()
        {
            coordinator.consumed();
        }
    }
}

impl std::fmt::Debug for Settlement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Settlement")
            .field("subscription", &self.subscription)
            .finish_non_exhaustive()
    }
}
