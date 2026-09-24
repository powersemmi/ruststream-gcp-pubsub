//! The in-process transport's state: one project's subscriptions, the topic each is attached to,
//! the messages each holds for its consumers, and the publish log.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};

use google_cloud_pubsub::model::Message as GcpMessage;
use ruststream::RawMessage;
use ruststream::testing::Coordinator;
use tokio::sync::mpsc;

use super::check_resource_name;
use super::limits;
use super::settle::Consumer;
use crate::broker::{subscription_path, topic_path};
use crate::error::PubSubError;
use crate::message::PubSubMessage;
use crate::subscription::{DeclaredRetries, GooglePubSub};

/// One message on its way to a consumer of one subscription, with the delivery attempt the
/// subscription is on for it, counting from one.
#[derive(Debug, Clone)]
pub(crate) struct Pending {
    pub(crate) message: GcpMessage,
    pub(crate) attempt: i32,
}

/// A message handed to a consumer, and whether the harness counted it in flight when it was.
#[derive(Debug)]
pub(crate) struct Handed {
    pub(crate) pending: Pending,
    pub(crate) counted: bool,
}

pub(crate) type HandedSender = mpsc::UnboundedSender<Handed>;
pub(crate) type HandedReceiver = mpsc::UnboundedReceiver<Handed>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct ConsumerId(u64);

/// A subscription's dead-letter policy, as the service holds it.
#[derive(Debug, Clone)]
struct DeadLetter {
    /// The full name of the topic a spent message is published to.
    topic: String,
    /// How many deliveries one message gets, counting the first.
    max_attempts: i32,
}

/// One subscription resource: the topic it is attached to, its policy, and what it holds.
#[derive(Debug)]
struct Subscription {
    topic: String,
    dead_letter: Option<DeadLetter>,
    /// What the subscription holds while no consumer is open, oldest first.
    backlog: VecDeque<Pending>,
    // Ordered by id, so the rotation below is reproducible: a test that opens two consumers of
    // one subscription always sees the same one served first.
    consumers: BTreeMap<ConsumerId, HandedSender>,
    /// How many messages the subscription has handed out, which makes the choice of the next
    /// consumer a rotation.
    turn: usize,
}

impl Subscription {
    fn attached_to(topic: String) -> Self {
        Self {
            topic,
            dead_letter: None,
            backlog: VecDeque::new(),
            consumers: BTreeMap::new(),
            turn: 0,
        }
    }

    /// Hands `pending` to the next consumer in rotation, or keeps it until one opens.
    fn hand(&mut self, pending: Pending, coordinator: Option<&Coordinator>) {
        if self.consumers.is_empty() {
            self.backlog.push_back(pending);
            return;
        }
        let index = self.turn % self.consumers.len();
        self.turn = self.turn.wrapping_add(1);
        let Some(sender) = self.consumers.values().nth(index) else {
            self.backlog.push_back(pending);
            return;
        };
        // Counted before the send, so the consumer cannot release it before it was counted.
        if let Some(coordinator) = coordinator {
            coordinator.enqueued();
        }
        let handed = Handed {
            pending,
            counted: coordinator.is_some(),
        };
        if let Err(mpsc::error::SendError(handed)) = sender.send(handed) {
            if let Some(coordinator) = coordinator {
                coordinator.consumed();
            }
            self.backlog.push_back(handed.pending);
        }
    }
}

#[derive(Debug, Default)]
struct State {
    /// By full resource name.
    subscriptions: HashMap<String, Subscription>,
    /// What was published to each topic, by the topic's full resource name.
    log: HashMap<String, Vec<RawMessage>>,
}

/// The transport behind one in-process connection: one project of the service, shared by the
/// connected broker and every subscriber, publisher and delivery taken from it.
#[derive(Debug)]
pub(crate) struct Project {
    /// The project the production broker was configured with, which resolves every short name.
    id: String,
    state: Mutex<State>,
    /// Mirrors a closed connection: handles that outlive the shutdown report an error rather than
    /// route into a dead transport.
    closed: AtomicBool,
    coordinator: OnceLock<Coordinator>,
    /// What the bare-name registrations of this connection declared, as the connected broker
    /// keeps it over the service.
    declared_retries: DeclaredRetries,
    next_consumer: AtomicU64,
}

impl Project {
    pub(crate) fn new(id: String) -> Arc<Self> {
        Arc::new(Self {
            id,
            state: Mutex::new(State::default()),
            closed: AtomicBool::new(false),
            coordinator: OnceLock::new(),
            declared_retries: DeclaredRetries::default(),
            next_consumer: AtomicU64::new(0),
        })
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Installs the harness coordinator. A second install is ignored: the harness contract asks
    /// for idempotency.
    pub(crate) fn install(&self, coordinator: Coordinator) {
        let _ = self.coordinator.set(coordinator);
    }

    pub(crate) fn coordinator(&self) -> Option<&Coordinator> {
        self.coordinator.get()
    }

    pub(crate) fn declared_retries(&self) -> &DeclaredRetries {
        &self.declared_retries
    }

    /// `Ok` while the connection is live, [`PubSubError::NotConnected`] once it has shut down:
    /// the variant a publisher of the service reports once its connection is closed.
    pub(crate) fn ensure_open(&self) -> Result<(), PubSubError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(PubSubError::NotConnected);
        }
        Ok(())
    }

    /// Closes the connection. The resources stay, as they do on the service.
    pub(crate) fn close(&self) {
        self.closed.store(true, Ordering::Release);
    }

    pub(crate) fn topic_path(&self, topic: &str) -> String {
        topic_path(&self.id, topic)
    }

    pub(crate) fn subscription_path(&self, subscription: &str) -> String {
        subscription_path(&self.id, subscription)
    }

    /// The topic the subscription `subscription` is attached to, once it exists.
    pub(crate) fn attached_topic(&self, subscription: &str) -> Option<String> {
        self.state()
            .subscriptions
            .get(&self.subscription_path(subscription))
            .map(|subscription| subscription.topic.clone())
    }

    /// Opens a consumer on the subscription `descriptor` names, creating the subscription where it
    /// does not exist yet and writing the dead-letter policy the registration declared.
    ///
    /// A subscription the descriptor creates is attached to the topic it names. One it only names
    /// is infrastructure the service expects to find, and the transport takes it to be attached
    /// to the topic of its own name.
    pub(crate) fn open(
        self: &Arc<Self>,
        descriptor: &GooglePubSub,
    ) -> Result<Consumer, PubSubError> {
        let name = descriptor.subscription();
        let path = self.subscription_path(name);
        let topic = if let Some(topic) = descriptor.create_topic_ref() {
            check_resource_name("topics", topic)
                .map_err(|reason| admin(self.topic_path(topic), reason))?;
            check_resource_name("subscriptions", name)
                .map_err(|reason| admin(path.clone(), reason))?;
            self.topic_path(topic)
        } else {
            check_resource_name("subscriptions", name).map_err(|reason| PubSubError::Receive {
                subscription: path.clone(),
                source: reason.into(),
            })?;
            path.replacen("/subscriptions/", "/topics/", 1)
        };
        let dead_letter = match descriptor.dead_letter_policy() {
            Some((dead_letter, max_attempts)) => {
                check_resource_name("topics", dead_letter)
                    .map_err(|reason| admin(self.topic_path(dead_letter), reason))?;
                Some(DeadLetter {
                    topic: self.topic_path(dead_letter),
                    max_attempts,
                })
            }
            None => None,
        };

        let id = ConsumerId(self.next_consumer.fetch_add(1, Ordering::Relaxed));
        let (sender, receiver) = mpsc::unbounded_channel();
        let coordinator = self.coordinator();
        let mut state = self.state();
        let subscription = state
            .subscriptions
            .entry(path.clone())
            .or_insert_with(|| Subscription::attached_to(topic));
        // A declaration reaches a subscription that exists as an update, and the last one written
        // is the policy the subscription carries; a registration that declares nothing leaves it.
        if dead_letter.is_some() {
            subscription.dead_letter = dead_letter;
        }
        subscription.consumers.insert(id, sender);
        let held: Vec<Pending> = subscription.backlog.drain(..).collect();
        for pending in held {
            subscription.hand(pending, coordinator);
        }
        drop(state);

        Ok(Consumer::new(
            Arc::clone(self),
            Arc::from(path),
            id,
            receiver,
            descriptor.limits(),
        ))
    }

    /// Closes the consumer `id` of `subscription`: what it held and had not handed out goes back
    /// to the subscription, as a streaming pull that closes rejects what it holds.
    pub(crate) fn cancel(&self, subscription: &str, id: ConsumerId, receiver: &mut HandedReceiver) {
        let mut state = self.state();
        if let Some(subscription) = state.subscriptions.get_mut(subscription) {
            subscription.consumers.remove(&id);
        }
        drop(state);
        while let Ok(handed) = receiver.try_recv() {
            self.reject(subscription, handed.pending);
            if handed.counted
                && let Some(coordinator) = self.coordinator()
            {
                coordinator.consumed();
            }
        }
    }

    /// A publish from a client of this connection: refused once the connection is closed, and
    /// otherwise routed as the service routes it.
    ///
    /// # Errors
    ///
    /// Returns [`PubSubError::NotConnected`] after shutdown, and [`PubSubError::Publish`] for a
    /// topic name or a message the service refuses.
    pub(crate) fn publish(&self, topic: &str, message: &GcpMessage) -> Result<(), PubSubError> {
        self.ensure_open()?;
        check_resource_name("topics", topic)
            .and_then(|()| check_message(message))
            .map_err(|reason| PubSubError::Publish {
                topic: self.topic_path(topic),
                source: reason.into(),
            })?;
        self.route(topic, message);
        Ok(())
    }

    /// Records `message` on `topic` and hands a copy to every subscription attached to it. A
    /// topic nothing is attached to keeps nothing, as on the service.
    fn route(&self, topic: &str, message: &GcpMessage) {
        let path = self.topic_path(topic);
        let coordinator = self.coordinator();
        let mut state = self.state();
        let delivered = PubSubMessage::published(topic, message.clone());
        state.log.entry(path.clone()).or_default().push(delivered);
        for subscription in state
            .subscriptions
            .values_mut()
            .filter(|subscription| subscription.topic == path)
        {
            subscription.hand(
                Pending {
                    message: message.clone(),
                    attempt: 1,
                },
                coordinator,
            );
        }
    }

    /// Every message published to `topic`, in publish order.
    pub(crate) fn published(&self, topic: &str) -> Vec<RawMessage> {
        self.state()
            .log
            .get(&self.topic_path(topic))
            .cloned()
            .unwrap_or_default()
    }

    /// The delivery attempt the service reports for `attempt` on `subscription`: the count, where
    /// the subscription has a dead-letter policy, and nothing otherwise.
    pub(crate) fn reported_attempt(&self, subscription: &str, attempt: i32) -> Option<i32> {
        self.state()
            .subscriptions
            .get(subscription)
            .and_then(|subscription| subscription.dead_letter.as_ref())
            .map(|_| attempt)
    }

    /// The service's answer to a rejected delivery: a message that has had every delivery its
    /// subscription's dead-letter policy allows is published to the dead-letter topic, and any
    /// other comes back to the subscription on its next attempt.
    pub(crate) fn reject(&self, subscription: &str, pending: Pending) {
        let coordinator = self.coordinator();
        let mut state = self.state();
        let Some(held) = state.subscriptions.get_mut(subscription) else {
            return;
        };
        if let Some(policy) = held
            .dead_letter
            .as_ref()
            .filter(|policy| pending.attempt >= policy.max_attempts)
        {
            let topic = policy.topic.clone();
            drop(state);
            self.route(&topic, &pending.message);
            return;
        }
        held.hand(
            Pending {
                message: pending.message,
                attempt: pending.attempt.saturating_add(1),
            },
            coordinator,
        );
    }
}

/// A refusal of the admin API, naming the resource it was about.
fn admin(name: String, reason: String) -> PubSubError {
    PubSubError::Admin {
        name,
        source: reason.into(),
    }
}

/// Checks a message against what the service accepts at publish time.
fn check_message(message: &GcpMessage) -> Result<(), String> {
    if message.data.is_empty() && message.attributes.is_empty() {
        return Err("a message must carry data or at least one attribute".to_owned());
    }
    if message.attributes.len() > limits::ATTRIBUTES {
        return Err(format!(
            "a message carries at most {} attributes, this one carries {}",
            limits::ATTRIBUTES,
            message.attributes.len()
        ));
    }
    let mut size = message.data.len() + message.ordering_key.len();
    for (key, value) in &message.attributes {
        if key.is_empty() || key.len() > limits::ATTRIBUTE_KEY {
            return Err(format!(
                "attribute key {key:?} must be 1 to {} bytes",
                limits::ATTRIBUTE_KEY
            ));
        }
        if key.starts_with("goog") {
            return Err(format!(
                "attribute key {key:?} starts with the reserved \"goog\""
            ));
        }
        if value.len() > limits::ATTRIBUTE_VALUE {
            return Err(format!(
                "attribute {key:?} is {} bytes, past the {} a value may carry",
                value.len(),
                limits::ATTRIBUTE_VALUE
            ));
        }
        size += key.len() + value.len();
    }
    if message.ordering_key.len() > limits::ORDERING_KEY {
        return Err(format!(
            "the ordering key is {} bytes, past the {} it may carry",
            message.ordering_key.len(),
            limits::ORDERING_KEY
        ));
    }
    if size > limits::MESSAGE {
        return Err(format!(
            "the message is {size} bytes, past the {} the service accepts",
            limits::MESSAGE
        ));
    }
    Ok(())
}
