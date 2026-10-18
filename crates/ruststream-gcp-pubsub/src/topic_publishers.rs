//! The client publishers of one connection, one per topic, and the topic a publisher handle keeps
//! at hand.
//!
//! The client batches and orders per client publisher, so a topic has exactly one on a connection:
//! two would let messages under one ordering key race each other. [`TopicPublishers`] owns them,
//! keyed so that every spelling of a topic finds the same one, and is what `shutdown` flushes.
//!
//! A publish reads the map through a snapshot and hands the message to the client publisher in
//! place: the client's `publish` only queues the message on the publisher's channel and returns a
//! future that owns its receiver, so nothing is cloned and no lock is taken. A handle that
//! publishes to one topic skips even the snapshot: its [`FirstTopic`] keeps that topic's client
//! publisher beside the handle.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};

use arc_swap::ArcSwap;
use google_cloud_pubsub::client::Publisher as GcpPublisher;

/// The key a topic's client publisher is kept under: the short id for a topic of `project`,
/// whether it is named by its id or by its full resource name, and the full name for a topic of
/// another project.
///
/// The key borrows from the name it is given, so finding a topic's client publisher allocates
/// nothing. A topic id holds no `/`, so the id after `projects/<project>/topics/` is the topic's
/// whole id.
pub(crate) fn topic_key<'a>(project: &str, topic: &'a str) -> &'a str {
    topic
        .strip_prefix("projects/")
        .and_then(|rest| rest.strip_prefix(project))
        .and_then(|rest| rest.strip_prefix("/topics/"))
        .unwrap_or(topic)
}

/// The client publishers of one connection, by [`topic_key`].
///
/// Readers load the current map from an [`ArcSwap`]: two atomic operations on slots the reading
/// thread owns, so publishes from many threads never write a shared cache line. A topic's first
/// publish copies the map with the new client publisher in it and swaps the copy in, under a
/// mutex that only those first publishes take. A connection pays that copy once per topic.
#[derive(Debug, Default)]
pub(crate) struct TopicPublishers {
    by_topic: ArcSwap<HashMap<Box<str>, GcpPublisher>>,
    /// Held while a first publish adds its topic, so racing first publishes to one topic still
    /// share one client publisher.
    adding: Mutex<()>,
}

impl TopicPublishers {
    /// Hands `use_it` the client publisher kept under `key`, made with `build` on the first use
    /// of the topic.
    ///
    /// `build` runs at most once per key, so racing first publishes to one topic still share one
    /// client publisher.
    pub(crate) fn with<Out>(
        &self,
        key: &str,
        build: impl FnOnce() -> GcpPublisher,
        use_it: impl FnOnce(&GcpPublisher) -> Out,
    ) -> Out {
        if let Some(publisher) = self.by_topic.load().get(key) {
            return use_it(publisher);
        }
        // The lock guards no data, only the order of the adds, so a panic under it leaves
        // nothing half-made.
        let _adding = self.adding.lock().unwrap_or_else(PoisonError::into_inner);
        let current = self.by_topic.load_full();
        if let Some(publisher) = current.get(key) {
            return use_it(publisher);
        }
        let publisher = build();
        let mut next = HashMap::clone(&current);
        next.insert(Box::from(key), publisher.clone());
        self.by_topic.store(Arc::new(next));
        use_it(&publisher)
    }

    /// Every client publisher made so far, which is what a shutdown flushes.
    pub(crate) fn all(&self) -> Vec<GcpPublisher> {
        self.by_topic.load().values().cloned().collect()
    }

    /// How many client publishers the connection holds.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.by_topic.load().len()
    }
}

/// The first topic a publisher handle published to, as the handle wrote its name, with that
/// topic's client publisher.
///
/// A handle mostly publishes to one topic, so this answers its every later publish with a string
/// comparison. A publish to any other topic goes to the connection's [`TopicPublishers`].
#[derive(Debug, Default)]
pub(crate) struct FirstTopic(OnceLock<(Box<str>, GcpPublisher)>);

impl FirstTopic {
    /// The client publisher of `topic`, when `topic` is the first topic this handle published to.
    pub(crate) fn get(&self, topic: &str) -> Option<&GcpPublisher> {
        self.0
            .get()
            .filter(|(first, _)| **first == *topic)
            .map(|(_, publisher)| publisher)
    }

    /// Keeps `publisher` as the client publisher of `topic`, unless the handle already keeps one.
    pub(crate) fn remember(&self, topic: &str, publisher: &GcpPublisher) {
        if self.0.get().is_none() {
            // A racing publish of another clone may have filled it first; either topic serves.
            let _ = self.0.set((Box::from(topic), publisher.clone()));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A topic of the connection's project has one key whichever way it is named, so a short id
    /// and the full resource name share one client publisher.
    #[test]
    fn a_short_id_and_the_full_name_of_one_topic_share_a_key() {
        assert_eq!(topic_key("shop", "orders"), "orders");
        assert_eq!(topic_key("shop", "projects/shop/topics/orders"), "orders");
    }

    /// A topic of another project keeps its full name, so it never meets a topic of the same id
    /// in the connection's own project.
    #[test]
    fn a_topic_of_another_project_keeps_its_full_name() {
        assert_eq!(
            topic_key("shop", "projects/billing/topics/orders"),
            "projects/billing/topics/orders"
        );
        assert_eq!(
            topic_key("shop", "projects/shopping/topics/orders"),
            "projects/shopping/topics/orders"
        );
    }
}
