//! The broker's in-process mode, behind the `testing` feature: the transport a connected broker
//! carries when the test harness connects it through `InProcess::connect_in_process` rather than
//! through `connect`.
//!
//! The connected broker, its subscriber and its delivery type each carry this transport as a
//! variant of their own, and the publisher reaches it through the connection cell it already
//! shares, so a service's routes, descriptors and publish policies run against it unchanged. It
//! has no configuration of its own: it reads the production broker's project and every descriptor
//! the service mounts, and it frames a publish with the same conversion a publish to the service
//! goes through. It never succeeds where the service fails. A publish, a subscription or a
//! declaration the service refuses (a resource name it does not accept, a message with nothing in
//! it, an attribute or an ordering key past its limits, a handle outliving its connection) is
//! refused here with the same error variant.
//!
//! What it models: topics and subscriptions as separate resources, a subscription attached to one
//! topic and holding every message published to that topic after it was created, whether or not a
//! consumer is open; competing consumers on one subscription; acknowledgement, rejection and
//! redelivery, a dropped delivery counting as a rejection; the delivery attempt count and the
//! dead-letter policy a registration declares; and a delayed retry held for its delay before the
//! rejection. What belongs to the service and is left to the live mode: lease deadlines and their
//! extension, flow control, ordered delivery by key, exactly-once delivery, retention, and a
//! topology the service expects to find but did not create (a subscription here that no descriptor
//! attaches to a topic is taken to exist, attached to the topic of its own name).

mod project;
mod settle;

use std::sync::Arc;

pub(crate) use project::Project;
pub(crate) use settle::{Consumer, Settlement};

use crate::error::PubSubError;
use crate::subscriber::PubSubSubscriber;
use crate::subscription::GooglePubSub;

/// Opens the subscription `descriptor` describes on the in-process transport: creates it where
/// the descriptor opts in, writes the declared dead-letter policy, and attaches a consumer.
///
/// # Errors
///
/// Returns [`PubSubError::NotConnected`] once the transport has shut down,
/// [`PubSubError::Admin`] for a resource name the service refuses to create, and
/// [`PubSubError::Receive`] for a subscription name the service refuses to pull from.
pub(crate) fn subscribe(
    project: &Arc<Project>,
    descriptor: &GooglePubSub,
) -> Result<PubSubSubscriber, PubSubError> {
    project.ensure_open()?;
    let consumer = project.open(descriptor)?;
    Ok(PubSubSubscriber::in_process(
        project.subscription_path(descriptor.subscription()),
        consumer,
        descriptor.batch_wait_value(),
    ))
}

/// The limits the service holds a message to at publish time.
pub(crate) mod limits {
    /// How many attributes one message may carry.
    pub(crate) const ATTRIBUTES: usize = 100;
    /// The longest attribute key, in bytes.
    pub(crate) const ATTRIBUTE_KEY: usize = 256;
    /// The longest attribute value, in bytes.
    pub(crate) const ATTRIBUTE_VALUE: usize = 1024;
    /// The longest ordering key, in bytes.
    pub(crate) const ORDERING_KEY: usize = 1024;
    /// The largest message the service accepts: its data, attributes and ordering key together.
    pub(crate) const MESSAGE: usize = 10 * 1024 * 1024;
}

/// Checks a project id against the rule the service applies: six to thirty lowercase letters,
/// digits and hyphens, starting with a letter and not ending with a hyphen, with an optional
/// `domain:` prefix for a domain-scoped project.
fn check_project(project: &str) -> Result<(), String> {
    // A colon names a domain, so what precedes it is a domain and never empty.
    let (domain_ok, id) = project
        .rsplit_once(':')
        .map_or((true, project), |(domain, id)| {
            let domain_ok = !domain.is_empty()
                && domain.chars().all(|c| {
                    c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, '.' | '-')
                });
            (domain_ok, id)
        });
    let id_ok = (6..=30).contains(&id.len())
        && id.starts_with(|c: char| c.is_ascii_lowercase())
        && !id.ends_with('-')
        && id
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-');
    if domain_ok && id_ok {
        Ok(())
    } else {
        Err(format!(
            "the project {project:?}, which is not a project id"
        ))
    }
}

/// Checks `name`, a short id or a full resource name in `collection` (`topics` or
/// `subscriptions`), against the rule the service applies to resource ids: three to 255
/// characters, starting with a letter, made of letters, digits and `-_.~+%`, and not starting
/// with `goog`.
pub(crate) fn check_resource_name(collection: &str, name: &str) -> Result<(), String> {
    let id = match name.strip_prefix("projects/") {
        Some(rest) => {
            let mut parts = rest.splitn(3, '/');
            let (project, kind, id) = (parts.next(), parts.next(), parts.next());
            match (project, kind, id) {
                (Some(project), Some(kind), Some(id)) if kind == collection => {
                    check_project(project).map_err(|reason| format!("{name:?} names {reason}"))?;
                    id
                }
                _ => {
                    return Err(format!(
                        "{name:?} is not a resource name of the form \
                         projects/{{project}}/{collection}/{{id}}"
                    ));
                }
            }
        }
        None => name,
    };
    let valid_char =
        |c: char| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | '~' | '+' | '%');
    if !(3..=255).contains(&id.len())
        || !id.starts_with(|c: char| c.is_ascii_alphabetic())
        || !id.chars().all(valid_char)
        || id.starts_with("goog")
    {
        return Err(format!(
            "{id:?} is not a valid resource id: 3 to 255 characters, starting with a letter, of \
             letters, digits and -_.~+%, and not starting with \"goog\""
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::check_resource_name;

    #[test]
    fn the_service_accepts_these_names() {
        for name in [
            "orders",
            "orders-workers",
            "conformance.lifecycle.1f-2a-0",
            "a~b+c%d_e",
            "projects/my-project/topics/orders",
            "projects/example.com:my-project/topics/orders",
        ] {
            assert!(check_resource_name("topics", name).is_ok(), "{name}");
        }
    }

    #[test]
    fn the_service_refuses_these_names() {
        for name in [
            "",
            "ab",
            "1orders",
            "orders workers",
            "orders/eu",
            "google-orders",
            "projects/my-project/subscriptions/orders",
            "projects//topics/orders",
            "projects/!/topics/orders",
            "projects/short/topics/orders",
            "projects/My-Project/topics/orders",
            "projects/my-project-/topics/orders",
            "projects/:my-project/topics/orders",
            "projects/my-project/topics",
        ] {
            assert!(check_resource_name("topics", name).is_err(), "{name}");
        }
        assert!(check_resource_name("topics", &"o".repeat(256)).is_err());
    }
}
