//! The crate prelude serves a routes file, and keeps the two vocabularies apart.
//!
//! Through this glob a publish policy arrives under its mount-site name, so `Publish` here is this
//! broker's policy - a name the framework's prelude leaves free for exactly that. The capability
//! traits a handler body bounds a slot with still come through, because a body names them from the
//! framework's prelude, which it imports on its own. These are compile-time pins, and they fail
//! the day either half moves.

use ruststream_gcp_pubsub::prelude::*;

/// A pin, not a helper: the bound is the whole point. The capability a handler body bounds a plain
/// slot with survives this glob.
fn _p<T: Publisher>() {}

/// The bound a body writes when it adjusts a per-message setting, naming this crate's settings
/// type. That is the one thing such a body takes from here rather than from the framework.
fn _s<T: Publisher<Options = PubSubPublishOptions>>() {}

/// The step itself, on the framework's publish builder, reached through the same glob.
fn _o<T: PubSubOrdering>(builder: T) -> T {
    builder.ordering_key("order-42")
}

/// The mount-site vocabulary: the publish policy, under the name every broker's prelude gives it,
/// carrying the default of the one setting a call site may override.
#[test]
fn the_policy_arrives_under_its_mount_site_name() {
    let policy: Publish = Publish::default().ordering_key("order-42");
    assert_eq!(
        format!("{policy:?}"),
        r#"PubSubPublish { ordering_key: Some("order-42") }"#
    );
}
