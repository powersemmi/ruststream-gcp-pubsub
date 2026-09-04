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

/// The same for the step this crate adds to that vocabulary.
fn _o<T: PubSubOrdering>() {}

/// The mount-site vocabulary: the publish policy, under the name every broker's prelude gives it.
/// This one holds no options, so the name is the whole expression a mount site writes.
#[test]
fn the_policy_arrives_under_its_mount_site_name() {
    let policy: Publish = Publish;
    assert_eq!(format!("{policy:?}"), "PubSubPublish");
}
