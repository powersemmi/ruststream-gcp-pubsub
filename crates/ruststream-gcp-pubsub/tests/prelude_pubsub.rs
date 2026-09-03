//! The crate prelude leaves the framework's vocabulary intact.
//!
//! One glob brings in both preludes, and an explicit re-export wins over a glob, so a name this
//! crate re-exports quietly takes over the framework's. These pins fail to compile the day that
//! happens, rather than leaving a service unable to name a trait it was told to use.

use ruststream_gcp_pubsub::prelude::*;

/// A pin, not a helper: the bound is the whole point. `Publish` reached through this glob has to
/// be the framework's slot capability trait, so re-exporting the publish policy under that name
/// turns this into `E0404: expected trait, found struct`.
fn _publish_is_the_frameworks_trait<T: Publish>() {}

/// The publish policy travels under its prefixed name, which is what a mount site attaches.
#[test]
fn the_policy_is_in_the_prelude_under_its_own_name() {
    let policy = PubSubPublish;
    assert_eq!(format!("{policy:?}"), "PubSubPublish");
}
