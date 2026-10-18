//! What the live tests of the examples share: the gate on the emulator, and subscriptions no
//! earlier run has left messages in.
//!
//! A live test skips when `PUBSUB_TEST_HOST` is unset, so `cargo test` without an emulator passes.
//! `RUSTSTREAM_REQUIRE_LIVE` turns that skip into a failure, for a run that stood an emulator up.

use std::error::Error;

use google_cloud_auth::credentials::anonymous;
use google_cloud_pubsub::client::SubscriptionAdmin;

/// The project the examples' brokers are built with; the emulator accepts any.
const PROJECT: &str = "my-project";

/// The emulator's address, or `None` to skip the test.
///
/// # Panics
///
/// Panics when `RUSTSTREAM_REQUIRE_LIVE` is set and `PUBSUB_TEST_HOST` is not: the run stood an
/// emulator up, and a test that skipped past it would report a pass it never earned.
pub(crate) fn host() -> Option<String> {
    match std::env::var("PUBSUB_TEST_HOST") {
        Ok(host) if !host.is_empty() => Some(host),
        _ => {
            let required = std::env::var("RUSTSTREAM_REQUIRE_LIVE").is_ok_and(|v| !v.is_empty());
            assert!(
                !required,
                "RUSTSTREAM_REQUIRE_LIVE is set, so this test must run, but PUBSUB_TEST_HOST is unset",
            );
            None
        }
    }
}

/// Deletes `subscriptions` where an earlier run created them, so the descriptors that create them
/// start empty and the run reads only what it publishes.
///
/// The emulator keeps its subscriptions until it stops, and an acknowledgement the client had not
/// sent before an earlier run shut down leaves its message there for the next run.
pub(crate) async fn fresh(host: &str, subscriptions: &[&str]) -> Result<(), Box<dyn Error>> {
    let admin = SubscriptionAdmin::builder()
        .with_endpoint(format!("http://{host}"))
        .with_credentials(anonymous::Builder::new().build())
        .build()
        .await?;
    for subscription in subscriptions {
        let deleted = admin
            .delete_subscription()
            .set_subscription(format!("projects/{PROJECT}/subscriptions/{subscription}"))
            .send()
            .await;
        match deleted {
            Err(err) if err.http_status_code() != Some(404) => return Err(err.into()),
            _ => {}
        }
    }
    Ok(())
}
