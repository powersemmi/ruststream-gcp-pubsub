//! The refunds service of the Testing guide and its tests: in process under `TestApp::start`, and
//! the same test body against the Pub/Sub emulator under `TestApp::start_live`. The live test
//! skips without `PUBSUB_TEST_HOST`, and `RUSTSTREAM_REQUIRE_LIVE=1` turns that skip into a
//! failure.
//!
//! ```text
//! cargo test --example guide_testing --features testing
//! just brokers-up
//! PUBSUB_TEST_HOST=127.0.0.1:8085 cargo test --example guide_testing --features testing
//! ```

use std::time::Duration;

use ruststream_gcp_pubsub::prelude::*;
use serde::{Deserialize, Serialize};

#[cfg(test)]
#[path = "../support/pubsub_live.rs"]
mod live;

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "refunds")]
struct Refund {
    id: u64,
    amount_cents: u64,
}

/// The answer to a refund that went through.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "refunds-done")]
struct Refunded {
    refund_id: u64,
}

/// Refunds above this wait for the finance team's approval.
const APPROVAL_LIMIT_CENTS: u64 = 100_000;

/// How long a refund waiting for approval waits before it is tried again.
const APPROVAL_WAIT: Duration = Duration::from_mins(1);

#[subscriber(GooglePubSub::new("refunds-workers").create_with_topic("refunds"), reply)]
async fn send_refund(refund: &Refund) -> Result<Refunded, HandlerOutcome> {
    if refund.amount_cents == 0 {
        tracing::warn!(refund.id, "refund without an amount dropped");
        return Err(HandlerOutcome::drop());
    }
    if refund.amount_cents > APPROVAL_LIMIT_CENTS {
        tracing::info!(refund.id, "refund waits for approval");
        return Err(HandlerOutcome::retry_after(APPROVAL_WAIT));
    }
    tracing::info!(refund.id, refund.amount_cents, "refund sent");
    Ok(Refunded {
        refund_id: refund.id,
    })
}

/// The service on `broker`: the production builder, unchanged in either mode.
fn service(broker: PubSubBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("refunds", "0.1.0")).with_broker(broker, |b| {
        // Five deliveries of one refund, the first included; then `refunds-review`.
        b.include(send_refund)
            .max_attempts(nonzero!(5u32))
            .dead_letter("refunds-review")
            .out_reply(Publish::default());
    })
}

#[ruststream::app]
fn app() -> impl App<State = ()> {
    service(PubSubBroker::new("my-project").emulator("localhost:8085"))
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use google_cloud_auth::credentials::anonymous;
    use google_cloud_pubsub::client::TopicAdmin;
    use ruststream::testing::TestApp;
    use ruststream_gcp_pubsub::prelude::*;

    use super::{APPROVAL_WAIT, Refund, Refunded, app, live, service};

    // --8<-- [start:answered]
    /// The body both modes run: a refund is answered on `refunds-done`.
    async fn a_refund_is_answered(tb: TestApp<()>) -> Result<(), Box<dyn Error>> {
        tb.broker::<PubSubBroker>()
            .message(&Refund {
                id: 7,
                amount_cents: 1250,
            })
            .publish()
            .await?;

        tb.broker::<PubSubBroker>()
            .subscriber("refunds-workers")
            .assert_called_once()
            .with(&Refund {
                id: 7,
                amount_cents: 1250,
            })
            .settled(HandlerOutcome::ack());
        tb.broker::<PubSubBroker>()
            .published::<Refunded>("refunds-done")
            .assert_called_once()
            .with(&Refunded { refund_id: 7 });
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn in_process() -> Result<(), Box<dyn Error>> {
        a_refund_is_answered(TestApp::start(app()).await?).await
    }
    // --8<-- [end:answered]

    // --8<-- [start:dropped]
    #[tokio::test]
    async fn a_refund_without_an_amount_is_dropped() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<PubSubBroker>()
            .message(&Refund {
                id: 8,
                amount_cents: 0,
            })
            .publish()
            .await?;

        tb.broker::<PubSubBroker>()
            .subscriber("refunds-workers")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        tb.broker::<PubSubBroker>()
            .published::<Refunded>("refunds-done")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }
    // --8<-- [end:dropped]

    // --8<-- [start:held]
    #[tokio::test(start_paused = true)]
    async fn a_refund_never_approved_goes_to_review() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let large = Refund {
            id: 9,
            amount_cents: 250_000,
        };

        tb.broker::<PubSubBroker>()
            .message(&large)
            .publish()
            .await?;
        tb.broker::<PubSubBroker>()
            .subscriber("refunds-workers")
            .assert_called_once()
            .settled(HandlerOutcome::retry_after(APPROVAL_WAIT));

        // Each wait ends in a redelivery, and the fifth delivery is the last one allowed.
        for deliveries in 2..=5 {
            tb.advance(APPROVAL_WAIT).await?;
            tb.broker::<PubSubBroker>()
                .subscriber("refunds-workers")
                .assert_called(deliveries);
        }
        // The fifth delivery waits too; when its wait ends, Pub/Sub moves the refund to review.
        tb.advance(APPROVAL_WAIT).await?;
        tb.broker::<PubSubBroker>()
            .published::<Refund>("refunds-review")
            .assert_called_once()
            .with(&large);
        tb.shutdown().await?;
        Ok(())
    }
    // --8<-- [end:held]

    /// Creates `topic` where it is missing, as operations do before the service runs.
    async fn create_topic(host: &str, topic: &str) -> Result<(), Box<dyn Error>> {
        let created = TopicAdmin::builder()
            .with_endpoint(format!("http://{host}"))
            .with_credentials(anonymous::Builder::new().build())
            .build()
            .await?
            .create_topic()
            .set_name(format!("projects/my-project/topics/{topic}"))
            .send()
            .await;
        match created {
            Err(err) if err.http_status_code() != Some(409) => Err(err.into()),
            _ => Ok(()),
        }
    }

    // --8<-- [start:live]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn against_the_emulator() -> Result<(), Box<dyn Error>> {
        let Some(host) = live::host() else {
            return Ok(());
        };
        live::fresh(&host, &["refunds-workers"]).await?;
        create_topic(&host, "refunds-done").await?;
        let broker = PubSubBroker::new("my-project").emulator(host);
        a_refund_is_answered(TestApp::start_live(service(broker)).await?).await
    }
    // --8<-- [end:live]
}
