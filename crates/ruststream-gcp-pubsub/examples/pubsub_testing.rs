//! Testing a Pub/Sub service: `TestApp::start` connects the production `PubSubBroker` in process,
//! with topics, subscriptions, redelivery and dead letters, and no emulator.
//!
//! ```text
//! cargo test --example pubsub_testing --features testing
//! ```

use std::time::Duration;

use ruststream_gcp_pubsub::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "payments")]
struct Payment {
    id: u64,
    amount_cents: i64,
    settled: bool,
}

/// How long a payment the bank has not settled waits before it comes back.
const RECHECK: Duration = Duration::from_secs(30);

#[subscriber(GooglePubSub::new("payments-workers").create_with_topic("payments"))]
async fn charge(payment: &Payment) -> HandlerOutcome {
    if payment.amount_cents <= 0 {
        return HandlerOutcome::drop();
    }
    if !payment.settled {
        return HandlerOutcome::retry_after(RECHECK);
    }
    tracing::info!(payment.id, "payment charged");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(
        PubSubBroker::new("my-project"),
        |b| {
            b.include(charge)
                .max_attempts(nonzero!(5u32))
                .dead_letter("payments-dead");
        },
    )
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_gcp_pubsub::prelude::*;

    use super::{Payment, RECHECK, app};

    // --8<-- [start:testapp]
    #[tokio::test]
    async fn a_settled_payment_is_charged() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let payment = Payment {
            id: 7,
            amount_cents: 1_000,
            settled: true,
        };

        tb.broker::<PubSubBroker>()
            .message(&payment)
            .publish()
            .await?;

        tb.broker::<PubSubBroker>()
            .subscriber("payments-workers")
            .assert_called_once()
            .with(&payment)
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }
    // --8<-- [end:testapp]

    // --8<-- [start:advance]
    #[tokio::test(start_paused = true)]
    async fn an_unsettled_payment_is_dead_lettered_after_five_attempts()
    -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let payment = Payment {
            id: 8,
            amount_cents: 1_000,
            settled: false,
        };

        tb.broker::<PubSubBroker>()
            .message(&payment)
            .publish()
            .await?;
        for _ in 0..5 {
            tb.advance(RECHECK).await?;
        }

        tb.broker::<PubSubBroker>()
            .subscriber("payments-workers")
            .assert_called(5);
        tb.broker::<PubSubBroker>()
            .published::<Payment>("payments-dead")
            .assert_called_once()
            .with(&payment);
        tb.shutdown().await?;
        Ok(())
    }
    // --8<-- [end:advance]
}
