//! Dead letters: `max_attempts` and `dead_letter` at the mount site become the subscription's own
//! dead-letter policy, and Pub/Sub moves a spent message to the dead-letter topic itself.
//!
//! ```text
//! cargo test --example pubsub_settlement_dead_letters --features testing
//! cargo run --example pubsub_settlement_dead_letters --features testing -- run
//! ```

use ruststream_gcp_pubsub::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "payments")]
struct Payment {
    id: u64,
    amount_cents: i64,
}

/// The gateway declines every amount ending in 7.
#[subscriber(GooglePubSub::new("payments-workers").create_with_topic("payments"))]
async fn charge(payment: &Payment) -> HandlerOutcome {
    if payment.amount_cents % 10 == 7 {
        tracing::warn!(payment.id, "charge declined, retrying");
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

// --8<-- [start:reader]
/// The dead-letter topic is an ordinary topic, read through a subscription of its own.
#[subscriber(GooglePubSub::new("payments-dead-audit").create_with_topic("payments-dead"))]
async fn on_dead_letter(payment: &Payment) -> HandlerOutcome {
    tracing::error!(payment.id, "payment dead-lettered");
    HandlerOutcome::ack()
}
// --8<-- [end:reader]

// --8<-- [start:declaration]
#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(
        PubSubBroker::new("my-project"),
        |b| {
            // Five deliveries of one payment; after the fifth Pub/Sub publishes it to
            // `payments-dead`, payload and attributes as they arrived.
            b.include(charge)
                .max_attempts(nonzero!(5u32))
                .dead_letter("payments-dead");
            b.include(on_dead_letter);
        },
    )
}
// --8<-- [end:declaration]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_gcp_pubsub::prelude::*;

    use super::{Payment, app};

    #[tokio::test]
    async fn the_fifth_decline_is_dead_lettered_and_read_there() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let declined = Payment {
            id: 7,
            amount_cents: 1_007,
        };
        tb.broker::<PubSubBroker>()
            .message(&declined)
            .publish()
            .await?;

        tb.broker::<PubSubBroker>()
            .subscriber("payments-workers")
            .assert_called(5);
        tb.broker::<PubSubBroker>()
            .published::<Payment>("payments-dead")
            .assert_called_once()
            .with(&declined);
        tb.broker::<PubSubBroker>()
            .subscriber("payments-dead-audit")
            .assert_called_once()
            .with(&declined)
            .settled(HandlerOutcome::ack());
        Ok(())
    }

    #[tokio::test]
    async fn a_charged_payment_never_reaches_the_dead_letter_topic() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let paid = Payment {
            id: 1,
            amount_cents: 1_000,
        };
        tb.broker::<PubSubBroker>().message(&paid).publish().await?;

        tb.broker::<PubSubBroker>()
            .subscriber("payments-workers")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.broker::<PubSubBroker>()
            .published::<Payment>("payments-dead")
            .assert_not_called();
        Ok(())
    }
}
