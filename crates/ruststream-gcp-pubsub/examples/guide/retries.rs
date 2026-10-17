//! The payments service of the Acknowledgement and retries guide: each answer of the bank settles
//! the delivery its own way, five deliveries at most, then the dead-letter topic.
//!
//! ```text
//! just brokers-up
//! cargo run --example guide_retries --features testing,ruststream/logging -- run
//! ```

use std::time::Duration;

use ruststream_gcp_pubsub::DELIVERY_ATTEMPT_HEADER;
use ruststream_gcp_pubsub::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "payments")]
struct Payment {
    id: u64,
    card: String,
    amount_cents: u64,
}

/// What the bank answered to a charge.
enum Answer {
    Charged,
    /// The bank holds the payment for a review that takes a while.
    Held,
    /// The bank did not answer.
    Unreachable,
    /// The card is closed; another try gets the same answer.
    Declined,
}

/// Cards of the bank's sandbox that give a fixed answer.
const HELD_CARD: &str = "4000000000009995";
const UNREACHABLE_CARD: &str = "4000000000000119";
const DECLINED_CARD: &str = "4000000000000002";

/// Stands in for the bank's client: its sandbox answers by card number.
fn ask_the_bank(payment: &Payment) -> Answer {
    match payment.card.as_str() {
        HELD_CARD => Answer::Held,
        UNREACHABLE_CARD => Answer::Unreachable,
        DECLINED_CARD => Answer::Declined,
        _ => Answer::Charged,
    }
}

// --8<-- [start:charge]
/// How long the bank's review of a held payment takes.
const HOLD: Duration = Duration::from_secs(30);

#[subscriber(GooglePubSub::new("payments-workers").create_with_topic("payments"))]
async fn charge(payment: &Payment, ctx: &mut Context<'_>) -> HandlerOutcome {
    let attempt = ctx
        .headers()
        .get_str(DELIVERY_ATTEMPT_HEADER)
        .unwrap_or("1");
    match ask_the_bank(payment) {
        Answer::Charged => HandlerOutcome::ack(),
        Answer::Held => {
            tracing::info!(
                payment.id,
                attempt,
                "payment held by the bank, asked again later"
            );
            HandlerOutcome::retry_after(HOLD)
        }
        Answer::Unreachable => {
            tracing::warn!(payment.id, attempt, "bank unreachable, payment retried");
            HandlerOutcome::retry()
        }
        Answer::Declined => {
            tracing::warn!(payment.id, "card declined, payment dropped");
            HandlerOutcome::drop()
        }
    }
}
// --8<-- [end:charge]

// --8<-- [start:review]
/// Reads the dead-letter topic through a subscription of its own.
#[subscriber(GooglePubSub::new("payments-dead-review").create_with_topic("payments-dead"))]
async fn review(payment: &Payment) -> HandlerOutcome {
    tracing::error!(
        payment.id,
        "payment ran out of attempts, a person looks at it"
    );
    HandlerOutcome::ack()
}
// --8<-- [end:review]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(
        PubSubBroker::new("my-project").emulator("localhost:8085"),
        |b| {
            // Five deliveries of one payment, the first included; then `payments-dead`.
            b.include(charge)
                .max_attempts(nonzero!(5u32))
                .dead_letter("payments-dead");
            b.include(review);
        },
    )
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::Duration;

    use ruststream::testing::TestApp;
    use ruststream_gcp_pubsub::prelude::*;

    use super::{DECLINED_CARD, HELD_CARD, HOLD, Payment, UNREACHABLE_CARD, app};

    fn payment(id: u64, card: &str) -> Payment {
        Payment {
            id,
            card: card.to_owned(),
            amount_cents: 1250,
        }
    }

    #[tokio::test]
    async fn a_charged_payment_is_acknowledged() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<PubSubBroker>()
            .message(&payment(1, "4242424242424242"))
            .publish()
            .await?;

        tb.broker::<PubSubBroker>()
            .subscriber("payments-workers")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_declined_payment_is_dropped_and_not_dead_lettered() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<PubSubBroker>()
            .message(&payment(2, DECLINED_CARD))
            .publish()
            .await?;

        tb.broker::<PubSubBroker>()
            .subscriber("payments-workers")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        tb.broker::<PubSubBroker>()
            .subscriber("payments-dead-review")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_payment_out_of_attempts_reaches_the_review() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<PubSubBroker>()
            .message(&payment(3, UNREACHABLE_CARD))
            .publish()
            .await?;

        tb.broker::<PubSubBroker>()
            .subscriber("payments-workers")
            .assert_called(5)
            .settled(HandlerOutcome::retry());
        tb.broker::<PubSubBroker>()
            .subscriber("payments-dead-review")
            .assert_called_once()
            .with(&payment(3, UNREACHABLE_CARD));
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_held_payment_comes_back_once_the_hold_is_over() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<PubSubBroker>()
            .message(&payment(4, HELD_CARD))
            .publish()
            .await?;
        tb.broker::<PubSubBroker>()
            .subscriber("payments-workers")
            .assert_called_once()
            .settled(HandlerOutcome::retry_after(HOLD));

        tb.advance(HOLD.saturating_sub(Duration::from_secs(1)))
            .await?;
        tb.broker::<PubSubBroker>()
            .subscriber("payments-workers")
            .assert_called_once();

        tb.advance(Duration::from_secs(1)).await?;
        tb.broker::<PubSubBroker>()
            .subscriber("payments-workers")
            .assert_called(2);
        tb.shutdown().await?;
        Ok(())
    }
}
