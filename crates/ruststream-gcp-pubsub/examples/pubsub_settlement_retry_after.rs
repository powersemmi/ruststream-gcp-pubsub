//! `retry_after`: the crate holds the delivery under its lease for the delay, then nacks it so
//! the subscription redelivers.
//!
//! ```text
//! cargo test --example pubsub_settlement_retry_after --features testing
//! cargo run --example pubsub_settlement_retry_after --features testing -- run
//! ```

use std::time::Duration;

use ruststream_gcp_pubsub::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "payments")]
struct Payment {
    id: u64,
    settled: bool,
}

/// How long a payment the bank has not settled waits before it is checked again.
const RECHECK: Duration = Duration::from_secs(30);

// --8<-- [start:handler]
/// A payment the bank has not settled yet comes back in thirty seconds, not at once.
#[subscriber(GooglePubSub::new("payments-workers").create_with_topic("payments"))]
async fn reconcile(payment: &Payment) -> HandlerOutcome {
    if !payment.settled {
        return HandlerOutcome::retry_after(RECHECK);
    }
    tracing::info!(payment.id, "payment reconciled");
    HandlerOutcome::ack()
}
// --8<-- [end:handler]

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(
        PubSubBroker::new("my-project"),
        |b| {
            b.include(reconcile);
        },
    )
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::Duration;

    use ruststream::testing::TestApp;
    use ruststream_gcp_pubsub::prelude::*;

    use super::{Payment, RECHECK, app};

    #[tokio::test(start_paused = true)]
    async fn an_unsettled_payment_comes_back_after_the_delay() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let payment = Payment {
            id: 7,
            settled: false,
        };
        tb.broker::<PubSubBroker>()
            .message(&payment)
            .publish()
            .await?;

        tb.advance(Duration::from_millis(29_999)).await?;
        tb.broker::<PubSubBroker>()
            .subscriber("payments-workers")
            .assert_called_once()
            .settled(HandlerOutcome::retry_after(RECHECK));

        tb.advance(Duration::from_millis(1)).await?;
        tb.broker::<PubSubBroker>()
            .subscriber("payments-workers")
            .assert_called(2)
            .with(&payment);
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_settled_payment_is_acknowledged_at_once() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let payment = Payment {
            id: 8,
            settled: true,
        };
        tb.broker::<PubSubBroker>()
            .message(&payment)
            .publish()
            .await?;
        tb.advance(RECHECK).await?;

        tb.broker::<PubSubBroker>()
            .subscriber("payments-workers")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        Ok(())
    }
}
