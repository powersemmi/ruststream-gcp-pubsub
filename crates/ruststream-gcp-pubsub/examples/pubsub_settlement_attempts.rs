//! Attempts: on a subscription with a dead-letter policy, a delivery reports which attempt it is
//! in the `pubsub-delivery-attempt` header.
//!
//! ```text
//! cargo test --example pubsub_settlement_attempts --features testing
//! cargo run --example pubsub_settlement_attempts --features testing -- run
//! ```

use ruststream_gcp_pubsub::DELIVERY_ATTEMPT_HEADER;
use ruststream_gcp_pubsub::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
    in_stock: bool,
}

// --8<-- [start:handler]
/// Waits for stock through two redeliveries, then gives the order up on the third attempt.
#[subscriber(GooglePubSub::new("orders-workers").create_with_topic("orders"))]
async fn reserve(order: &Order, ctx: &mut Context<'_>) -> HandlerOutcome {
    let attempt: u32 = ctx
        .headers()
        .get_str(DELIVERY_ATTEMPT_HEADER)
        .and_then(|attempt| attempt.parse().ok())
        .unwrap_or(1);
    if order.in_stock {
        return HandlerOutcome::ack();
    }
    if attempt >= 3 {
        tracing::error!(order.id, attempt, "no stock, order given up");
        return HandlerOutcome::drop();
    }
    tracing::warn!(order.id, attempt, "no stock yet");
    HandlerOutcome::retry()
}
// --8<-- [end:handler]

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        PubSubBroker::new("my-project"),
        |b| {
            // The dead-letter policy is what makes Pub/Sub count the attempts.
            b.include(reserve)
                .max_attempts(nonzero!(5u32))
                .dead_letter("orders-dead");
        },
    )
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_gcp_pubsub::prelude::*;

    use super::{Order, app};

    #[tokio::test]
    async fn the_third_attempt_gives_the_order_up() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let order = Order {
            id: 7,
            in_stock: false,
        };
        tb.broker::<PubSubBroker>()
            .message(&order)
            .publish()
            .await?;

        tb.broker::<PubSubBroker>()
            .subscriber("orders-workers")
            .assert_called(3)
            .settled(HandlerOutcome::drop());
        // Dropped before the cap, so it is acknowledged and never dead-lettered.
        tb.broker::<PubSubBroker>()
            .published::<Order>("orders-dead")
            .assert_not_called();
        Ok(())
    }
}
