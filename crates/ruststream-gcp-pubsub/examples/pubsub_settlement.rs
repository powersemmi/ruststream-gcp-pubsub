//! Acknowledgement: `ack` acknowledges, `retry` nacks so the subscription redelivers, and `drop`
//! acknowledges a delivery that is not worth another attempt.
//!
//! ```text
//! cargo test --example pubsub_settlement --features testing
//! cargo run --example pubsub_settlement --features testing -- run
//! ```

use ruststream_gcp_pubsub::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
    quantity: u32,
    in_stock: bool,
}

// --8<-- [start:outcomes]
/// Reserves stock for an order.
#[subscriber(GooglePubSub::new("orders-workers").create_with_topic("orders"))]
async fn reserve(order: &Order) -> HandlerOutcome {
    if order.quantity == 0 {
        // Nothing to reserve, now or later: acknowledged, never redelivered.
        return HandlerOutcome::drop();
    }
    if !order.in_stock {
        // Nacked: the subscription delivers the order again.
        return HandlerOutcome::retry();
    }
    tracing::info!(order.id, "stock reserved");
    HandlerOutcome::ack()
}
// --8<-- [end:outcomes]

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        PubSubBroker::new("my-project"),
        |b| {
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
    async fn a_reserved_order_is_acknowledged_once() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let order = Order {
            id: 1,
            quantity: 2,
            in_stock: true,
        };
        tb.broker::<PubSubBroker>()
            .message(&order)
            .publish()
            .await?;

        tb.broker::<PubSubBroker>()
            .subscriber("orders-workers")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        Ok(())
    }

    #[tokio::test]
    async fn an_empty_order_is_dropped_and_not_redelivered() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let order = Order {
            id: 2,
            quantity: 0,
            in_stock: true,
        };
        tb.broker::<PubSubBroker>()
            .message(&order)
            .publish()
            .await?;

        tb.broker::<PubSubBroker>()
            .subscriber("orders-workers")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        tb.broker::<PubSubBroker>()
            .published::<Order>("orders-dead")
            .assert_not_called();
        Ok(())
    }

    #[tokio::test]
    async fn an_order_out_of_stock_is_redelivered() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let order = Order {
            id: 3,
            quantity: 1,
            in_stock: false,
        };
        tb.broker::<PubSubBroker>()
            .message(&order)
            .publish()
            .await?;

        // Every delivery the subscription's cap allows, then the dead-letter topic.
        tb.broker::<PubSubBroker>()
            .subscriber("orders-workers")
            .assert_called(5)
            .settled(HandlerOutcome::retry());
        tb.broker::<PubSubBroker>()
            .published::<Order>("orders-dead")
            .assert_called_once()
            .with(&order);
        Ok(())
    }
}
