//! Batches: a handler that takes a slice receives batches assembled on the client. `batch(n)`
//! bounds their size, and `batch_wait` how long a partial batch waits for the rest.
//!
//! ```text
//! cargo test --example pubsub_subscriptions_batches --features testing
//! cargo run --example pubsub_subscriptions_batches --features testing -- run
//! ```

use std::time::Duration;

use ruststream_gcp_pubsub::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

// --8<-- [start:batches]
/// The slice makes this a batch handler; a partial batch waits 200 milliseconds for the rest.
#[subscriber(GooglePubSub::new("orders-workers").batch_wait(Duration::from_millis(200)))]
async fn settle(orders: &[Order]) -> HandlerOutcome {
    tracing::info!(count = orders.len(), "orders settled");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        PubSubBroker::new("my-project"),
        |b| {
            // At most 50 orders reach the handler per call.
            b.include(settle.batch(nonzero!(50)));
        },
    )
}
// --8<-- [end:batches]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_gcp_pubsub::prelude::*;

    use super::{Order, app};

    #[tokio::test(start_paused = true)]
    async fn orders_published_together_meet_in_one_batch() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        // Published together, the three orders meet in one batch.
        let broker = tb.broker::<PubSubBroker>();
        let (first, second, third) = tokio::join!(
            broker
                .message(&Order { id: 1 })
                .to("orders-workers")
                .publish(),
            broker
                .message(&Order { id: 2 })
                .to("orders-workers")
                .publish(),
            broker
                .message(&Order { id: 3 })
                .to("orders-workers")
                .publish(),
        );
        first?;
        second?;
        third?;

        let batches = tb
            .broker::<PubSubBroker>()
            .subscriber("orders-workers")
            .assert_batch_sizes(&[3])
            .batches::<Order>();
        let orders: Vec<Order> = batches.into_iter().flatten().collect();
        assert_eq!(orders, [Order { id: 1 }, Order { id: 2 }, Order { id: 3 }]);
        tb.shutdown().await?;
        Ok(())
    }
}
