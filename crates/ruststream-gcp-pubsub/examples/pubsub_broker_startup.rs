//! Startup: the broker builds its clients, and each subscription creates or updates its topology
//! before its streaming pull opens. A declaration Pub/Sub cannot hold stops the service there.
//!
//! ```text
//! cargo test --example pubsub_broker_startup --features testing
//! cargo run --example pubsub_broker_startup --features testing -- run
//! ```

use ruststream_gcp_pubsub::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

// --8<-- [start:app]
/// The subscription `orders-workers` on the topic `orders`, both created when they are missing.
#[subscriber(GooglePubSub::new("orders-workers").create_with_topic("orders"))]
async fn place(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "order placed");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        PubSubBroker::new("my-project"),
        |b| {
            // Before the pull opens: the topics `orders` and `orders-dead`, then the
            // subscription with its dead-letter policy.
            b.include(place)
                .max_attempts(nonzero!(5u32))
                .dead_letter("orders-dead");
        },
    )
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_gcp_pubsub::prelude::*;

    use super::{Order, app, place};

    #[tokio::test]
    async fn the_created_subscription_reads_its_topic() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<PubSubBroker>()
            .message(&Order { id: 7 })
            .to("orders")
            .publish()
            .await?;

        tb.broker::<PubSubBroker>()
            .subscriber("orders-workers")
            .assert_called_once()
            .with(&Order { id: 7 })
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_cap_pubsub_does_not_accept_stops_the_startup() {
        let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
            PubSubBroker::new("my-project"),
            |b| {
                b.include(place)
                    .max_attempts(nonzero!(3u32))
                    .dead_letter("orders-dead");
            },
        );

        let failed = TestApp::start(app).await.err();
        let failed = format!("{:#}", failed.expect("startup took a cap of three"));
        assert!(failed.contains("max_attempts(3)"), "{failed}");
    }
}
