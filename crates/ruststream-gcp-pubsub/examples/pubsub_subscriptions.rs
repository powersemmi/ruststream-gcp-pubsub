//! A subscription by name: `#[subscriber("orders-workers")]` reads that existing subscription.
//!
//! ```text
//! cargo test --example pubsub_subscriptions --features testing
//! cargo run --example pubsub_subscriptions --features testing -- run
//! ```

use ruststream_gcp_pubsub::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

// --8<-- [start:name]
/// Reads the subscription `orders-workers`, which the deployment created on its topic.
#[subscriber("orders-workers")]
async fn place(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "order placed");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        PubSubBroker::new("my-project"),
        |b| {
            b.include(place);
        },
    )
}
// --8<-- [end:name]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_gcp_pubsub::prelude::*;

    use super::{Order, app};

    #[tokio::test]
    async fn the_subscription_reads_its_topic() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        // In process, a subscription the service only names is attached to the topic of its own
        // name.
        tb.broker::<PubSubBroker>()
            .message(&Order { id: 7 })
            .to("orders-workers")
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
    async fn another_topic_is_not_read() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<PubSubBroker>()
            .message(&Order { id: 8 })
            .to("refunds")
            .publish()
            .await?;

        tb.broker::<PubSubBroker>()
            .subscriber("orders-workers")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }
}
