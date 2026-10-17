//! The subscription descriptor: `GooglePubSub` names a subscription and carries its settings, and
//! `create_with_topic` creates the subscription and its topic at startup.
//!
//! ```text
//! cargo test --example pubsub_subscriptions_descriptor --features testing
//! cargo run --example pubsub_subscriptions_descriptor --features testing -- run
//! ```

use ruststream_gcp_pubsub::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

// --8<-- [start:create-with-topic]
// --8<-- [start:descriptor]
/// The subscription `orders-workers` on the topic `orders`, with at most 100 orders unsettled.
#[subscriber(
    GooglePubSub::new("orders-workers")
        .create_with_topic("orders")
        .max_outstanding(100)
)]
async fn place(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "order placed");
    HandlerOutcome::ack()
}
// --8<-- [end:descriptor]

/// A second subscription on the same topic: it receives every order too.
#[subscriber(GooglePubSub::new("orders-audit").create_with_topic("orders"))]
async fn audit(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "order audited");
    HandlerOutcome::ack()
}
// --8<-- [end:create-with-topic]

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        PubSubBroker::new("my-project"),
        |b| {
            b.include(place);
            b.include(audit);
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
    async fn every_subscription_of_the_topic_receives_the_order() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<PubSubBroker>()
            .message(&Order { id: 7 })
            .publish()
            .await?;

        for subscription in ["orders-workers", "orders-audit"] {
            tb.broker::<PubSubBroker>()
                .subscriber(subscription)
                .assert_called_once()
                .with(&Order { id: 7 })
                .settled(HandlerOutcome::ack());
        }
        tb.shutdown().await?;
        Ok(())
    }
}
