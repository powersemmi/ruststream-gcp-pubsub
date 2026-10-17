//! The orders service of the Topics and subscriptions guide: billing and shipping each read every
//! order through a subscription the service creates, the audit reads a subscription operations
//! created, and the reports subscription is named by the deployment.
//!
//! ```text
//! just brokers-up
//! cargo run --example guide_topics_and_subscriptions --features testing,ruststream/logging -- run
//! ```

use std::env;

use ruststream_gcp_pubsub::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
    amount_cents: u64,
}

// --8<-- [start:fan-out]
/// Billing's own subscription on `orders`: it receives every order.
#[subscriber(GooglePubSub::new("orders-billing").create_with_topic("orders"))]
async fn bill(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, order.amount_cents, "order billed");
    HandlerOutcome::ack()
}

/// Shipping's own subscription on the same topic: it receives every order too.
#[subscriber(GooglePubSub::new("orders-shipping").create_with_topic("orders"))]
async fn ship(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "order shipped");
    HandlerOutcome::ack()
}
// --8<-- [end:fan-out]

// --8<-- [start:existing]
/// A subscription operations created on `orders`; the service only reads it.
#[subscriber("orders-audit")]
async fn audit(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "order audited");
    HandlerOutcome::ack()
}

/// A subscription whose name the deployment decides, mounted with `.name(..)` below.
#[subscriber(GooglePubSub)]
async fn report(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "order reported");
    HandlerOutcome::ack()
}
// --8<-- [end:existing]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App {
    let reports = env::var("REPORTS_SUBSCRIPTION").unwrap_or_else(|_| "orders-reports".to_owned());
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        PubSubBroker::new("my-project").emulator("localhost:8085"),
        |b| {
            b.include(bill);
            b.include(ship);
            b.include(audit);
            b.include(report.name(reports));
        },
    )
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_gcp_pubsub::prelude::*;

    use super::{Order, app};

    #[tokio::test]
    async fn every_subscription_of_the_topic_receives_the_order() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let order = Order {
            id: 7,
            amount_cents: 1250,
        };

        tb.broker::<PubSubBroker>()
            .message(&order)
            .to("orders")
            .publish()
            .await?;

        for subscription in ["orders-billing", "orders-shipping"] {
            tb.broker::<PubSubBroker>()
                .subscriber(subscription)
                .assert_called_once()
                .with(&order)
                .settled(HandlerOutcome::ack());
        }
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn the_subscriptions_operations_own_are_read_by_name() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let order = Order {
            id: 8,
            amount_cents: 990,
        };

        // In process, a subscription the service only names reads the topic of its own name.
        for subscription in ["orders-audit", "orders-reports"] {
            tb.broker::<PubSubBroker>()
                .message(&order)
                .to(subscription)
                .publish()
                .await?;
            tb.broker::<PubSubBroker>()
                .subscriber(subscription)
                .assert_called_once()
                .with(&order)
                .settled(HandlerOutcome::ack());
        }
        tb.shutdown().await?;
        Ok(())
    }
}
