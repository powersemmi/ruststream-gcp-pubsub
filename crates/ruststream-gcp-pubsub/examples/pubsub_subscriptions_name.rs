//! A subscription named at the mount site: `#[subscriber(GooglePubSub)]` fixes the kind, and
//! `.name(..)` where the handler is mounted names the subscription.
//!
//! ```text
//! cargo test --example pubsub_subscriptions_name --features testing
//! cargo run --example pubsub_subscriptions_name --features testing -- run
//! ```

use std::env;

use ruststream_gcp_pubsub::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

// --8<-- [start:name]
/// A Pub/Sub subscription whose name the deployment decides.
#[subscriber(GooglePubSub)]
async fn place(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "order placed");
    HandlerOutcome::ack()
}

/// One service per region, each reading the subscription of its region.
fn service(subscription: String) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        PubSubBroker::new("my-project"),
        |b| {
            b.include(place.name(subscription));
        },
    )
}
// --8<-- [end:name]

#[ruststream::app]
fn app() -> impl App<State = ()> {
    let subscription = env::var("ORDERS_SUBSCRIPTION").unwrap_or_else(|_| "orders-workers".into());
    service(subscription)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_gcp_pubsub::prelude::*;

    use super::{Order, service};

    #[tokio::test]
    async fn the_named_subscription_is_read() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service("orders-eu-workers".to_owned())).await?;

        tb.broker::<PubSubBroker>()
            .message(&Order { id: 7 })
            .to("orders-eu-workers")
            .publish()
            .await?;

        tb.broker::<PubSubBroker>()
            .subscriber("orders-eu-workers")
            .assert_called_once()
            .with(&Order { id: 7 })
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }
}
