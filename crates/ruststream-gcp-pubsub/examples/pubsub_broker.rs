//! The broker: `PubSubBroker::new` records the project, and the runtime connects it at startup.
//!
//! ```text
//! cargo test --example pubsub_broker --features testing
//! cargo run --example pubsub_broker --features testing -- run
//! ```

use ruststream_gcp_pubsub::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

// --8<-- [start:app]
/// Reads the subscription `orders-workers`.
#[subscriber("orders-workers")]
async fn place(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "order placed");
    HandlerOutcome::ack()
}

/// Application Default Credentials, against `pubsub.googleapis.com`.
#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        PubSubBroker::new("my-project"),
        |b| {
            b.include(place);
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
    async fn the_subscription_is_read() -> Result<(), Box<dyn Error>> {
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
}
