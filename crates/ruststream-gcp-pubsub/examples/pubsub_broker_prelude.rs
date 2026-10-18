//! The prelude: one glob brings the core prelude, the broker, the descriptor, the publish policy
//! and the ordering-key step.
//!
//! ```text
//! cargo test --example pubsub_broker_prelude --features testing
//! cargo run --example pubsub_broker_prelude --features testing -- run
//! ```

use ruststream_gcp_pubsub::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "receipts")]
struct Receipt {
    order_id: u64,
}

// --8<-- [start:app]
/// The descriptor, the broker and the policy all come from the one glob.
#[subscriber(GooglePubSub::new("orders-workers").create_with_topic("orders"), reply)]
async fn receipt(order: &Order) -> Receipt {
    Receipt { order_id: order.id }
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        PubSubBroker::new("my-project"),
        |b| {
            b.include(receipt).out_reply(Publish::default());
        },
    )
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_gcp_pubsub::prelude::*;

    use super::{Order, Receipt, app};

    #[tokio::test]
    async fn every_order_gets_its_receipt() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<PubSubBroker>()
            .message(&Order { id: 7 })
            .to("orders")
            .publish()
            .await?;

        tb.broker::<PubSubBroker>()
            .published::<Receipt>("receipts")
            .assert_called_once()
            .with(&Receipt { order_id: 7 });
        tb.shutdown().await?;
        Ok(())
    }
}
