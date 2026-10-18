//! The policy's key: `Publish::default().ordering_key(..)` orders every message of a mount site
//! under one key, a reply included.
//!
//! ```text
//! cargo test --example pubsub_publishing_ordering_policy --features testing
//! cargo run --example pubsub_publishing_ordering_policy --features testing -- run
//! ```

use ruststream_gcp_pubsub::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "ledger")]
struct LedgerEntry {
    order_id: u64,
}

// --8<-- [start:policy]
/// Books every order in the ledger.
#[subscriber("orders-workers", reply)]
async fn book(order: &Order) -> LedgerEntry {
    LedgerEntry { order_id: order.id }
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        PubSubBroker::new("my-project"),
        |b| {
            // The ledger is one sequence, so every entry goes out under one key.
            b.include(book)
                .out_reply(Publish::default().ordering_key("ledger"));
        },
    )
}
// --8<-- [end:policy]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_gcp_pubsub::PARTITION_KEY_HEADER;
    use ruststream_gcp_pubsub::prelude::*;

    use super::{LedgerEntry, Order, app};

    #[tokio::test]
    async fn every_entry_goes_out_under_the_policys_key() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        for id in [7, 8] {
            tb.broker::<PubSubBroker>()
                .message(&Order { id })
                .to("orders-workers")
                .publish()
                .await?;
        }

        tb.broker::<PubSubBroker>()
            .published::<LedgerEntry>("ledger")
            .assert_called(2)
            .with_header(PARTITION_KEY_HEADER, "ledger");
        Ok(())
    }
}
