//! The `partition-key` header: the portable spelling of the ordering key, read by the publisher
//! where nothing else named one.
//!
//! ```text
//! cargo test --example pubsub_publishing_ordering_header --features testing
//! cargo run --example pubsub_publishing_ordering_header --features testing -- run
//! ```

use ruststream_gcp_pubsub::PARTITION_KEY_HEADER;
use ruststream_gcp_pubsub::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Shipment {
    order_id: u64,
}

// --8<-- [start:partition-key]
/// The body names no broker type: the `partition-key` header orders the shipment.
#[subscriber("orders-workers")]
async fn ship(order: &Order, Out(out): Out<impl Publisher>) -> HandlerOutcome {
    let mut headers = HeaderMap::new();
    headers.insert(PARTITION_KEY_HEADER, format!("order-{}", order.id));
    let shipment = Shipment { order_id: order.id };
    let sent = out
        .message(&shipment)
        .with_headers(headers)
        .to("shipments")
        .publish()
        .await;
    match sent {
        Ok(()) => HandlerOutcome::ack(),
        Err(_) => HandlerOutcome::retry(),
    }
}
// --8<-- [end:partition-key]

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        PubSubBroker::new("my-project"),
        |b| {
            b.include(ship).out(DefaultSlot, Publish::default()).build();
        },
    )
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_gcp_pubsub::PARTITION_KEY_HEADER;
    use ruststream_gcp_pubsub::prelude::*;

    use super::{Order, Shipment, app};

    #[tokio::test]
    async fn the_header_names_the_key() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<PubSubBroker>()
            .message(&Order { id: 7 })
            .to("orders-workers")
            .publish()
            .await?;

        // The call named no key, so the slot carries no setting; the publisher read the header.
        tb.out::<DefaultSlot>()
            .assert_called_once()
            .assert_options_default();
        tb.broker::<PubSubBroker>()
            .published::<Shipment>("shipments")
            .assert_called_once()
            .with(&Shipment { order_id: 7 })
            .with_header(PARTITION_KEY_HEADER, "order-7");
        Ok(())
    }
}
