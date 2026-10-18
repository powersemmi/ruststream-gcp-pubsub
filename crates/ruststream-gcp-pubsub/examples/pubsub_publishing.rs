//! Publishing: `Publish`, the prelude's name for `PubSubPublish`, publishes a handler's reply and
//! the messages of its `Out` slots to Pub/Sub topics.
//!
//! ```text
//! cargo test --example pubsub_publishing --features testing
//! cargo run --example pubsub_publishing --features testing -- run
//! ```

use ruststream_gcp_pubsub::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "receipts")]
struct Receipt {
    order_id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Invoice {
    order_id: u64,
}

// --8<-- [start:reply]
/// The receipt goes to `receipts`, the topic its type names.
#[subscriber(GooglePubSub::new("orders-receipts").create_with_topic("orders"), reply)]
async fn receipt(order: &Order) -> Receipt {
    Receipt { order_id: order.id }
}
// --8<-- [end:reply]

// --8<-- [start:slot]
/// The invoice goes to the topic the call names.
#[subscriber(GooglePubSub::new("orders-billing").create_with_topic("orders"))]
async fn bill(order: &Order, Out(out): Out<impl Publisher>) -> HandlerOutcome {
    let invoice = Invoice { order_id: order.id };
    match out.message(&invoice).to("invoices").publish().await {
        Ok(()) => HandlerOutcome::ack(),
        Err(err) => {
            tracing::warn!(order.id, error = %err, "invoice not published");
            HandlerOutcome::retry()
        }
    }
}
// --8<-- [end:slot]

// --8<-- [start:mount]
#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        PubSubBroker::new("my-project"),
        |b| {
            b.include(receipt).out_reply(Publish::default());
            b.include(bill).out(DefaultSlot, Publish::default()).build();
        },
    )
}
// --8<-- [end:mount]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_gcp_pubsub::prelude::*;

    use super::{Invoice, Order, Receipt, app};

    #[tokio::test]
    async fn an_order_gets_its_receipt_and_its_invoice() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<PubSubBroker>()
            .message(&Order { id: 7 })
            .publish()
            .await?;

        tb.broker::<PubSubBroker>()
            .published::<Receipt>("receipts")
            .assert_called_once()
            .with(&Receipt { order_id: 7 });
        tb.broker::<PubSubBroker>()
            .published::<Invoice>("invoices")
            .assert_called_once()
            .with(&Invoice { order_id: 7 });
        tb.out::<DefaultSlot>().assert_called_once();
        Ok(())
    }
}
