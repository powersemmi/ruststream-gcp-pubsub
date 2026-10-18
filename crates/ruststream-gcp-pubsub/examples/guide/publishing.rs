//! The orders service of the Publishing and replies guide: a receipt answers each order, an
//! invoice goes out through an `Out` slot, and the service announces itself once it is up.
//!
//! ```text
//! just brokers-up
//! cargo run --example guide_publishing --features testing,ruststream/logging -- run
//! ```

use ruststream_gcp_pubsub::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
    amount_cents: u64,
}

// --8<-- [start:reply]
/// The receipt of an order; its type names the topic it goes to.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "receipts")]
struct Receipt {
    order_id: u64,
}

#[subscriber(GooglePubSub::new("orders-receipts").create_with_topic("orders"), reply)]
async fn receipt(order: &Order) -> Receipt {
    tracing::info!(order.id, "receipt sent");
    Receipt { order_id: order.id }
}
// --8<-- [end:reply]

// --8<-- [start:slot]
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Invoice {
    order_id: u64,
    amount_cents: u64,
}

/// Publishes the invoice of an order to the topic the call names.
#[subscriber(GooglePubSub::new("orders-billing").create_with_topic("orders"))]
async fn bill(order: &Order, Out(out): Out<impl Publisher>) -> HandlerOutcome {
    let invoice = Invoice {
        order_id: order.id,
        amount_cents: order.amount_cents,
    };
    match out.message(&invoice).to("invoices").publish().await {
        Ok(()) => HandlerOutcome::ack(),
        Err(err) => {
            tracing::warn!(order.id, error = %err, "invoice not published, order retried");
            HandlerOutcome::retry()
        }
    }
}
// --8<-- [end:slot]

/// What the service announces once its subscriptions are open.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "services")]
struct Started {
    service: String,
}

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        PubSubBroker::new("my-project").emulator("localhost:8085"),
        |b| {
            b.include(receipt).out_reply(Publish::default());
            b.include(bill).out(DefaultSlot, Publish::default()).build();
            b.after_startup(Publish::default(), async move |publisher| {
                publisher
                    .message(&Started {
                        service: "orders".to_owned(),
                    })
                    .publish()
                    .await
            });
        },
    )
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_gcp_pubsub::prelude::*;

    use super::{Invoice, Order, Receipt, Started, app};

    #[tokio::test]
    async fn an_order_gets_its_receipt_and_its_invoice() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<PubSubBroker>()
            .message(&Order {
                id: 7,
                amount_cents: 1250,
            })
            .publish()
            .await?;

        tb.broker::<PubSubBroker>()
            .published::<Receipt>("receipts")
            .assert_called_once()
            .with(&Receipt { order_id: 7 });
        tb.broker::<PubSubBroker>()
            .published::<Invoice>("invoices")
            .assert_called_once()
            .with(&Invoice {
                order_id: 7,
                amount_cents: 1250,
            });
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn the_service_announces_itself_once_it_is_up() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<PubSubBroker>()
            .published::<Started>("services")
            .assert_called_once()
            .with(&Started {
                service: "orders".to_owned(),
            });
        tb.shutdown().await?;
        Ok(())
    }
}
