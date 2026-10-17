//! The orders service of the Publishing and replies guide, written without the macros: the reply
//! is the body's return type, the slot an entry of its `Outs`, and the policies are the same
//! steps of the mount chain.
//!
//! ```text
//! just brokers-up
//! cargo run --example manual_guide_publishing --features testing,ruststream/logging -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_gcp_pubsub::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
    amount_cents: u64,
}

// --8<-- [start:reply]
/// The receipt of an order; its type names the topic it goes to.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "receipts")]
struct Receipt {
    order_id: u64,
}

struct ReceiptFor;

impl Handle<Order, Receipt> for ReceiptFor {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Receipt, HandlerOutcome>> {
        tracing::info!(order.id, "receipt sent");
        ready(Ok(Receipt { order_id: order.id }))
    }
}
// --8<-- [end:reply]

// --8<-- [start:slot]
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Invoice {
    order_id: u64,
    amount_cents: u64,
}

/// Publishes the invoice of an order to the topic the call names.
struct Bill;

impl<Entry> Handle<Order, (), Outs<(Entry,)>> for Bill
where
    Entry: OutEntry<DefaultSlot, Wire: Publisher>,
{
    async fn handle(
        &self,
        order: &Order,
        outs: &Outs<(Entry,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let invoice = Invoice {
            order_id: order.id,
            amount_cents: order.amount_cents,
        };
        let sent = outs
            .get(DefaultSlot)
            .message(&invoice)
            .to("invoices")
            .publish()
            .await;
        if let Err(err) = sent {
            tracing::warn!(order.id, error = %err, "invoice not published, order retried");
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
}
// --8<-- [end:slot]

/// What the service announces once its subscriptions are open.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "services")]
struct Started {
    service: String,
}

// --8<-- [start:app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        PubSubBroker::new("my-project").emulator("localhost:8085"),
        |b| {
            b.include(
                subscriber(
                    GooglePubSub::new("orders-receipts").create_with_topic("orders"),
                    ReceiptFor,
                )
                .reply()
                .build(),
            )
            .out_reply(Publish::default());
            b.include(
                subscriber(
                    GooglePubSub::new("orders-billing").create_with_topic("orders"),
                    Bill,
                )
                .build(),
            )
            .out(DefaultSlot, Publish::default())
            .build();
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

fn main() -> ExitCode {
    run_main(app)
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
