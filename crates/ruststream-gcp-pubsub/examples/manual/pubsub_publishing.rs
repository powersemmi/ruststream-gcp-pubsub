//! The service of `examples/pubsub_publishing.rs` written without the macros: the reply is the
//! body's return type, the slot an entry of its `Outs`, and the policies are the same steps of the
//! mount chain.
//!
//! ```text
//! cargo test --example manual_pubsub_publishing --features testing
//! cargo run --example manual_pubsub_publishing --features testing -- run
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
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "receipts")]
struct Receipt {
    order_id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Invoice {
    order_id: u64,
}

// --8<-- [start:reply]
/// The receipt goes to `receipts`, the topic its type names.
struct ReceiptFor;

impl Handle<Order, Receipt> for ReceiptFor {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Receipt, HandlerOutcome>> {
        ready(Ok(Receipt { order_id: order.id }))
    }
}
// --8<-- [end:reply]

// --8<-- [start:slot]
/// The invoice goes to the topic the call names.
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
        let invoice = Invoice { order_id: order.id };
        let sent = outs
            .get(DefaultSlot)
            .message(&invoice)
            .to("invoices")
            .publish()
            .await;
        if let Err(err) = sent {
            tracing::warn!(order.id, error = %err, "invoice not published");
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
}
// --8<-- [end:slot]

// --8<-- [start:mount]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        PubSubBroker::new("my-project"),
        |b| {
            b.include(
                subscriber("orders-receipts", ReceiptFor)
                    .map_source(|_| {
                        GooglePubSub::new("orders-receipts").create_with_topic("orders")
                    })
                    .reply()
                    .build(),
            )
            .out_reply(Publish::default());
            b.include(
                subscriber("orders-billing", Bill)
                    .map_source(|_| GooglePubSub::new("orders-billing").create_with_topic("orders"))
                    .build(),
            )
            .out(DefaultSlot, Publish::default())
            .build();
        },
    )
}
// --8<-- [end:mount]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

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
