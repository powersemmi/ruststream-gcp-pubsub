//! The service of `examples/pubsub_publishing_headers.rs` written without the macros: the body
//! reads the delivery's headers through its `Context`.
//!
//! ```text
//! cargo test --example manual_pubsub_publishing_headers --features testing
//! cargo run --example manual_pubsub_publishing_headers --features testing -- run
//! ```

use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_gcp_pubsub::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "shipments")]
struct Shipment {
    order_id: u64,
}

// --8<-- [start:handler]
/// The tenant arrives as an attribute of the order and leaves as an attribute of the shipment.
struct Ship;

impl<Entry> Handle<Order, (), Outs<(Entry,)>> for Ship
where
    Entry: OutEntry<DefaultSlot, Wire: Publisher>,
{
    async fn handle(
        &self,
        order: &Order,
        outs: &Outs<(Entry,)>,
        ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let mut headers = HeaderMap::new();
        if let Some(tenant) = ctx.headers().get_str("tenant") {
            headers.insert("tenant", tenant.to_owned());
        }
        let shipment = Shipment { order_id: order.id };
        let sent = outs
            .get(DefaultSlot)
            .message(&shipment)
            .with_headers(headers)
            .publish()
            .await;
        sent.map_err(|_| HandlerOutcome::retry())
    }
}
// --8<-- [end:handler]

fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        PubSubBroker::new("my-project"),
        |b| {
            b.include(subscriber("orders-workers", Ship).build())
                .out(DefaultSlot, Publish::default())
                .build();
        },
    )
}

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_gcp_pubsub::prelude::*;

    use super::{Order, Shipment, app};

    #[tokio::test]
    async fn the_tenant_travels_on() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let mut headers = HeaderMap::new();
        headers.insert("tenant", "acme");
        tb.broker::<PubSubBroker>()
            .message(&Order { id: 7 })
            .with_headers(headers)
            .to("orders-workers")
            .publish()
            .await?;

        tb.broker::<PubSubBroker>()
            .published::<Shipment>("shipments")
            .assert_called_once()
            .with(&Shipment { order_id: 7 })
            .with_header("tenant", "acme");
        Ok(())
    }
}
