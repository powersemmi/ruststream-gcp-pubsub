//! The service of `examples/pubsub_publishing_ordering_header.rs` written without the macros:
//! the body sets the header on its slot's publish.
//!
//! ```text
//! cargo test --example manual_pubsub_publishing_ordering_header --features testing
//! cargo run --example manual_pubsub_publishing_ordering_header --features testing -- run
//! ```

use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_gcp_pubsub::PARTITION_KEY_HEADER;
use ruststream_gcp_pubsub::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Shipment {
    order_id: u64,
}

// --8<-- [start:partition-key]
/// The body names no broker type: the `partition-key` header orders the shipment.
struct Ship;

impl<Entry> Handle<Order, (), Outs<(Entry,)>> for Ship
where
    Entry: OutEntry<DefaultSlot, Wire: Publisher>,
{
    async fn handle(
        &self,
        order: &Order,
        outs: &Outs<(Entry,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let mut headers = HeaderMap::new();
        headers.insert(PARTITION_KEY_HEADER, format!("order-{}", order.id));
        let shipment = Shipment { order_id: order.id };
        let sent = outs
            .get(DefaultSlot)
            .message(&shipment)
            .with_headers(headers)
            .to("shipments")
            .publish()
            .await;
        sent.map_err(|_| HandlerOutcome::retry())
    }
}
// --8<-- [end:partition-key]

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
