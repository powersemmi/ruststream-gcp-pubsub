//! Headers: a message's headers travel as Pub/Sub attributes, and a delivery's attributes are its
//! headers.
//!
//! ```text
//! cargo test --example pubsub_publishing_headers --features testing
//! cargo run --example pubsub_publishing_headers --features testing -- run
//! ```

use ruststream_gcp_pubsub::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "shipments")]
struct Shipment {
    order_id: u64,
}

// --8<-- [start:handler]
/// The tenant arrives as an attribute of the order and leaves as an attribute of the shipment.
#[subscriber("orders-workers")]
async fn ship(
    order: &Order,
    ctx: &mut Context<'_>,
    Out(out): Out<impl Publisher>,
) -> HandlerOutcome {
    let mut headers = HeaderMap::new();
    if let Some(tenant) = ctx.headers().get_str("tenant") {
        headers.insert("tenant", tenant.to_owned());
    }
    let shipment = Shipment { order_id: order.id };
    match out.message(&shipment).with_headers(headers).publish().await {
        Ok(()) => HandlerOutcome::ack(),
        Err(_) => HandlerOutcome::retry(),
    }
}
// --8<-- [end:handler]

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
