//! The `ordering_key` step: one publish names the key it is ordered under.
//!
//! ```text
//! cargo test --example pubsub_publishing_ordering_key --features testing
//! cargo run --example pubsub_publishing_ordering_key --features testing -- run
//! ```

use ruststream_gcp_pubsub::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Confirmation {
    order_id: u64,
}

// --8<-- [start:ordering-key]
/// Each confirmation goes out under its order's key, named on the call.
#[subscriber("orders-workers")]
async fn confirm(
    order: &Order,
    Out(out): Out<impl Publisher<Options = PubSubPublishOptions>>,
) -> HandlerOutcome {
    let sent = out
        .message(&Confirmation { order_id: order.id })
        .to("confirmations")
        .ordering_key(format!("order-{}", order.id))
        .publish()
        .await;
    match sent {
        Ok(()) => HandlerOutcome::ack(),
        Err(_) => HandlerOutcome::retry(),
    }
}
// --8<-- [end:ordering-key]

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        PubSubBroker::new("my-project"),
        |b| {
            b.include(confirm)
                .out(DefaultSlot, Publish::default())
                .build();
        },
    )
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_gcp_pubsub::PARTITION_KEY_HEADER;
    use ruststream_gcp_pubsub::prelude::*;

    use super::{Confirmation, Order, app};

    #[tokio::test]
    async fn the_call_names_the_key() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<PubSubBroker>()
            .message(&Order { id: 7 })
            .to("orders-workers")
            .publish()
            .await?;

        tb.out::<DefaultSlot>()
            .assert_called_once()
            .with_options(&PubSubPublishOptions {
                ordering_key: Some("order-7".to_owned()),
            });
        // A delivery of the confirmation reports the key in its `partition-key` header.
        tb.broker::<PubSubBroker>()
            .published::<Confirmation>("confirmations")
            .assert_called_once()
            .with(&Confirmation { order_id: 7 })
            .with_header(PARTITION_KEY_HEADER, "order-7");
        Ok(())
    }
}
