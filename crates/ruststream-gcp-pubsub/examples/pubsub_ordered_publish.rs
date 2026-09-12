//! Ordered publishing: messages sharing an ordering key are delivered in publish order.
//!
//! The mount site fixes the key a whole run of publishes belongs to, and a handler that sends one
//! message per order names the key of that order.
//!
//! Run the emulator first (`just brokers-up`), then:
//! `cargo run --example pubsub_ordered_publish -- run`

use std::io;

use ruststream_gcp_pubsub::prelude::*;
use serde::{Deserialize, Serialize};

/// The model the seed publishes. It declares no destination, so each publish names the topic.
#[derive(Serialize, Outgoing)]
struct OrderStep {
    step: &'static str,
}

/// What the handler consumes and forwards.
#[derive(Debug, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

// --8<-- [start:handler]
/// Each order's confirmation goes out under that order's own key, so a body that names the step
/// imports this crate's prelude and bounds its slot on the crate's settings type.
#[subscriber("orders-workers")]
async fn confirm(
    order: &Order,
    Out(out): Out<impl Publisher<Options = PubSubPublishOptions>>,
) -> HandlerOutcome {
    if out
        .message(order)
        .to("confirmations")
        .ordering_key(format!("order-{}", order.id))
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}
// --8<-- [end:handler]

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("order-events", "0.1.0")).with_broker(
        PubSubBroker::new("my-project").emulator("localhost:8085"),
        |b| {
            b.include(confirm)
                .out(DefaultSlot, Publish::default())
                .build();

            // The scope's after_startup is the home of a first publish: the publisher arrives
            // already paired with the connected broker, so the seed cannot race the connect.
            // --8<-- [start:ordered]
            // Every message this publisher sends belongs to one order, so the key is named once.
            b.after_startup(
                Publish::default().ordering_key("order-42"),
                async move |publisher| -> io::Result<()> {
                    for step in ["created", "paid", "shipped"] {
                        publisher
                            .message(&OrderStep { step })
                            .to("orders")
                            .publish()
                            .await
                            .map_err(io::Error::other)?;
                    }
                    Ok(())
                },
            );
            // --8<-- [end:ordered]
        },
    )
}
