//! Ordered publishing: messages sharing an ordering key are delivered in publish order.
//!
//! The mount site fixes the key a whole run of publishes belongs to, a handler that sends one
//! message per order names the key of that order, and a reply takes its key from the delivery it
//! answers.
//!
//! Run the emulator first (`just brokers-up`), then:
//! `cargo run --example pubsub_ordered_publish -- run`

use std::io;

// `Outgoing` names the derive at the crate root and the publish pipeline's message type in
// `runtime`; a publish transform takes the second one.
use ruststream::runtime::{Outgoing, PublishContext};
use ruststream_gcp_pubsub::PARTITION_KEY_HEADER;
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

/// The receipt an order gets back. It always goes to the same topic, so the type says so once.
#[derive(Serialize, Outgoing)]
#[outgoing(name = "receipts")]
struct Receipt {
    order_id: u64,
}

/// Answers an order with its receipt.
#[subscriber("orders-receipts", publish)]
async fn receipt(order: &Order) -> Receipt {
    Receipt { order_id: order.id }
}

// --8<-- [start:reply_key]
/// Sends each receipt under the key of the order it answers: a delivery reports its ordering key
/// as the partition key, and the reply goes out under the same one, so one order's receipts stay
/// ordered among themselves.
///
/// A transform that writes a setting names the settings type it writes, which is how it mounts
/// over a Pub/Sub publisher and over no other broker's.
struct ReplyUnderTheOrdersKey;

impl<C> PublishTransform<ForReply<C>, PubSubPublishOptions> for ReplyUnderTheOrdersKey {
    type Destination = Reads;

    fn apply(
        &self,
        _out: &mut Outgoing<'_>,
        options: &mut Option<PubSubPublishOptions>,
        cx: &PublishContext<'_, C>,
    ) {
        if let Some(key) = cx.headers().get_str(PARTITION_KEY_HEADER) {
            options
                .get_or_insert_with(PubSubPublishOptions::default)
                .ordering_key = Some(key.to_owned());
        }
    }
}
// --8<-- [end:reply_key]

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("order-events", "0.1.0")).with_broker(
        PubSubBroker::new("my-project").emulator("localhost:8085"),
        |b| {
            b.include(confirm)
                .out(DefaultSlot, Publish::default())
                .build();

            b.include(receipt)
                .out_reply(Publish::default())
                .transform(ReplyUnderTheOrdersKey);

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
