//! The service of `examples/pubsub_publishing_ordering_transform.rs` written without the macros:
//! the transform is the same type, mounted with the same step.
//!
//! ```text
//! cargo test --example manual_pubsub_publishing_ordering_transform --features testing
//! cargo run --example manual_pubsub_publishing_ordering_transform --features testing -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
// `Outgoing` names the derive at the crate root and the publish pipeline's message type in
// `runtime`; a publish transform takes the second one.
use ruststream::runtime::{Outgoing, PublishContext};
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
#[outgoing(name = "receipts")]
struct Receipt {
    order_id: u64,
}

/// Answers an order with its receipt.
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

// --8<-- [start:transform]
/// Sends each receipt under the key of the order it answers, which the delivery reports in its
/// `partition-key` header.
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

fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        PubSubBroker::new("my-project"),
        |b| {
            b.include(subscriber("orders-workers", ReceiptFor).reply().build())
                .out_reply(Publish::default())
                .transform(ReplyUnderTheOrdersKey);
        },
    )
}
// --8<-- [end:transform]

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

    use super::{Order, Receipt, app};

    #[tokio::test]
    async fn the_receipt_goes_out_under_the_orders_key() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let mut headers = HeaderMap::new();
        headers.insert(PARTITION_KEY_HEADER, "order-7");
        tb.broker::<PubSubBroker>()
            .message(&Order { id: 7 })
            .with_headers(headers)
            .to("orders-workers")
            .publish()
            .await?;

        tb.broker::<PubSubBroker>()
            .published::<Receipt>("receipts")
            .assert_called_once()
            .with(&Receipt { order_id: 7 })
            .with_options(&PubSubPublishOptions {
                ordering_key: Some("order-7".to_owned()),
            });
        Ok(())
    }
}
