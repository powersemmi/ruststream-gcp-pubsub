//! The service of `examples/pubsub_publishing_ordering_key.rs` written without the macros: the
//! slot's entry is bounded on the crate's settings type, and the step is the same.
//!
//! ```text
//! cargo test --example manual_pubsub_publishing_ordering_key --features testing
//! cargo run --example manual_pubsub_publishing_ordering_key --features testing -- run
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
struct Confirmation {
    order_id: u64,
}

// --8<-- [start:ordering-key]
/// Each confirmation goes out under its order's key, named on the call.
struct Confirm;

impl<Entry> Handle<Order, (), Outs<(Entry,)>> for Confirm
where
    Entry: OutEntry<DefaultSlot, Wire: Publisher<Options = PubSubPublishOptions>>,
{
    async fn handle(
        &self,
        order: &Order,
        outs: &Outs<(Entry,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let sent = outs
            .get(DefaultSlot)
            .message(&Confirmation { order_id: order.id })
            .to("confirmations")
            .ordering_key(format!("order-{}", order.id))
            .publish()
            .await;
        sent.map_err(|_| HandlerOutcome::retry())
    }
}
// --8<-- [end:ordering-key]

fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        PubSubBroker::new("my-project"),
        |b| {
            b.include(subscriber("orders-workers", Confirm).build())
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
