//! The service of `examples/pubsub_publishing_ordering_policy.rs` written without the macros:
//! the policy is the same step of the mount chain.
//!
//! ```text
//! cargo test --example manual_pubsub_publishing_ordering_policy --features testing
//! cargo run --example manual_pubsub_publishing_ordering_policy --features testing -- run
//! ```

use std::future::{Future, ready};
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
#[outgoing(name = "ledger")]
struct LedgerEntry {
    order_id: u64,
}

// --8<-- [start:policy]
/// Books every order in the ledger.
struct Book;

impl Handle<Order, LedgerEntry> for Book {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<LedgerEntry, HandlerOutcome>> {
        ready(Ok(LedgerEntry { order_id: order.id }))
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        PubSubBroker::new("my-project"),
        |b| {
            // The ledger is one sequence, so every entry goes out under one key.
            b.include(subscriber("orders-workers", Book).reply().build())
                .out_reply(Publish::default().ordering_key("ledger"));
        },
    )
}
// --8<-- [end:policy]

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

    use super::{LedgerEntry, Order, app};

    #[tokio::test]
    async fn every_entry_goes_out_under_the_policys_key() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        for id in [7, 8] {
            tb.broker::<PubSubBroker>()
                .message(&Order { id })
                .to("orders-workers")
                .publish()
                .await?;
        }

        tb.broker::<PubSubBroker>()
            .published::<LedgerEntry>("ledger")
            .assert_called(2)
            .with_header(PARTITION_KEY_HEADER, "ledger");
        Ok(())
    }
}
