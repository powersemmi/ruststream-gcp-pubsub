//! The service of `examples/pubsub_subscriptions_batches.rs` written without the macros: the
//! body implements `Handle<[Order]>`, and `batch(n)` is a step of its chain.
//!
//! ```text
//! cargo test --example manual_pubsub_subscriptions_batches --features testing
//! cargo run --example manual_pubsub_subscriptions_batches --features testing -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;
use std::time::Duration;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_gcp_pubsub::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
}

// --8<-- [start:batches]
/// The slice makes this a batch handler; a partial batch waits 200 milliseconds for the rest.
struct Settle;

impl Handle<[Order]> for Settle {
    fn handle(
        &self,
        orders: &[Order],
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), Vec<HandlerOutcome>>> {
        tracing::info!(count = orders.len(), "orders settled");
        ready(Ok(()))
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        PubSubBroker::new("my-project"),
        |b| {
            // At most 50 orders reach the handler per call.
            b.include(
                subscriber("orders-workers", Settle)
                    .map_source(|_| {
                        GooglePubSub::new("orders-workers").batch_wait(Duration::from_millis(200))
                    })
                    .batch(nonzero!(50))
                    .build(),
            );
        },
    )
}
// --8<-- [end:batches]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_gcp_pubsub::prelude::*;

    use super::{Order, app};

    #[tokio::test(start_paused = true)]
    async fn orders_published_together_meet_in_one_batch() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        // Published together, the three orders meet in one batch.
        let broker = tb.broker::<PubSubBroker>();
        let (first, second, third) = tokio::join!(
            broker
                .message(&Order { id: 1 })
                .to("orders-workers")
                .publish(),
            broker
                .message(&Order { id: 2 })
                .to("orders-workers")
                .publish(),
            broker
                .message(&Order { id: 3 })
                .to("orders-workers")
                .publish(),
        );
        first?;
        second?;
        third?;

        let batches = tb
            .broker::<PubSubBroker>()
            .subscriber("orders-workers")
            .assert_batch_sizes(&[3])
            .batches::<Order>();
        let orders: Vec<Order> = batches.into_iter().flatten().collect();
        assert_eq!(orders, [Order { id: 1 }, Order { id: 2 }, Order { id: 3 }]);
        tb.shutdown().await?;
        Ok(())
    }
}
