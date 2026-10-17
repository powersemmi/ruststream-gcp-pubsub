//! The service of `examples/pubsub_broker_startup.rs` without the macros: the handler is a named
//! type, `subscriber(..)` takes the descriptor, and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_pubsub_broker_startup --features testing
//! cargo run --example manual_pubsub_broker_startup --features testing -- run
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

// --8<-- [start:app]
struct Place;

impl Handle<Order> for Place {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, "order placed");
        ready(Ok(()))
    }
}

/// The subscription `orders-workers` on the topic `orders`, both created when they are missing.
fn orders() -> GooglePubSub {
    GooglePubSub::new("orders-workers").create_with_topic("orders")
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        PubSubBroker::new("my-project"),
        |b| {
            // Before the pull opens: the topics `orders` and `orders-dead`, then the
            // subscription with its dead-letter policy.
            b.include(subscriber(orders(), Place).build())
                .max_attempts(nonzero!(5u32))
                .dead_letter("orders-dead");
        },
    )
}
// --8<-- [end:app]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_gcp_pubsub::prelude::*;

    use super::{Order, Place, app, orders};

    #[tokio::test]
    async fn the_created_subscription_reads_its_topic() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<PubSubBroker>()
            .message(&Order { id: 7 })
            .to("orders")
            .publish()
            .await?;

        tb.broker::<PubSubBroker>()
            .subscriber("orders-workers")
            .assert_called_once()
            .with(&Order { id: 7 })
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_cap_pubsub_does_not_accept_stops_the_startup() {
        let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
            PubSubBroker::new("my-project"),
            |b| {
                b.include(subscriber(orders(), Place).build())
                    .max_attempts(nonzero!(3u32))
                    .dead_letter("orders-dead");
            },
        );

        let failed = TestApp::start(app).await.err();
        let failed = format!("{:#}", failed.expect("startup took a cap of three"));
        assert!(failed.contains("max_attempts(3)"), "{failed}");
    }
}
