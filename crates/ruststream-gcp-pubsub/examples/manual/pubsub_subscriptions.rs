//! The service of `examples/pubsub_subscriptions.rs` written without the macros: a name in
//! `subscriber(..)` reads that existing subscription.
//!
//! ```text
//! cargo test --example manual_pubsub_subscriptions --features testing
//! cargo run --example manual_pubsub_subscriptions --features testing -- run
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

// --8<-- [start:name]
/// Reads the subscription `orders-workers`, which the deployment created on its topic.
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

fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        PubSubBroker::new("my-project"),
        |b| {
            b.include(subscriber("orders-workers", Place).build());
        },
    )
}
// --8<-- [end:name]

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

    #[tokio::test]
    async fn the_subscription_reads_its_topic() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        // In process, a subscription the service only names is attached to the topic of its own
        // name.
        tb.broker::<PubSubBroker>()
            .message(&Order { id: 7 })
            .to("orders-workers")
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
    async fn another_topic_is_not_read() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<PubSubBroker>()
            .message(&Order { id: 8 })
            .to("refunds")
            .publish()
            .await?;

        tb.broker::<PubSubBroker>()
            .subscriber("orders-workers")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }
}
