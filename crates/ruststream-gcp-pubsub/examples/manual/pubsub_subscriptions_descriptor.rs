//! The service of `examples/pubsub_subscriptions_descriptor.rs` written without the macros: the
//! handler mounts on the descriptor itself, `subscriber(descriptor, body)`.
//!
//! ```text
//! cargo test --example manual_pubsub_subscriptions_descriptor --features testing
//! cargo run --example manual_pubsub_subscriptions_descriptor --features testing -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_gcp_pubsub::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

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

struct Audit;

impl Handle<Order> for Audit {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, "order audited");
        ready(Ok(()))
    }
}

// --8<-- [start:create-with-topic]
// --8<-- [start:descriptor]
/// The subscription `orders-workers` on the topic `orders`, with at most 100 orders unsettled.
fn orders() -> GooglePubSub {
    GooglePubSub::new("orders-workers")
        .create_with_topic("orders")
        .max_outstanding(100)
}
// --8<-- [end:descriptor]

/// A second subscription on the same topic: it receives every order too.
fn audit() -> GooglePubSub {
    GooglePubSub::new("orders-audit").create_with_topic("orders")
}
// --8<-- [end:create-with-topic]

fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        PubSubBroker::new("my-project"),
        |b| {
            b.include(subscriber(orders(), Place).build());
            b.include(subscriber(audit(), Audit).build());
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
    use ruststream_gcp_pubsub::prelude::*;

    use super::{Order, app};

    #[tokio::test]
    async fn every_subscription_of_the_topic_receives_the_order() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<PubSubBroker>()
            .message(&Order { id: 7 })
            .publish()
            .await?;

        for subscription in ["orders-workers", "orders-audit"] {
            tb.broker::<PubSubBroker>()
                .subscriber(subscription)
                .assert_called_once()
                .with(&Order { id: 7 })
                .settled(HandlerOutcome::ack());
        }
        tb.shutdown().await?;
        Ok(())
    }
}
