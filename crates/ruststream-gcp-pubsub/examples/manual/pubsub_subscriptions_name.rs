//! The service of `examples/pubsub_subscriptions_name.rs` written without the macros: the
//! definition starts from an unnamed `GooglePubSub`, and `.name(..)` names it at the mount site.
//!
//! ```text
//! cargo test --example manual_pubsub_subscriptions_name --features testing
//! cargo run --example manual_pubsub_subscriptions_name --features testing -- run
//! ```

use std::env;
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
/// A Pub/Sub subscription whose name the deployment decides.
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

/// One service per region, each reading the subscription of its region.
fn service(subscription: String) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        PubSubBroker::new("my-project"),
        |b| {
            b.include(
                subscriber(Unnamed::<GooglePubSub>::new(), Place)
                    .name(subscription)
                    .build(),
            );
        },
    )
}
// --8<-- [end:name]

fn app() -> impl App<State = ()> {
    let subscription = env::var("ORDERS_SUBSCRIPTION").unwrap_or_else(|_| "orders-workers".into());
    service(subscription)
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

    use super::{Order, service};

    #[tokio::test]
    async fn the_named_subscription_is_read() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service("orders-eu-workers".to_owned())).await?;

        tb.broker::<PubSubBroker>()
            .message(&Order { id: 7 })
            .to("orders-eu-workers")
            .publish()
            .await?;

        tb.broker::<PubSubBroker>()
            .subscriber("orders-eu-workers")
            .assert_called_once()
            .with(&Order { id: 7 })
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }
}
