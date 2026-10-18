//! The orders service of the Topics and subscriptions guide, written without the macros: each
//! handler is a type with an `impl Handle`, mounted on its subscription with the `subscriber`
//! constructor.
//!
//! ```text
//! just brokers-up
//! cargo run --example manual_guide_topics_and_subscriptions --features testing,ruststream/logging -- run
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
    amount_cents: u64,
}

// --8<-- [start:fan-out]
/// Bills an order; mounted on billing's own subscription, it receives every order.
struct Bill;

impl Handle<Order> for Bill {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, order.amount_cents, "order billed");
        ready(Ok(()))
    }
}

/// Ships an order; mounted on shipping's own subscription, it receives every order too.
struct Ship;

impl Handle<Order> for Ship {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, "order shipped");
        ready(Ok(()))
    }
}
// --8<-- [end:fan-out]

// --8<-- [start:existing]
/// Audits an order; mounted on a subscription operations created.
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

/// Reports an order; mounted on the subscription the deployment names.
struct Report;

impl Handle<Order> for Report {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, "order reported");
        ready(Ok(()))
    }
}
// --8<-- [end:existing]

// --8<-- [start:app]
fn app() -> impl App {
    let reports = env::var("REPORTS_SUBSCRIPTION").unwrap_or_else(|_| "orders-reports".to_owned());
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        PubSubBroker::new("my-project").emulator("localhost:8085"),
        |b| {
            b.include(
                subscriber(
                    GooglePubSub::new("orders-billing").create_with_topic("orders"),
                    Bill,
                )
                .build(),
            );
            b.include(
                subscriber(
                    GooglePubSub::new("orders-shipping").create_with_topic("orders"),
                    Ship,
                )
                .build(),
            );
            b.include(subscriber("orders-audit", Audit).build());
            b.include(subscriber(GooglePubSub::new(reports), Report).build());
        },
    )
}

fn main() -> ExitCode {
    run_main(app)
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_gcp_pubsub::prelude::*;

    use super::{Order, app};

    #[tokio::test]
    async fn every_subscription_of_the_topic_receives_the_order() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let order = Order {
            id: 7,
            amount_cents: 1250,
        };

        tb.broker::<PubSubBroker>()
            .message(&order)
            .to("orders")
            .publish()
            .await?;

        for subscription in ["orders-billing", "orders-shipping"] {
            tb.broker::<PubSubBroker>()
                .subscriber(subscription)
                .assert_called_once()
                .with(&order)
                .settled(HandlerOutcome::ack());
        }
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn the_subscriptions_operations_own_are_read_by_name() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let order = Order {
            id: 8,
            amount_cents: 990,
        };

        // In process, a subscription the service only names reads the topic of its own name.
        for subscription in ["orders-audit", "orders-reports"] {
            tb.broker::<PubSubBroker>()
                .message(&order)
                .to(subscription)
                .publish()
                .await?;
            tb.broker::<PubSubBroker>()
                .subscriber(subscription)
                .assert_called_once()
                .with(&order)
                .settled(HandlerOutcome::ack());
        }
        tb.shutdown().await?;
        Ok(())
    }
}
