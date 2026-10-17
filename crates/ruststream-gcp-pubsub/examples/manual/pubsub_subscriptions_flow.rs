//! The service of `examples/pubsub_subscriptions_flow.rs` written without the macros: each
//! descriptor joins its chain through `map_source`.
//!
//! ```text
//! cargo test --example manual_pubsub_subscriptions_flow --features testing
//! just brokers-up
//! PUBSUB_TEST_HOST=127.0.0.1:8085 cargo test --example manual_pubsub_subscriptions_flow --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::future::{Future, ready};
use std::process::ExitCode;
use std::time::Duration;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_gcp_pubsub::prelude::*;
use serde::{Deserialize, Serialize};

#[cfg(test)]
#[path = "../support/pubsub_live.rs"]
mod live;

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Report {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Invoice {
    id: u64,
    ready: bool,
}

struct Index;

impl Handle<Order> for Index {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, "order indexed");
        ready(Ok(()))
    }
}

// --8<-- [start:max-outstanding]
/// At most 50 orders are unsettled at once; the service holds the rest back.
fn orders() -> GooglePubSub {
    GooglePubSub::new("flow-orders-workers")
        .create_with_topic("flow-orders")
        .max_outstanding(50)
}
// --8<-- [end:max-outstanding]

struct Render;

impl Handle<Report> for Render {
    fn handle(
        &self,
        report: &Report,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(report.id, "report rendered");
        ready(Ok(()))
    }
}

// --8<-- [start:ack-extension]
/// A report takes minutes to render, so each extension of its lease reaches five minutes.
fn reports() -> GooglePubSub {
    GooglePubSub::new("flow-reports-workers")
        .create_with_topic("flow-reports")
        .ack_extension(Duration::from_mins(5))
}
// --8<-- [end:ack-extension]

/// How long an invoice that is not ready waits before it comes back.
const NOT_READY: Duration = Duration::from_mins(90);

// --8<-- [start:max-lease]
/// An invoice that is not ready comes back in ninety minutes, inside a lease of two hours.
struct Send;

impl Handle<Invoice> for Send {
    fn handle(
        &self,
        invoice: &Invoice,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        if !invoice.ready {
            return ready(Err(HandlerOutcome::retry_after(NOT_READY)));
        }
        tracing::info!(invoice.id, "invoice sent");
        ready(Ok(()))
    }
}

fn invoices() -> GooglePubSub {
    GooglePubSub::new("flow-invoices-workers")
        .create_with_topic("flow-invoices")
        .max_lease(Duration::from_hours(2))
}
// --8<-- [end:max-lease]

fn service(broker: PubSubBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(broker, |b| {
        b.include(
            subscriber("flow-orders-workers", Index)
                .map_source(|_| orders())
                .build(),
        );
        b.include(
            subscriber("flow-reports-workers", Render)
                .map_source(|_| reports())
                .build(),
        );
        b.include(
            subscriber("flow-invoices-workers", Send)
                .map_source(|_| invoices())
                .build(),
        );
    })
}

fn app() -> impl App<State = ()> {
    service(PubSubBroker::new("my-project"))
}

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::future::{Future, ready};
    use std::time::Duration;

    use ruststream::testing::TestApp;
    use ruststream_gcp_pubsub::prelude::*;

    use super::{Invoice, NOT_READY, Order, Report, app, invoices, live, service};

    /// The body both modes run: each subscription opens with its settings and handles its message.
    async fn every_subscription_handles_its_message(tb: TestApp<()>) -> Result<(), Box<dyn Error>> {
        tb.broker::<PubSubBroker>()
            .message(&Order { id: 1 })
            .to("flow-orders")
            .publish()
            .await?;
        tb.broker::<PubSubBroker>()
            .message(&Report { id: 2 })
            .to("flow-reports")
            .publish()
            .await?;
        tb.broker::<PubSubBroker>()
            .message(&Invoice { id: 3, ready: true })
            .to("flow-invoices")
            .publish()
            .await?;

        for subscription in [
            "flow-orders-workers",
            "flow-reports-workers",
            "flow-invoices-workers",
        ] {
            tb.broker::<PubSubBroker>()
                .subscriber(subscription)
                .assert_called_once()
                .settled(HandlerOutcome::ack());
        }
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn in_process() -> Result<(), Box<dyn Error>> {
        every_subscription_handles_its_message(TestApp::start(app()).await?).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn against_the_emulator() -> Result<(), Box<dyn Error>> {
        let Some(host) = live::host() else {
            return Ok(());
        };
        live::fresh(
            &host,
            &[
                "flow-orders-workers",
                "flow-reports-workers",
                "flow-invoices-workers",
            ],
        )
        .await?;
        let broker = PubSubBroker::new("my-project").emulator(host);
        every_subscription_handles_its_message(TestApp::start_live(service(broker)).await?).await
    }

    #[tokio::test(start_paused = true)]
    async fn an_invoice_that_is_not_ready_comes_back_inside_the_lease() -> Result<(), Box<dyn Error>>
    {
        let tb = TestApp::start(app()).await?;
        tb.broker::<PubSubBroker>()
            .message(&Invoice {
                id: 4,
                ready: false,
            })
            .to("flow-invoices")
            .publish()
            .await?;

        tb.advance(Duration::from_mins(89)).await?;
        tb.broker::<PubSubBroker>()
            .subscriber("flow-invoices-workers")
            .assert_called_once()
            .settled(HandlerOutcome::retry_after(NOT_READY));

        tb.advance(Duration::from_mins(1)).await?;
        tb.broker::<PubSubBroker>()
            .subscriber("flow-invoices-workers")
            .assert_called(2);
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_delay_past_the_lease_comes_back_at_once() -> Result<(), Box<dyn Error>> {
        /// Asks for three hours on a subscription that leases a delivery for two.
        struct TooLate;

        impl Handle<Invoice> for TooLate {
            fn handle(
                &self,
                invoice: &Invoice,
                _outs: &(),
                _ctx: &mut Context<'_>,
            ) -> impl Future<Output = Result<(), HandlerOutcome>> {
                let _ = invoice.id;
                ready(Err(HandlerOutcome::retry_after(Duration::from_hours(3))))
            }
        }
        let app = RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(
            PubSubBroker::new("my-project"),
            |b| {
                // The cap ends the redeliveries, so the test sees how many came back.
                b.include(
                    subscriber("flow-invoices-workers", TooLate)
                        .map_source(|_| invoices())
                        .build(),
                )
                .max_attempts(nonzero!(5u32))
                .dead_letter("flow-invoices-dead");
            },
        );
        let tb = TestApp::start(app).await?;
        tb.broker::<PubSubBroker>()
            .message(&Invoice {
                id: 5,
                ready: false,
            })
            .to("flow-invoices")
            .publish()
            .await?;

        // No time passed: the refused delay rejected each delivery on the spot.
        tb.broker::<PubSubBroker>()
            .subscriber("flow-invoices-workers")
            .assert_called(5);
        tb.broker::<PubSubBroker>()
            .published::<Invoice>("flow-invoices-dead")
            .assert_called_once()
            .with(&Invoice {
                id: 5,
                ready: false,
            });
        Ok(())
    }
}
