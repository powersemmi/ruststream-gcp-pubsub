//! The service of `examples/pubsub_broker_emulator.rs` without the macros: the handler is a named
//! type and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_pubsub_broker_emulator --features testing
//! just brokers-up
//! PUBSUB_TEST_HOST=127.0.0.1:8085 cargo test --example manual_pubsub_broker_emulator --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::future::{Future, ready};
use std::process::ExitCode;

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

/// The emulator starts empty, so the subscription creates itself and its topic.
fn orders() -> GooglePubSub {
    GooglePubSub::new("emulator-orders-workers").create_with_topic("emulator-orders")
}

fn service(broker: PubSubBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(subscriber(orders(), Place).build());
    })
}

// --8<-- [start:broker]
/// The emulator at `host`, a `host:port` without a scheme.
fn broker(host: &str) -> PubSubBroker {
    PubSubBroker::new("my-project").emulator(host)
}

fn app() -> impl App<State = ()> {
    service(broker("localhost:8085"))
}
// --8<-- [end:broker]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_gcp_pubsub::prelude::*;

    use super::{Order, app, broker, live, service};

    /// The body both modes run: the order reaches the handler through its topic.
    async fn places_an_order(tb: TestApp<()>) -> Result<(), Box<dyn Error>> {
        tb.broker::<PubSubBroker>()
            .message(&Order { id: 7 })
            .to("emulator-orders")
            .publish()
            .await?;

        tb.broker::<PubSubBroker>()
            .subscriber("emulator-orders-workers")
            .assert_called_once()
            .with(&Order { id: 7 })
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn in_process() -> Result<(), Box<dyn Error>> {
        places_an_order(TestApp::start(app()).await?).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn against_the_emulator() -> Result<(), Box<dyn Error>> {
        let Some(host) = live::host() else {
            return Ok(());
        };
        live::fresh(&host, &["emulator-orders-workers"]).await?;
        places_an_order(TestApp::start_live(service(broker(&host))).await?).await
    }
}
