//! The service of `examples/pubsub_publishing_ordering.rs` written without the macros: the body
//! reads the key through its `Context`.
//!
//! ```text
//! cargo test --example manual_pubsub_publishing_ordering --features testing
//! just brokers-up
//! PUBSUB_TEST_HOST=127.0.0.1:8085 cargo test --example manual_pubsub_publishing_ordering --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_gcp_pubsub::PARTITION_KEY_HEADER;
use ruststream_gcp_pubsub::prelude::*;
use serde::{Deserialize, Serialize};

#[cfg(test)]
#[path = "../support/pubsub_live.rs"]
mod live;

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "ordering-order-events")]
struct OrderEvent {
    order_id: u64,
    step: String,
}

// --8<-- [start:ordered]
/// The subscription `create_with_topic` creates has message ordering on, so the events of one
/// order arrive in the order they were published.
struct Track;

impl Handle<OrderEvent> for Track {
    fn handle(
        &self,
        event: &OrderEvent,
        _outs: &(),
        ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        let key = ctx
            .headers()
            .get_str(PARTITION_KEY_HEADER)
            .unwrap_or_default();
        tracing::info!(event.order_id, event.step, key, "order event");
        ready(Ok(()))
    }
}

fn order_events() -> GooglePubSub {
    GooglePubSub::new("ordering-order-events-workers").create_with_topic("ordering-order-events")
}
// --8<-- [end:ordered]

fn service(broker: PubSubBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(subscriber(order_events(), Track).build());
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

    use ruststream::testing::TestApp;
    use ruststream_gcp_pubsub::PARTITION_KEY_HEADER;
    use ruststream_gcp_pubsub::prelude::*;

    use super::{OrderEvent, app, live, service};

    /// The body both modes run: three events of one order, published under its key, arrive in
    /// that order.
    async fn the_events_of_one_order_arrive_in_order(
        tb: TestApp<()>,
    ) -> Result<(), Box<dyn Error>> {
        let steps = ["created", "paid", "shipped"];
        for step in steps {
            let mut headers = HeaderMap::new();
            headers.insert(PARTITION_KEY_HEADER, "order-42");
            tb.broker::<PubSubBroker>()
                .message(&OrderEvent {
                    order_id: 42,
                    step: step.to_owned(),
                })
                .with_headers(headers)
                .publish()
                .await?;
        }

        let received: Vec<String> = tb
            .broker::<PubSubBroker>()
            .subscriber("ordering-order-events-workers")
            .assert_called(3)
            .received::<OrderEvent>()
            .into_iter()
            .map(|event| event.step)
            .collect();
        assert_eq!(received, steps);
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn in_process() -> Result<(), Box<dyn Error>> {
        the_events_of_one_order_arrive_in_order(TestApp::start(app()).await?).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn against_the_emulator() -> Result<(), Box<dyn Error>> {
        let Some(host) = live::host() else {
            return Ok(());
        };
        live::fresh(&host, &["ordering-order-events-workers"]).await?;
        let broker = PubSubBroker::new("my-project").emulator(host);
        the_events_of_one_order_arrive_in_order(TestApp::start_live(service(broker)).await?).await
    }
}
