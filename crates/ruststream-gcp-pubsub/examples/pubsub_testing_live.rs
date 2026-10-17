//! One test body, two modes: in process under `TestApp::start`, and against the Pub/Sub emulator
//! under `TestApp::start_live`. The live test skips without `PUBSUB_TEST_HOST`, and
//! `RUSTSTREAM_REQUIRE_LIVE=1` turns that skip into a failure.
//!
//! ```text
//! cargo test --example pubsub_testing_live --features testing
//! just brokers-up
//! PUBSUB_TEST_HOST=127.0.0.1:8085 RUSTSTREAM_REQUIRE_LIVE=1 \
//!     cargo test --example pubsub_testing_live --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream_gcp_pubsub::prelude::*;
use serde::{Deserialize, Serialize};

#[cfg(test)]
#[path = "support/pubsub_live.rs"]
mod live;

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "testing-live-payments")]
struct Payment {
    amount: u64,
}

/// The emulator starts empty, so the subscription creates itself and its topic.
#[subscriber(
    GooglePubSub::new("testing-live-payments-workers").create_with_topic("testing-live-payments")
)]
async fn accept(payment: &Payment) -> HandlerOutcome {
    if payment.amount == 0 {
        return HandlerOutcome::drop();
    }
    HandlerOutcome::ack()
}

/// The service on `broker`: the production builder, unchanged in either mode.
fn service(broker: PubSubBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(broker, |b| {
        b.include(accept);
    })
}

#[ruststream::app]
fn app() -> impl App<State = ()> {
    service(PubSubBroker::new("my-project"))
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_gcp_pubsub::prelude::*;

    use super::{Payment, app, live, service};

    // --8<-- [start:start-live]
    /// The body both modes run; only the start call differs.
    async fn accepts_a_payment(tb: TestApp<()>) -> Result<(), Box<dyn Error>> {
        tb.broker::<PubSubBroker>()
            .message(&Payment { amount: 100 })
            .publish()
            .await?;

        tb.broker::<PubSubBroker>()
            .subscriber("testing-live-payments-workers")
            .assert_called_once()
            .with(&Payment { amount: 100 })
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn in_process() -> Result<(), Box<dyn Error>> {
        accepts_a_payment(TestApp::start(app()).await?).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn against_the_emulator() -> Result<(), Box<dyn Error>> {
        let Some(host) = live::host() else {
            return Ok(());
        };
        live::fresh(&host, &["testing-live-payments-workers"]).await?;
        let broker = PubSubBroker::new("my-project").emulator(host);
        accepts_a_payment(TestApp::start_live(service(broker)).await?).await
    }
    // --8<-- [end:start-live]
}
