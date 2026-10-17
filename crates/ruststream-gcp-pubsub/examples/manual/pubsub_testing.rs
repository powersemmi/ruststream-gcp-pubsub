//! The tests of `examples/pubsub_testing.rs` over the service written without the macros: the
//! test bodies are the same.
//!
//! ```text
//! cargo test --example manual_pubsub_testing --features testing
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
#[outgoing(name = "payments")]
struct Payment {
    id: u64,
    amount_cents: i64,
    settled: bool,
}

/// How long a payment the bank has not settled waits before it comes back.
const RECHECK: Duration = Duration::from_secs(30);

struct Charge;

impl Handle<Payment> for Charge {
    fn handle(
        &self,
        payment: &Payment,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        if payment.amount_cents <= 0 {
            return ready(Err(HandlerOutcome::drop()));
        }
        if !payment.settled {
            return ready(Err(HandlerOutcome::retry_after(RECHECK)));
        }
        tracing::info!(payment.id, "payment charged");
        ready(Ok(()))
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(
        PubSubBroker::new("my-project"),
        |b| {
            b.include(
                subscriber("payments-workers", Charge)
                    .map_source(|_| {
                        GooglePubSub::new("payments-workers").create_with_topic("payments")
                    })
                    .build(),
            )
            .max_attempts(nonzero!(5u32))
            .dead_letter("payments-dead");
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

    use super::{Payment, RECHECK, app};

    // --8<-- [start:testapp]
    #[tokio::test]
    async fn a_settled_payment_is_charged() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let payment = Payment {
            id: 7,
            amount_cents: 1_000,
            settled: true,
        };

        tb.broker::<PubSubBroker>()
            .message(&payment)
            .publish()
            .await?;

        tb.broker::<PubSubBroker>()
            .subscriber("payments-workers")
            .assert_called_once()
            .with(&payment)
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }
    // --8<-- [end:testapp]

    // --8<-- [start:advance]
    #[tokio::test(start_paused = true)]
    async fn an_unsettled_payment_is_dead_lettered_after_five_attempts()
    -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let payment = Payment {
            id: 8,
            amount_cents: 1_000,
            settled: false,
        };

        tb.broker::<PubSubBroker>()
            .message(&payment)
            .publish()
            .await?;
        for _ in 0..5 {
            tb.advance(RECHECK).await?;
        }

        tb.broker::<PubSubBroker>()
            .subscriber("payments-workers")
            .assert_called(5);
        tb.broker::<PubSubBroker>()
            .published::<Payment>("payments-dead")
            .assert_called_once()
            .with(&payment);
        tb.shutdown().await?;
        Ok(())
    }
    // --8<-- [end:advance]
}
