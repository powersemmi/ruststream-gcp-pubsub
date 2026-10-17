//! The service of `examples/pubsub_settlement_dead_letters.rs` written without the macros: the
//! cap and the dead-letter topic are the same steps of the mount chain.
//!
//! ```text
//! cargo test --example manual_pubsub_settlement_dead_letters --features testing
//! cargo run --example manual_pubsub_settlement_dead_letters --features testing -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

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
}

/// The gateway declines every amount ending in 7.
struct Charge;

impl Handle<Payment> for Charge {
    fn handle(
        &self,
        payment: &Payment,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        if payment.amount_cents % 10 == 7 {
            tracing::warn!(payment.id, "charge declined, retrying");
            return ready(Err(HandlerOutcome::retry()));
        }
        ready(Ok(()))
    }
}

fn payments() -> GooglePubSub {
    GooglePubSub::new("payments-workers").create_with_topic("payments")
}

// --8<-- [start:reader]
/// The dead-letter topic is an ordinary topic, read through a subscription of its own.
struct OnDeadLetter;

impl Handle<Payment> for OnDeadLetter {
    fn handle(
        &self,
        payment: &Payment,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::error!(payment.id, "payment dead-lettered");
        ready(Ok(()))
    }
}

fn dead_letters() -> GooglePubSub {
    GooglePubSub::new("payments-dead-audit").create_with_topic("payments-dead")
}
// --8<-- [end:reader]

// --8<-- [start:declaration]
fn app() -> impl App {
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(
        PubSubBroker::new("my-project"),
        |b| {
            // Five deliveries of one payment; after the fifth Pub/Sub publishes it to
            // `payments-dead`, payload and attributes as they arrived.
            b.include(
                subscriber("payments-workers", Charge)
                    .map_source(|_| payments())
                    .build(),
            )
            .max_attempts(nonzero!(5u32))
            .dead_letter("payments-dead");
            b.include(
                subscriber("payments-dead-audit", OnDeadLetter)
                    .map_source(|_| dead_letters())
                    .build(),
            );
        },
    )
}
// --8<-- [end:declaration]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_gcp_pubsub::prelude::*;

    use super::{Payment, app};

    #[tokio::test]
    async fn the_fifth_decline_is_dead_lettered_and_read_there() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let declined = Payment {
            id: 7,
            amount_cents: 1_007,
        };
        tb.broker::<PubSubBroker>()
            .message(&declined)
            .publish()
            .await?;

        tb.broker::<PubSubBroker>()
            .subscriber("payments-workers")
            .assert_called(5);
        tb.broker::<PubSubBroker>()
            .published::<Payment>("payments-dead")
            .assert_called_once()
            .with(&declined);
        tb.broker::<PubSubBroker>()
            .subscriber("payments-dead-audit")
            .assert_called_once()
            .with(&declined)
            .settled(HandlerOutcome::ack());
        Ok(())
    }

    #[tokio::test]
    async fn a_charged_payment_never_reaches_the_dead_letter_topic() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let paid = Payment {
            id: 1,
            amount_cents: 1_000,
        };
        tb.broker::<PubSubBroker>().message(&paid).publish().await?;

        tb.broker::<PubSubBroker>()
            .subscriber("payments-workers")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.broker::<PubSubBroker>()
            .published::<Payment>("payments-dead")
            .assert_not_called();
        Ok(())
    }
}
