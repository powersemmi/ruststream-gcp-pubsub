//! The service of `examples/pubsub_settlement_attempts.rs` written without the macros: the body
//! reads the header through its `Context`.
//!
//! ```text
//! cargo test --example manual_pubsub_settlement_attempts --features testing
//! cargo run --example manual_pubsub_settlement_attempts --features testing -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_gcp_pubsub::DELIVERY_ATTEMPT_HEADER;
use ruststream_gcp_pubsub::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
    in_stock: bool,
}

// --8<-- [start:handler]
/// Waits for stock through two redeliveries, then gives the order up on the third attempt.
struct Reserve;

impl Handle<Order> for Reserve {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        let attempt: u32 = ctx
            .headers()
            .get_str(DELIVERY_ATTEMPT_HEADER)
            .and_then(|attempt| attempt.parse().ok())
            .unwrap_or(1);
        if order.in_stock {
            return ready(Ok(()));
        }
        if attempt >= 3 {
            tracing::error!(order.id, attempt, "no stock, order given up");
            return ready(Err(HandlerOutcome::drop()));
        }
        tracing::warn!(order.id, attempt, "no stock yet");
        ready(Err(HandlerOutcome::retry()))
    }
}
// --8<-- [end:handler]

fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        PubSubBroker::new("my-project"),
        |b| {
            // The dead-letter policy is what makes Pub/Sub count the attempts.
            b.include(
                subscriber("orders-workers", Reserve)
                    .map_source(|_| GooglePubSub::new("orders-workers").create_with_topic("orders"))
                    .build(),
            )
            .max_attempts(nonzero!(5u32))
            .dead_letter("orders-dead");
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
    async fn the_third_attempt_gives_the_order_up() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let order = Order {
            id: 7,
            in_stock: false,
        };
        tb.broker::<PubSubBroker>()
            .message(&order)
            .publish()
            .await?;

        tb.broker::<PubSubBroker>()
            .subscriber("orders-workers")
            .assert_called(3)
            .settled(HandlerOutcome::drop());
        // Dropped before the cap, so it is acknowledged and never dead-lettered.
        tb.broker::<PubSubBroker>()
            .published::<Order>("orders-dead")
            .assert_not_called();
        Ok(())
    }
}
