//! Capping the retries on Pub/Sub: a handler asks for a later attempt, and the mount site says
//! how many attempts a message gets and where a spent one goes.
//!
//! Run the emulator first (`just brokers-up`), then:
//! `cargo run --example pubsub_retry`

use std::time::Duration;

use ruststream_gcp_pubsub::prelude::*;
use serde::Deserialize;

// --8<-- [start:handler]
#[derive(Debug, Deserialize)]
struct Payment {
    id: u64,
    settled: bool,
}

/// The upstream has not settled this payment yet, so an immediate redelivery would only spin.
/// Ask for the next attempt in five seconds instead.
#[subscriber(GooglePubSub::new("payments-workers").create_with_topic("payments"))]
async fn reconcile(payment: &Payment) -> HandlerOutcome {
    if !payment.settled {
        return HandlerOutcome::retry_after(Duration::from_secs(5));
    }
    println!("payment {} settled", payment.id);
    HandlerOutcome::ack()
}
// --8<-- [end:handler]

// --8<-- [start:mount]
#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(
        PubSubBroker::new("my-project").emulator("localhost:8085"),
        |b| {
            // The declaration becomes the subscription's dead-letter policy: Pub/Sub gives one
            // payment five deliveries, then publishes it to `payments-dead` itself.
            b.include(reconcile)
                .max_attempts(nonzero!(5u32))
                .dead_letter("payments-dead");
        },
    )
}
// --8<-- [end:mount]
