//! A batch handler on Pub/Sub: one call per batch of orders instead of one call per order.
//!
//! Run the emulator first (`just brokers-up`), then:
//! `cargo run --example pubsub_batches`

use std::time::Duration;

use ruststream_gcp_pubsub::prelude::*;
use serde::Deserialize;

// --8<-- [start:handler]
#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

/// The slice parameter is what makes this a batch handler; `batch_wait` says how long a batch
/// that is not yet full may wait for the rest of it.
#[subscriber(
    PubSubSubscription::new("orders-workers")
        .create_with_topic("orders")
        .batch_wait(Duration::from_millis(200))
)]
async fn settle(orders: &[Order]) -> HandlerOutcome {
    println!("settling {} orders", orders.len());
    for order in orders {
        println!("  order {}", order.id);
    }
    HandlerOutcome::ack()
}
// --8<-- [end:handler]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        PubSubBroker::new("my-project").emulator("localhost:8085"),
        // At most 50 orders reach the handler per call.
        |b| b.include(settle.batch(nonzero!(50))),
    )
}
// --8<-- [end:app]
