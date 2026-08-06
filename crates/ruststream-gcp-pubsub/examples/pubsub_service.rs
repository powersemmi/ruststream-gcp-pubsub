//! A minimal Pub/Sub service: consume orders from an existing subscription.
//!
//! Run the emulator first (`just brokers-up`), then:
//! `cargo run --example pubsub_service`

use ruststream::runtime::{App, AppInfo, HandlerResult, RustStream};
use ruststream::subscriber;
use ruststream_gcp_pubsub::{PubSubBroker, PubSubSubscription};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

#[subscriber(PubSubSubscription::new("orders-workers").create_with_topic("orders"))]
async fn handle(order: &Order) -> HandlerResult {
    println!("got order {}", order.id);
    HandlerResult::Ack
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        PubSubBroker::new("my-project").emulator("localhost:8085"),
        |b| b.include(handle),
    )
}
