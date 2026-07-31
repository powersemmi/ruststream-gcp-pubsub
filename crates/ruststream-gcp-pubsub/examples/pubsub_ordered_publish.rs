//! Ordered publishing: a `partition-key` header becomes the message's ordering key, so
//! deliveries for one key stay in publish order.
//!
//! Run the emulator first (`just brokers-up`), then:
//! `cargo run --example pubsub_ordered_publish`

use ruststream::{Broker, ConnectedBroker, Headers, OutgoingMessage, Publisher};
use ruststream_gcp_pubsub::{PARTITION_KEY_HEADER, PubSubBroker};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let connected = PubSubBroker::new("my-project")
        .emulator("localhost:8085")
        .connect()
        .await?;

    let publisher = connected.publisher();
    for step in ["created", "paid", "shipped"] {
        let mut headers = Headers::new();
        headers.insert(PARTITION_KEY_HEADER, "order-42");
        publisher
            .publish(OutgoingMessage::new("orders", step.as_bytes()).with_headers(headers))
            .await?;
    }

    connected.shutdown().await?;
    Ok(())
}
