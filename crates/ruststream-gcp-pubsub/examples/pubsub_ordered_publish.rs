//! Ordered publishing: a `partition-key` header becomes the message's ordering key, so
//! deliveries for one key stay in publish order.
//!
//! Run the emulator first (`just brokers-up`), then:
//! `cargo run --example pubsub_ordered_publish -- run`

use std::io;

use ruststream::runtime::{App, AppInfo, RustStream};
use ruststream::{Headers, OutgoingMessage, Publisher};
use ruststream_gcp_pubsub::{PARTITION_KEY_HEADER, PubSubBroker, PubSubPublish};

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("order-events", "0.1.0")).with_broker(
        PubSubBroker::new("my-project").emulator("localhost:8085"),
        |b| {
            // The scope's after_startup is the home of a first publish: the publisher arrives
            // already paired with the connected broker, so the seed cannot race the connect.
            b.after_startup(PubSubPublish, async move |publisher| -> io::Result<()> {
                for step in ["created", "paid", "shipped"] {
                    let mut headers = Headers::new();
                    headers.insert(PARTITION_KEY_HEADER, "order-42");
                    let msg = OutgoingMessage::new("orders", step.as_bytes()).with_headers(headers);
                    publisher.publish(msg).await.map_err(io::Error::other)?;
                }
                Ok(())
            });
        },
    )
}
