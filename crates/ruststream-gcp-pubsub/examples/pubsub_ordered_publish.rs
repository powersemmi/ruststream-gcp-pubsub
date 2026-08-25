//! Ordered publishing: a publish names its ordering key, so deliveries for one key stay in
//! publish order.
//!
//! Run the emulator first (`just brokers-up`), then:
//! `cargo run --example pubsub_ordered_publish -- run`

use std::io;

use ruststream::runtime::{App, AppInfo, PublishExt, RustStream};
use ruststream_gcp_pubsub::{PubSubBroker, PubSubOrdering, PubSubPublish};

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("order-events", "0.1.0")).with_broker(
        PubSubBroker::new("my-project").emulator("localhost:8085"),
        |b| {
            // The scope's after_startup is the home of a first publish: the publisher arrives
            // already paired with the connected broker, so the seed cannot race the connect.
            // --8<-- [start:ordered]
            b.after_startup(PubSubPublish, async move |publisher| -> io::Result<()> {
                let ordered = publisher.with_ordering_key("order-42");
                for step in ["created", "paid", "shipped"] {
                    ordered
                        .raw(step.as_bytes())
                        .to("orders")
                        .publish()
                        .await
                        .map_err(io::Error::other)?;
                }
                Ok(())
            });
            // --8<-- [end:ordered]
        },
    )
}
