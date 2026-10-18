//! The `AsyncAPI` document of a Pub/Sub service: the server is the host clients dial, a reply's
//! channel reports its topic, and a mount site's fixed ordering key reaches the message's
//! `googlepubsub` binding. The tests read the document the app generates.
//!
//! ```text
//! cargo run --example pubsub_asyncapi --features asyncapi -- asyncapi gen
//! cargo test --example pubsub_asyncapi --features asyncapi
//! ```

use ruststream_gcp_pubsub::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

#[derive(Debug, Serialize, Outgoing)]
#[outgoing(name = "receipts")]
struct Receipt {
    order_id: u64,
}

// --8<-- [start:channels]
/// The subscription `orders-workers` is a channel the service receives on, and the topic
/// `receipts` a channel it sends to.
#[subscriber(GooglePubSub::new("orders-workers"), reply)]
async fn receipt(order: &Order) -> Receipt {
    Receipt { order_id: order.id }
}
// --8<-- [end:channels]

// --8<-- [start:server]
/// The regional endpoint; the document reports `us-east1-pubsub.googleapis.com`.
fn broker() -> PubSubBroker {
    PubSubBroker::new("my-project").endpoint("https://us-east1-pubsub.googleapis.com")
}
// --8<-- [end:server]

// --8<-- [start:app]
// --8<-- [start:messages]
fn service(broker: PubSubBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("orders", "1.0.0")).with_broker_labeled("pubsub", broker, |b| {
        // Every receipt goes out under one key, so the document states it.
        b.include(receipt)
            .out_reply(Publish::default().ordering_key("receipts"));
    })
}
// --8<-- [end:messages]

#[ruststream::app]
fn app() -> impl App<State = ()> {
    service(broker())
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::asyncapi::build_spec;
    use serde_json::{Value, json};

    use super::{broker, service};

    fn document() -> Result<Value, Box<dyn Error>> {
        Ok(serde_json::from_str(
            &build_spec(&service(broker())).to_json()?,
        )?)
    }

    #[test]
    fn the_server_is_the_host_clients_dial() -> Result<(), Box<dyn Error>> {
        let spec = document()?;

        assert_eq!(
            spec["servers"]["pubsub"],
            json!({
                "host": "us-east1-pubsub.googleapis.com",
                "protocol": "googlepubsub",
            })
        );
        Ok(())
    }

    #[test]
    fn the_fixed_ordering_key_reaches_the_message_binding() -> Result<(), Box<dyn Error>> {
        let spec = document()?;

        assert_eq!(
            spec["components"]["messages"]["Receipt"]["bindings"],
            json!({
                "googlepubsub": {
                    "orderingKey": "receipts",
                    "bindingVersion": "0.2.0",
                },
            })
        );
        Ok(())
    }

    #[test]
    fn the_reply_channel_is_its_topic() -> Result<(), Box<dyn Error>> {
        let spec = document()?;

        assert_eq!(spec["channels"]["receipts"]["address"], "receipts");
        assert!(spec["channels"]["receipts"]["bindings"].is_null());
        assert!(spec["channels"]["orders-workers"]["bindings"].is_null());
        Ok(())
    }
}
