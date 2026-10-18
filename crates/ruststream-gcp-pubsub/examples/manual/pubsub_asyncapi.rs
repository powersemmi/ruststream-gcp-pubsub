//! The service of `examples/pubsub_asyncapi.rs` written without the macros: the document is the
//! same, built from the same declarations.
//!
//! ```text
//! cargo run --example manual_pubsub_asyncapi --features asyncapi -- asyncapi gen
//! cargo test --example manual_pubsub_asyncapi --features asyncapi
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_gcp_pubsub::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
}

#[derive(Debug, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "receipts")]
struct Receipt {
    order_id: u64,
}

// --8<-- [start:channels]
/// The subscription `orders-workers` is a channel the service receives on, and the topic
/// `receipts` a channel it sends to.
struct ReceiptFor;

impl Handle<Order, Receipt> for ReceiptFor {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Receipt, HandlerOutcome>> {
        ready(Ok(Receipt { order_id: order.id }))
    }
}

fn orders() -> GooglePubSub {
    GooglePubSub::new("orders-workers")
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
        b.include(subscriber(orders(), ReceiptFor).reply().build())
            .out_reply(Publish::default().ordering_key("receipts"));
    })
}
// --8<-- [end:messages]

fn app() -> impl App<State = ()> {
    service(broker())
}
// --8<-- [end:app]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

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
