//! What a Pub/Sub service reports about itself in the generated `AsyncAPI` document.
//!
//! The document is built before anything connects, so what reaches it is what the descriptor and
//! the publish policy hold: the host clients dial, and the ordering key a mount site fixed. The
//! topic behind a subscription is not among them, which the channel case pins.

#![cfg(feature = "asyncapi")]

use ruststream::asyncapi::build_spec;
use ruststream::runtime::{AppInfo, RustStream};
use ruststream_gcp_pubsub::PubSubBroker;
use ruststream_gcp_pubsub::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The order a handler reads.
#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

/// The receipt it answers with, on a channel of its own.
#[derive(Debug, Serialize, Outgoing)]
#[outgoing(name = "order-receipts")]
struct Receipt {
    id: u64,
}

#[subscriber(GooglePubSub::new("orders-workers"), publish)]
async fn confirm(order: &Order) -> Receipt {
    Receipt { id: order.id }
}

/// The document of a service whose replies all go out under one key.
fn document() -> Value {
    // --8<-- [start:mount]
    let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker_labeled(
        "pubsub",
        PubSubBroker::new("my-project").emulator("localhost:8085"),
        |b| {
            b.include(confirm)
                .out_reply(Publish::default().ordering_key("order-42"));
        },
    );
    // --8<-- [end:mount]
    let json = build_spec(&app)
        .to_json()
        .expect("the document must serialize");
    serde_json::from_str(&json).expect("valid JSON")
}

/// What the mount site above puts under the message it publishes. The documentation shows this
/// excerpt, and this test is what keeps it true.
// --8<-- [start:binding]
const RECEIPT_BINDING: &str = r#"{
  "bindingVersion": "0.2.0",
  "orderingKey": "order-42"
}"#;
// --8<-- [end:binding]

/// The ordering key a mount site fixed is a fact of the publisher, so it reaches the message
/// binding. The core writes the binding version, which is what makes the binding readable by a
/// tool rather than a bag of fields.
#[test]
fn a_fixed_ordering_key_reaches_the_message_binding() {
    let value = document();
    let binding = &value["components"]["messages"]["Receipt"]["bindings"]["googlepubsub"];

    assert_eq!(binding["orderingKey"], "order-42");
    assert_eq!(binding["bindingVersion"], "0.2.0");
    assert_eq!(
        serde_json::to_string_pretty(binding).expect("a binding body serializes"),
        RECEIPT_BINDING,
    );
}

/// The channel binding describes a topic - its labels, its retention, its storage policy, its
/// schema - and a channel here is a subscription, whose topic only the live connection knows. So
/// the crate describes neither side of the channel rather than inventing one.
#[test]
fn a_subscription_channel_carries_no_binding() {
    let value = document();

    assert!(value["channels"]["orders-workers"]["bindings"].is_null());
    assert!(value["operations"]["receive_orders_workers"]["bindings"].is_null());
    assert!(value["channels"]["order-receipts"]["bindings"].is_null());
}

/// The server is the host clients dial, under the protocol key the specification lists. Pub/Sub
/// is reached over an API whose version is not a fact of the transport, so nothing claims one.
#[test]
fn the_server_reports_the_host_and_the_protocol_alone() {
    let value = document();
    let server = &value["servers"]["pubsub"];

    assert_eq!(server["host"], "localhost:8085");
    assert_eq!(server["protocol"], "googlepubsub");
    assert!(server["protocolVersion"].is_null());
    assert!(server["bindings"].is_null());
}
