//! `PubSubError` where a service meets it: a publish the service refuses, and a startup that
//! fails.
//!
//! ```text
//! cargo test --example pubsub_broker_errors --features testing
//! ```

use ruststream_gcp_pubsub::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
    note: String,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "shipments")]
struct Shipment {
    order_id: u64,
}

// --8<-- [start:handler]
/// Ships an order with its note as an attribute of the shipment.
#[subscriber("orders-workers")]
async fn ship(order: &Order, Out(out): Out<impl Publisher>) -> HandlerOutcome {
    let mut headers = HeaderMap::new();
    headers.insert("note", order.note.clone());
    let shipment = Shipment { order_id: order.id };
    if let Err(err) = out.message(&shipment).with_headers(headers).publish().await {
        // The error names the topic and the service's reason. The same shipment would be
        // refused again, so the order is dropped rather than retried.
        tracing::warn!(subscription = "orders-workers", order.id, error = %err, "shipment refused");
        return HandlerOutcome::drop();
    }
    HandlerOutcome::ack()
}
// --8<-- [end:handler]

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        PubSubBroker::new("my-project"),
        |b| {
            b.include(ship).out(DefaultSlot, Publish::default()).build();
        },
    )
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_gcp_pubsub::PubSubError;
    use ruststream_gcp_pubsub::prelude::*;

    use super::{Order, Shipment, app};

    /// The `PubSubError` behind `err`, if any.
    fn pubsub_error<'a>(err: &'a (dyn Error + 'static)) -> Option<&'a PubSubError> {
        let mut cause = Some(err);
        while let Some(err) = cause {
            if let Some(pubsub) = err.downcast_ref::<PubSubError>() {
                return Some(pubsub);
            }
            cause = err.source();
        }
        None
    }

    #[tokio::test]
    async fn a_shipment_the_service_takes_acks_the_order() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<PubSubBroker>()
            .message(&Order {
                id: 7,
                note: "leave at the door".to_owned(),
            })
            .to("orders-workers")
            .publish()
            .await?;

        tb.broker::<PubSubBroker>()
            .subscriber("orders-workers")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.broker::<PubSubBroker>()
            .published::<Shipment>("shipments")
            .assert_called_once()
            .with(&Shipment { order_id: 7 })
            .with_header("note", "leave at the door");
        Ok(())
    }

    #[tokio::test]
    async fn a_note_past_the_attribute_limit_drops_the_order() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        // An attribute value holds at most 1024 bytes.
        tb.broker::<PubSubBroker>()
            .message(&Order {
                id: 8,
                note: "x".repeat(1025),
            })
            .to("orders-workers")
            .publish()
            .await?;

        tb.broker::<PubSubBroker>()
            .subscriber("orders-workers")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        tb.broker::<PubSubBroker>()
            .published::<Shipment>("shipments")
            .assert_not_called();
        Ok(())
    }

    #[tokio::test]
    async fn an_empty_subscription_name_fails_startup_with_invalid_descriptor() {
        #[subscriber(GooglePubSub::new(""))]
        async fn nowhere(order: &Order) -> HandlerOutcome {
            let _ = order.id;
            HandlerOutcome::ack()
        }
        let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
            PubSubBroker::new("my-project"),
            |b| {
                b.include(nowhere);
            },
        );

        let failed = TestApp::start(app).await.err();
        let failed = failed.expect("startup took an empty subscription name");
        assert!(
            matches!(
                pubsub_error(&failed),
                Some(PubSubError::InvalidDescriptor(_))
            ),
            "{failed}"
        );
    }
}
