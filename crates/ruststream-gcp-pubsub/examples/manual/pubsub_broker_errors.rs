//! The service of `examples/pubsub_broker_errors.rs` without the macros: the handler is a named
//! type and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_pubsub_broker_errors --features testing
//! ```

use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_gcp_pubsub::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
    note: String,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "shipments")]
struct Shipment {
    order_id: u64,
}

// --8<-- [start:handler]
/// Ships an order with its note as an attribute of the shipment.
struct Ship;

impl<Entry> Handle<Order, (), Outs<(Entry,)>> for Ship
where
    Entry: OutEntry<DefaultSlot, Wire: Publisher>,
{
    async fn handle(
        &self,
        order: &Order,
        outs: &Outs<(Entry,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let mut headers = HeaderMap::new();
        headers.insert("note", order.note.clone());
        let shipment = Shipment { order_id: order.id };
        let sent = outs
            .get(DefaultSlot)
            .message(&shipment)
            .with_headers(headers)
            .publish()
            .await;
        if let Err(err) = sent {
            // The error names the topic and the service's reason. The same shipment would be
            // refused again, so the order is dropped rather than retried.
            tracing::warn!(subscription = "orders-workers", order.id, error = %err, "shipment refused");
            return Err(HandlerOutcome::drop());
        }
        Ok(())
    }
}
// --8<-- [end:handler]

fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        PubSubBroker::new("my-project"),
        |b| {
            b.include(subscriber("orders-workers", Ship).build())
                .out(DefaultSlot, Publish::default())
                .build();
        },
    )
}

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::future::{Future, ready};

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
        struct Nowhere;

        impl Handle<Order> for Nowhere {
            fn handle(
                &self,
                order: &Order,
                _outs: &(),
                _ctx: &mut Context<'_>,
            ) -> impl Future<Output = Result<(), HandlerOutcome>> {
                let _ = order.id;
                ready(Ok(()))
            }
        }
        let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
            PubSubBroker::new("my-project"),
            |b| {
                b.include(subscriber(GooglePubSub::new(""), Nowhere).build());
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
