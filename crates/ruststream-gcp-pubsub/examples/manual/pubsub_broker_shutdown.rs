//! The service of `examples/pubsub_broker_shutdown.rs` without the macros: the handler is a named
//! type and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_pubsub_broker_shutdown --features testing
//! cargo run --example manual_pubsub_broker_shutdown --features testing -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_gcp_pubsub::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "shipments")]
struct Shipment {
    order_id: u64,
}

// --8<-- [start:app]
/// Answers every order with a shipment on the topic `shipments`.
struct Ship;

impl Handle<Order, Shipment> for Ship {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Shipment, HandlerOutcome>> {
        ready(Ok(Shipment { order_id: order.id }))
    }
}

fn service(broker: PubSubBroker) -> impl App<State = ()> {
    // At shutdown the runtime flushes the shipments still buffered for `shipments`.
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(subscriber("orders-workers", Ship).reply().build())
            .out_reply(Publish::default());
    })
}

fn app() -> impl App<State = ()> {
    service(PubSubBroker::new("my-project"))
}
// --8<-- [end:app]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_gcp_pubsub::PubSubError;
    use ruststream_gcp_pubsub::prelude::*;

    use super::{Order, Shipment, service};

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
    async fn the_shipment_is_out_and_a_later_publish_reports_the_shutdown()
    -> Result<(), Box<dyn Error>> {
        let broker = PubSubBroker::new("my-project");
        let publisher = broker.publisher();
        let tb = TestApp::start(service(broker)).await?;
        tb.broker::<PubSubBroker>()
            .message(&Order { id: 7 })
            .to("orders-workers")
            .publish()
            .await?;
        tb.broker::<PubSubBroker>()
            .published::<Shipment>("shipments")
            .assert_called_once()
            .with(&Shipment { order_id: 7 });
        tb.shutdown().await?;

        let late = publisher
            .message(&Shipment { order_id: 8 })
            .publish()
            .await
            .expect_err("a publish after shutdown succeeded");
        assert!(
            matches!(pubsub_error(&late), Some(PubSubError::NotConnected)),
            "{late}"
        );
        Ok(())
    }
}
