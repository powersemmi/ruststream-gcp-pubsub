//! The payments service of the Production guide, written without the macros: the configuration
//! and the broker are the same functions, the handler is a type, and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_guide_production --features testing
//! GOOGLE_CLOUD_PROJECT=my-project PUBSUB_ENDPOINT=https://us-east1-pubsub.googleapis.com \
//!     cargo run --example manual_guide_production --features testing,ruststream/logging -- run
//! ```

use std::env;
use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_gcp_pubsub::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Payment {
    id: u64,
    amount_cents: u64,
}

// --8<-- [start:config]
/// Where the service runs, read from its environment.
struct Config {
    project: String,
    /// A locational endpoint, such as `https://us-east1-pubsub.googleapis.com`.
    endpoint: Option<String>,
}

impl Config {
    fn from_env() -> Self {
        Self {
            project: env::var("GOOGLE_CLOUD_PROJECT")
                .expect("GOOGLE_CLOUD_PROJECT names the project"),
            endpoint: env::var("PUBSUB_ENDPOINT").ok(),
        }
    }
}

/// Application Default Credentials, and the endpoint the deployment names, if any.
fn broker(config: Config) -> PubSubBroker {
    let broker = PubSubBroker::new(config.project);
    match config.endpoint {
        Some(endpoint) => broker.endpoint(endpoint),
        None => broker,
    }
}
// --8<-- [end:config]

// --8<-- [start:service]
/// Charges a payment read from a subscription operations created on the topic `payments`.
struct Charge;

impl Handle<Payment> for Charge {
    fn handle(
        &self,
        payment: &Payment,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        if payment.amount_cents == 0 {
            tracing::warn!(payment.id, "payment without an amount dropped");
            return ready(Err(HandlerOutcome::drop()));
        }
        tracing::info!(payment.id, payment.amount_cents, "payment charged");
        ready(Ok(()))
    }
}

fn service(config: Config) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(broker(config), |b| {
        // Written onto the subscription at startup, which needs `pubsub.subscriptions.update`.
        b.include(subscriber("payments-workers", Charge).build())
            .max_attempts(nonzero!(5u32))
            .dead_letter("payments-dead");
    })
}

fn app() -> impl App<State = ()> {
    service(Config::from_env())
}

fn main() -> ExitCode {
    run_main(app)
}
// --8<-- [end:service]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_gcp_pubsub::prelude::*;

    use super::{Config, Payment, service};

    #[tokio::test]
    async fn the_regional_service_charges_a_payment() -> Result<(), Box<dyn Error>> {
        // In process nothing is dialled, so no credentials are read.
        let tb = TestApp::start(service(Config {
            project: "my-project".to_owned(),
            endpoint: Some("https://us-east1-pubsub.googleapis.com".to_owned()),
        }))
        .await?;
        let payment = Payment {
            id: 7,
            amount_cents: 1250,
        };

        // In process, a subscription the service only names reads the topic of its own name.
        tb.broker::<PubSubBroker>()
            .message(&payment)
            .to("payments-workers")
            .publish()
            .await?;

        tb.broker::<PubSubBroker>()
            .subscriber("payments-workers")
            .assert_called_once()
            .with(&payment)
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_payment_without_an_amount_is_dropped() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(Config {
            project: "my-project".to_owned(),
            endpoint: None,
        }))
        .await?;

        tb.broker::<PubSubBroker>()
            .message(&Payment {
                id: 8,
                amount_cents: 0,
            })
            .to("payments-workers")
            .publish()
            .await?;

        tb.broker::<PubSubBroker>()
            .subscriber("payments-workers")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        tb.shutdown().await?;
        Ok(())
    }
}
