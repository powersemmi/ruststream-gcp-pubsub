//! The payments service of the Production guide: the project and the endpoint come from the
//! environment, the credentials are Application Default Credentials, and the subscription is one
//! operations created.
//!
//! ```text
//! cargo test --example guide_production --features testing
//! GOOGLE_CLOUD_PROJECT=my-project PUBSUB_ENDPOINT=https://us-east1-pubsub.googleapis.com \
//!     cargo run --example guide_production --features testing,ruststream/logging -- run
//! ```

use std::env;

use ruststream_gcp_pubsub::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
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
/// A subscription operations created on the topic `payments`.
#[subscriber("payments-workers")]
async fn charge(payment: &Payment) -> HandlerOutcome {
    if payment.amount_cents == 0 {
        tracing::warn!(payment.id, "payment without an amount dropped");
        return HandlerOutcome::drop();
    }
    tracing::info!(payment.id, payment.amount_cents, "payment charged");
    HandlerOutcome::ack()
}

fn service(config: Config) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(broker(config), |b| {
        // Written onto the subscription at startup, which needs `pubsub.subscriptions.update`.
        b.include(charge)
            .max_attempts(nonzero!(5u32))
            .dead_letter("payments-dead");
    })
}

#[ruststream::app]
fn app() -> impl App<State = ()> {
    service(Config::from_env())
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
