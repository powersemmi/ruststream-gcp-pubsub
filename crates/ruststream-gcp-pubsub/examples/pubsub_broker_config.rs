//! The broker's connection settings: `credentials` replaces Application Default Credentials, and
//! `endpoint` names another service endpoint. The tests run the same apps in process, where the
//! settings are carried and nothing is dialled.
//!
//! ```text
//! cargo test --example pubsub_broker_config --features testing,asyncapi
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::fs;

use google_cloud_auth::build_errors::Error as CredentialsError;
use google_cloud_auth::credentials::service_account;
use ruststream_gcp_pubsub::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

#[subscriber("orders-workers")]
async fn place(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "order placed");
    HandlerOutcome::ack()
}

/// Where the deployment mounts the key.
const KEY_FILE: &str = "/var/secrets/pubsub/key.json";

fn service(broker: PubSubBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker_labeled("pubsub", broker, |b| {
        b.include(place);
    })
}

// --8<-- [start:credentials]
/// Signs in with the service account key the deployment mounts as a secret.
fn broker(key: Value) -> Result<PubSubBroker, CredentialsError> {
    let credentials = service_account::Builder::new(key).build()?;
    Ok(PubSubBroker::new("my-project").credentials(credentials))
}
// --8<-- [end:credentials]

// --8<-- [start:endpoint]
/// The regional endpoint of `us-east1`: every request of the service goes to that region.
fn regional() -> PubSubBroker {
    PubSubBroker::new("my-project").endpoint("https://us-east1-pubsub.googleapis.com")
}
// --8<-- [end:endpoint]

#[ruststream::app]
fn app() -> impl App<State = ()> {
    // A service that cannot read its key does not start.
    let key = fs::read_to_string(KEY_FILE).expect("the service account key is mounted");
    let key = serde_json::from_str(&key).expect("the key file is JSON");
    service(broker(key).expect("the key file is a service account key"))
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::asyncapi::build_spec;
    use ruststream::testing::TestApp;
    use ruststream_gcp_pubsub::prelude::*;
    use serde_json::{Value, json};

    use super::{Order, broker, regional, service};

    /// The body both apps run: the order reaches the handler.
    async fn places_an_order(tb: TestApp<()>) -> Result<(), Box<dyn Error>> {
        tb.broker::<PubSubBroker>()
            .message(&Order { id: 7 })
            .to("orders-workers")
            .publish()
            .await?;

        tb.broker::<PubSubBroker>()
            .subscriber("orders-workers")
            .assert_called_once()
            .with(&Order { id: 7 })
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn the_signed_in_service_runs_in_process() -> Result<(), Box<dyn Error>> {
        // The in-process mode dials nothing, so the key is never used to sign a request.
        let key = json!({
            "type": "service_account",
            "client_email": "orders-svc@my-project.iam.gserviceaccount.com",
            "private_key_id": "test-key-id",
            "private_key": "-----BEGIN PRIVATE KEY-----\ntest\n-----END PRIVATE KEY-----\n",
            "project_id": "my-project",
        });
        places_an_order(TestApp::start(service(broker(key)?)).await?).await
    }

    #[tokio::test]
    async fn the_regional_service_runs_in_process() -> Result<(), Box<dyn Error>> {
        places_an_order(TestApp::start(service(regional())).await?).await
    }

    #[test]
    fn the_document_names_the_regional_endpoint() -> Result<(), Box<dyn Error>> {
        let spec: Value = serde_json::from_str(&build_spec(&service(regional())).to_json()?)?;

        assert_eq!(
            spec["servers"]["pubsub"]["host"],
            json!("us-east1-pubsub.googleapis.com")
        );
        Ok(())
    }
}
