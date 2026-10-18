//! The reports service of the Flow control and leases guide, written without the macros: the
//! descriptor with its three settings is the first argument of the `subscriber` constructor.
//!
//! ```text
//! cargo test --example manual_guide_flow_control --features testing
//! just brokers-up
//! PUBSUB_TEST_HOST=127.0.0.1:8085 cargo test --example manual_guide_flow_control --features testing
//! cargo run --example manual_guide_flow_control --features testing,ruststream/logging -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;
use std::time::Duration;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_gcp_pubsub::prelude::*;
use serde::{Deserialize, Serialize};

#[cfg(test)]
#[path = "../../support/pubsub_live.rs"]
mod live;

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "reports")]
struct ReportRequest {
    id: u64,
    data_ready: bool,
}

// --8<-- [start:render]
/// How long a report whose data is late waits before it is tried again.
const DATA_LATE: Duration = Duration::from_secs(20);

struct Render;

impl Handle<ReportRequest> for Render {
    fn handle(
        &self,
        request: &ReportRequest,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        if !request.data_ready {
            tracing::info!(request.id, "report data late, report held");
            return ready(Err(HandlerOutcome::retry_after(DATA_LATE)));
        }
        tracing::info!(request.id, "report rendered");
        ready(Ok(()))
    }
}

fn reports() -> GooglePubSub {
    GooglePubSub::new("reports-renderer")
        .create_with_topic("reports")
        .max_outstanding(10)
        .ack_extension(Duration::from_mins(1))
        .max_lease(Duration::from_mins(30))
}
// --8<-- [end:render]

// --8<-- [start:app]
/// The service on `broker`; `app` runs it against the local emulator.
fn service(broker: PubSubBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("reports", "0.1.0")).with_broker(broker, |b| {
        b.include(subscriber(reports(), Render).build());
    })
}

fn app() -> impl App<State = ()> {
    service(PubSubBroker::new("my-project").emulator("localhost:8085"))
}

fn main() -> ExitCode {
    run_main(app)
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::Duration;

    use ruststream::testing::TestApp;
    use ruststream_gcp_pubsub::prelude::*;

    use super::{DATA_LATE, ReportRequest, app, live, service};

    /// What a live redelivery may take to arrive after the hold ends.
    const SLACK: Duration = Duration::from_secs(2);

    #[tokio::test]
    async fn a_report_with_its_data_is_rendered() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<PubSubBroker>()
            .message(&ReportRequest {
                id: 1,
                data_ready: true,
            })
            .publish()
            .await?;

        tb.broker::<PubSubBroker>()
            .subscriber("reports-renderer")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    /// The body both modes run: the late report stays with the service for the whole wait, which
    /// is longer than the subscription's ten-second acknowledgement deadline, and comes back once.
    async fn a_late_report_is_held_under_its_lease(tb: TestApp<()>) -> Result<(), Box<dyn Error>> {
        tb.broker::<PubSubBroker>()
            .message(&ReportRequest {
                id: 2,
                data_ready: false,
            })
            .publish()
            .await?;
        tb.broker::<PubSubBroker>()
            .subscriber("reports-renderer")
            .assert_called_once()
            .settled(HandlerOutcome::retry_after(DATA_LATE));

        tb.advance(DATA_LATE + SLACK).await?;
        tb.broker::<PubSubBroker>()
            .subscriber("reports-renderer")
            .assert_called(2);
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn in_process() -> Result<(), Box<dyn Error>> {
        a_late_report_is_held_under_its_lease(TestApp::start(app()).await?).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn against_the_emulator() -> Result<(), Box<dyn Error>> {
        let Some(host) = live::host() else {
            return Ok(());
        };
        live::fresh(&host, &["reports-renderer"]).await?;
        let broker = PubSubBroker::new("my-project").emulator(host);
        a_late_report_is_held_under_its_lease(TestApp::start_live(service(broker)).await?).await
    }
}
