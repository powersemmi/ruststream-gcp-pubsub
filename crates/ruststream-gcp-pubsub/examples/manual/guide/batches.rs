//! The clicks service of the Batches guide, written without the macros: the body implements
//! `Handle<[Click]>`, `Err(outcomes)` settles click by click, and `batch(n)` is a step of the
//! chain.
//!
//! ```text
//! just brokers-up
//! cargo run --example manual_guide_batches --features testing,ruststream/logging -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;
use std::time::Duration;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_gcp_pubsub::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "clicks")]
struct Click {
    page: String,
    user: u64,
}

/// The id the warehouse has not loaded yet; a click from it is refused.
const UNKNOWN_USER: u64 = 0;

// --8<-- [start:load]
/// Loads a batch of clicks into the warehouse in one write. A click from a user the warehouse
/// does not know yet comes back on its own; a click without a page is dropped.
struct Load;

impl Handle<[Click]> for Load {
    fn handle(
        &self,
        clicks: &[Click],
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), Vec<HandlerOutcome>>> {
        tracing::info!(count = clicks.len(), "clicks loaded");
        // `Err` carries one outcome per click, in the order of the batch.
        ready(Err(clicks.iter().map(settle).collect()))
    }
}

fn settle(click: &Click) -> HandlerOutcome {
    if click.page.is_empty() {
        HandlerOutcome::drop()
    } else if click.user == UNKNOWN_USER {
        HandlerOutcome::retry()
    } else {
        HandlerOutcome::ack()
    }
}
// --8<-- [end:load]

// --8<-- [start:app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("clicks", "0.1.0")).with_broker(
        PubSubBroker::new("my-project").emulator("localhost:8085"),
        |b| {
            // At most a hundred clicks per call; a click refused five times goes to `clicks-dead`.
            b.include(
                subscriber(
                    GooglePubSub::new("clicks-loader")
                        .create_with_topic("clicks")
                        .batch_wait(Duration::from_millis(500)),
                    Load,
                )
                .batch(nonzero!(100))
                .build(),
            )
            .max_attempts(nonzero!(5u32))
            .dead_letter("clicks-dead");
        },
    )
}

fn main() -> ExitCode {
    run_main(app)
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_gcp_pubsub::prelude::*;

    use super::{Click, UNKNOWN_USER, app};

    fn click(page: &str, user: u64) -> Click {
        Click {
            page: page.to_owned(),
            user,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn clicks_published_together_meet_in_one_batch() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        let (home, pricing, nowhere) = (click("/", 1), click("/pricing", 2), click("", 3));
        let broker = tb.broker::<PubSubBroker>();
        let (first, second, third) = tokio::join!(
            broker.message(&home).publish(),
            broker.message(&pricing).publish(),
            broker.message(&nowhere).publish(),
        );
        first?;
        second?;
        third?;

        let batches = tb
            .broker::<PubSubBroker>()
            .subscriber("clicks-loader")
            .assert_batch_sizes(&[3])
            .batches::<Click>();
        assert_eq!(batches, [vec![home, pricing, nowhere]]);
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_refused_click_comes_back_alone_until_it_is_dead_lettered()
    -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        let (home, stranger) = (click("/", 1), click("/pricing", UNKNOWN_USER));
        let broker = tb.broker::<PubSubBroker>();
        let (first, second) = tokio::join!(
            broker.message(&home).publish(),
            broker.message(&stranger).publish(),
        );
        first?;
        second?;

        // The batch of two settles click by click: the refused one is delivered again on its
        // own, five deliveries in all, and then Pub/Sub moves it to the dead-letter topic.
        tb.broker::<PubSubBroker>()
            .subscriber("clicks-loader")
            .assert_batch_sizes(&[2, 1, 1, 1, 1]);
        tb.broker::<PubSubBroker>()
            .published::<Click>("clicks-dead")
            .assert_called_once()
            .with(&stranger);
        tb.shutdown().await?;
        Ok(())
    }
}
