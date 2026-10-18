//! The ledger service of the Ordering keys guide, written without the macros: the lanes are the
//! `workers_by_key` step of the chain, the slot's entry is bounded on the crate's settings type,
//! and the transform is the same type as in the macro form.
//!
//! ```text
//! just brokers-up
//! cargo run --example manual_guide_ordering --features testing,ruststream/logging -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
// `Outgoing` names the derive at the crate root and the publish pipeline's message type in
// `runtime`; a publish transform takes the second one.
use ruststream::runtime::{Outgoing, PublishContext};
use ruststream::schemars::JsonSchema;
use ruststream_gcp_pubsub::PARTITION_KEY_HEADER;
use ruststream_gcp_pubsub::prelude::*;
use serde::{Deserialize, Serialize};

/// One change to an account, published under the account's id as its ordering key.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "ledger")]
struct Entry {
    account: String,
    amount_cents: i64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Posted {
    account: String,
    amount_cents: i64,
}

// --8<-- [start:post]
/// Posts an entry and publishes the posting under the account's key.
struct Post;

impl<Slot> Handle<Entry, (), Outs<(Slot,)>> for Post
where
    Slot: OutEntry<DefaultSlot, Wire: Publisher<Options = PubSubPublishOptions>>,
{
    async fn handle(
        &self,
        entry: &Entry,
        outs: &Outs<(Slot,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let posted = Posted {
            account: entry.account.clone(),
            amount_cents: entry.amount_cents,
        };
        let sent = outs
            .get(DefaultSlot)
            .message(&posted)
            .to("postings")
            .ordering_key(entry.account.clone())
            .publish()
            .await;
        if let Err(err) = sent {
            tracing::warn!(entry.account, error = %err, "posting not published, entry retried");
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
}
// --8<-- [end:post]

// --8<-- [start:notify]
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "statements")]
struct Statement {
    account: String,
    amount_cents: i64,
}

/// Answers each entry with a statement line for the account's owner.
struct Notify;

impl Handle<Entry, Statement> for Notify {
    fn handle(
        &self,
        entry: &Entry,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Statement, HandlerOutcome>> {
        ready(Ok(Statement {
            account: entry.account.clone(),
            amount_cents: entry.amount_cents,
        }))
    }
}

/// Sends each reply under the key of the entry it answers, which the delivery reports in its
/// `partition-key` header.
struct UnderTheEntrysKey;

impl<C> PublishTransform<ForReply<C>, PubSubPublishOptions> for UnderTheEntrysKey {
    type Destination = Reads;

    fn apply(
        &self,
        _out: &mut Outgoing<'_>,
        options: &mut Option<PubSubPublishOptions>,
        cx: &PublishContext<'_, C>,
    ) {
        if let Some(key) = cx.headers().get_str(PARTITION_KEY_HEADER) {
            options
                .get_or_insert_with(PubSubPublishOptions::default)
                .ordering_key = Some(key.to_owned());
        }
    }
}
// --8<-- [end:notify]

// --8<-- [start:app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("ledger", "0.1.0")).with_broker(
        PubSubBroker::new("my-project").emulator("localhost:8085"),
        |b| {
            // The entries of one account reach one lane in publish order; eight lanes run side
            // by side.
            b.include(
                subscriber(
                    GooglePubSub::new("ledger-posting").create_with_topic("ledger"),
                    Post,
                )
                .workers_by_key(nonzero!(8))
                .build(),
            )
            .out(DefaultSlot, Publish::default())
            .build();
            b.include(
                subscriber(
                    GooglePubSub::new("ledger-statements").create_with_topic("ledger"),
                    Notify,
                )
                .reply()
                .build(),
            )
            .out_reply(Publish::default())
            .transform(UnderTheEntrysKey);
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
    use ruststream_gcp_pubsub::PARTITION_KEY_HEADER;
    use ruststream_gcp_pubsub::prelude::*;

    use super::{Entry, Posted, Statement, app};

    /// Publishes an entry the way the account service does: under the account's key.
    async fn publish_entry<State>(tb: &TestApp<State>, entry: &Entry) -> Result<(), Box<dyn Error>>
    where
        State: Send + Sync + 'static,
    {
        let mut headers = HeaderMap::new();
        headers.insert(PARTITION_KEY_HEADER, entry.account.clone());
        tb.broker::<PubSubBroker>()
            .message(entry)
            .with_headers(headers)
            .publish()
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn the_posting_keeps_the_accounts_key() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        publish_entry(
            &tb,
            &Entry {
                account: "acct-1".to_owned(),
                amount_cents: -1250,
            },
        )
        .await?;

        tb.out::<DefaultSlot>()
            .assert_called_once()
            .with_options(&PubSubPublishOptions {
                ordering_key: Some("acct-1".to_owned()),
            });
        tb.broker::<PubSubBroker>()
            .published::<Posted>("postings")
            .assert_called_once()
            .with(&Posted {
                account: "acct-1".to_owned(),
                amount_cents: -1250,
            })
            .with_header(PARTITION_KEY_HEADER, "acct-1");
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn the_statement_goes_out_under_the_entrys_key() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        publish_entry(
            &tb,
            &Entry {
                account: "acct-2".to_owned(),
                amount_cents: 990,
            },
        )
        .await?;

        tb.broker::<PubSubBroker>()
            .published::<Statement>("statements")
            .assert_called_once()
            .with(&Statement {
                account: "acct-2".to_owned(),
                amount_cents: 990,
            })
            .with_options(&PubSubPublishOptions {
                ordering_key: Some("acct-2".to_owned()),
            });
        tb.shutdown().await?;
        Ok(())
    }
}
