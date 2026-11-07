// The harness macros generate the group module, its items and the paths between them, and a
// benchmark function takes its setup value by value because the harness owns the drop; the
// crate's lints are written for the library surface, not for generated benchmark scaffolding.
#![allow(
    missing_docs,
    unused_qualifications,
    unreachable_pub,
    clippy::must_use_candidate,
    clippy::needless_pass_by_value
)]
//! Replying: the handler returns a value, the runtime encodes it and hands it to this crate's
//! publisher, which builds the Pub/Sub message and publishes it to the topic the reply type
//! declares through the client, waiting for the emulator to confirm it.

mod common;

use std::hint::black_box;

use common::{Latch, MESSAGES, Order, Pending};
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream_gcp_pubsub::prelude::*;
use serde::Serialize;

/// A reply with a destination of its own: the mount site adds nothing to it.
#[derive(Debug, Serialize, Outgoing)]
#[outgoing(name = "confirmations")]
struct Confirmation {
    id: u64,
}

#[subscriber(GooglePubSub::new("orders-workers"), reply)]
async fn confirm(order: &Order, ctx: &mut Context<'_, (), Latch>) -> Confirmation {
    ctx.state().arrived();
    Confirmation {
        id: black_box(order.id),
    }
}

fn app(messages: usize) -> Pending {
    common::pending(messages, |b| b.include(confirm))
}

// The client's batched acknowledgements put a fraction of an allocation on each delivery, so the
// floor is stated over a thousand of them. The longest run allocated 209,687 to 210,387 blocks
// over seven runs on google-cloud-pubsub 1.7, moved by the client's publish and lease timers,
// which fire more often on a loaded machine; the floor, 213,900, is the highest plus the widest
// spread seen on this scenario, 3,503. The client's own publish path is most of a reply: moving
// it from 1.3 to 1.6 added about 13 blocks per reply with this crate unchanged.
#[library_benchmark(config = common::config_every(99_535, 1_000, 14_830))]
#[bench::first(app(1))]
#[bench::base(app(MESSAGES))]
#[bench::twice(app(2 * MESSAGES))]
fn service(app: Pending) {
    common::start_and_drain(app);
}

library_benchmark_group!(name = reply_group; benchmarks = service);
main!(library_benchmark_groups = reply_group);
