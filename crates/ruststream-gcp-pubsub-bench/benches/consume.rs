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
//! Consuming a small JSON body: the streaming pull yields a delivery, this crate wraps it in its
//! message, the dispatcher decodes it into a struct, the handler reads a field, and the runtime
//! acks it through the client.

mod common;

use std::hint::black_box;

use common::{Latch, MESSAGES, Order, Pending};
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream_gcp_pubsub::prelude::*;

#[subscriber(GooglePubSub::new("orders-workers"))]
async fn consume(order: &Order, ctx: &mut Context<'_, (), Latch>) -> HandlerOutcome {
    black_box((order.id, order.quantity));
    ctx.state().arrived();
    HandlerOutcome::ack()
}

fn app(messages: usize) -> Pending {
    common::pending(messages, |b| {
        b.include(consume);
    })
}

// The client's batched acknowledgements put a fraction of an allocation on each delivery, so the
// floor is stated over a thousand of them. The longest run allocated 18,130 to 18,135 blocks over
// six runs; the floor, 18,154, is the highest plus a tenth of a percent.
#[library_benchmark(config = common::config_every(5_324, 1_000, 7_506))]
#[bench::first(app(1))]
#[bench::base(app(MESSAGES))]
#[bench::twice(app(2 * MESSAGES))]
fn service(app: Pending) {
    common::start_and_drain(app);
}

library_benchmark_group!(name = consume_group; benchmarks = service);
main!(library_benchmark_groups = consume_group);
