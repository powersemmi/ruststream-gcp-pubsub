//! What a publish hands to the client: the payload buffer the framework wrote, and the header map
//! the transforms filled.
//!
//! Both travel rather than being copied, and content equality cannot say so - the bytes are equal
//! either way. The payload is read off its address; the map is read off this thread's allocation
//! count, because `Bytes` keeps its data pointer across a clone.
#![cfg(feature = "testing")]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::time::Duration;

use bytes::Bytes;
use ruststream::testing::expect_published;
use ruststream::{Broker, BytesMut, ConnectedBroker, HeaderMap, OutgoingMessage, Publisher};
use ruststream_gcp_pubsub::testing::{PubSubTestBroker, PubSubTestPublisher};

const WAIT: Duration = Duration::from_secs(1);

/// Counts this thread's allocations. A thread-local count rather than a global one: the test
/// binary runs other tests beside this one, and their allocations are none of this
/// measurement's business.
struct Counting;

thread_local! {
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.with(|count| count.set(count.get() + 1));
        // SAFETY: the layout is the caller's, forwarded unchanged to the system allocator.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: the pointer and layout are the caller's, forwarded unchanged.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// What this thread has allocated so far.
fn allocations() -> usize {
    ALLOCATIONS.with(Cell::get)
}

fn two_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert("x-tenant", Bytes::from_static(b"acme"));
    headers.insert("x-region", Bytes::from_static(b"eu-central-1"));
    headers
}

/// The allocations one publish to `topic` costs, with the log entry for that topic already in
/// place: the first message under a name grows tables that later ones do not.
async fn one_publish(publisher: &PubSubTestPublisher, topic: &str, headers: HeaderMap) -> usize {
    let warmup =
        OutgoingMessage::produced(topic, BytesMut::from(&b"{}"[..])).with_headers(headers.clone());
    publisher.publish(warmup, None).await.expect("publish");

    let measured =
        OutgoingMessage::produced(topic, BytesMut::from(&b"{}"[..])).with_headers(headers);
    let before = allocations();
    publisher.publish(measured, None).await.expect("publish");
    allocations() - before
}

/// Pub/Sub keeps the payload, so the buffer the framework wrote reaches the router rather than a
/// copy of it, and the in-process transport answers the same way.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn published_payload_is_the_buffer_the_framework_wrote() {
    let broker = PubSubTestBroker::new().connect().await.expect("connect");
    let publisher = broker.publisher();
    let payload = BytesMut::from(&b"first"[..]);
    let written_at = payload.as_ptr();

    publisher
        .publish(OutgoingMessage::produced("events", payload), None)
        .await
        .expect("publish");

    let observed = expect_published(&broker, "events", 1, WAIT).await;
    assert_eq!(
        observed[0].payload().as_ptr(),
        written_at,
        "the transport keeps the payload, so it takes the buffer instead of copying it",
    );
    broker.shutdown().await.expect("shutdown");
}

/// A publish carrying two headers costs exactly one copy of the map more than a publish carrying
/// none, and that copy is the published log's snapshot. A publisher that cloned the map on the
/// way in would cost two.
#[tokio::test]
async fn a_publish_copies_the_header_map_once() {
    let broker = PubSubTestBroker::new().connect().await.expect("connect");
    let publisher = broker.publisher();

    let headers = two_headers();
    // The first map copied in this process brings the hash table's own machinery up; what a copy
    // costs from then on is the table.
    drop(headers.clone());
    let before = allocations();
    let copy = headers.clone();
    let one_copy = allocations() - before;
    drop(copy);

    let bare = one_publish(&publisher, "events.plain", HeaderMap::new()).await;
    let carried = one_publish(&publisher, "events.other", headers).await;

    assert_eq!(
        carried - bare,
        one_copy,
        "the publisher hands the map it was given to the router; only the published log keeps a \
         copy of its own",
    );
    broker.shutdown().await.expect("shutdown");
}
