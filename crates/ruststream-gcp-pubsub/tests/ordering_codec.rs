//! An ordering key must not change which codec a publish encodes with.
//!
//! The key is an adapter on the publisher, and an adapter naming no codec of its own used to send
//! the builder back to the crate default: a service that chose a codec at its mount site got one
//! wire format for keyed messages and another for every other message it sent. The two handlers
//! here differ only in the key, so a difference in the bytes is the adapter's doing.

#![cfg(feature = "testing")]

use bytes::BytesMut;
use ruststream::Outgoing;
use ruststream::codec::{Codec, CodecError, JsonCodec};
use ruststream::runtime::Out;
use ruststream::testing::TestApp;
use ruststream_gcp_pubsub::PARTITION_KEY_HEADER;
use ruststream_gcp_pubsub::prelude::*;
use ruststream_gcp_pubsub::testing::{PubSubTestBroker, PubSubTestPublish};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

/// A codec that cannot be mistaken for the default one: every payload it writes opens with a
/// marker byte json never emits.
#[derive(Debug, Clone, Copy, Default)]
struct MarkerCodec;

const MARKER: u8 = 0xAB;

impl Codec for MarkerCodec {
    fn encode<T: Serialize>(&self, value: &T) -> Result<BytesMut, CodecError> {
        let mut out = BytesMut::new();
        out.extend_from_slice(&[MARKER]);
        out.extend_from_slice(&JsonCodec.encode(value)?);
        Ok(out)
    }

    fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, CodecError> {
        JsonCodec.decode(bytes.strip_prefix(&[MARKER]).unwrap_or(bytes))
    }
}

#[derive(Debug, Serialize, Deserialize, Outgoing)]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
struct Forwarded {
    id: u64,
}

/// Publishes straight through the slot.
#[subscriber("orders-plain")]
async fn plain(order: &Order, Out(out): Out<impl Publisher>) -> HandlerOutcome {
    if out
        .message(&Forwarded { id: order.id })
        .to("copies-plain")
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

/// The same publish, with an ordering key named on the way.
#[subscriber("orders-keyed")]
async fn keyed(order: &Order, Out(out): Out<impl PubSubOrdering>) -> HandlerOutcome {
    let ordered = out.with_ordering_key(format!("order-{}", order.id));
    if ordered
        .message(&Forwarded { id: order.id })
        .to("copies-keyed")
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_ordering_key_does_not_change_which_codec_applies() {
    let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        PubSubTestBroker::new(),
        |b| {
            b.include(plain)
                .out(DefaultSlot, PubSubTestPublish)
                .codec(MarkerCodec)
                .build();
            b.include(keyed)
                .out(DefaultSlot, PubSubTestPublish)
                .codec(MarkerCodec)
                .build();
        },
    );
    let tb = TestApp::start(app)
        .await
        .expect("the harness starts the app");

    for (source, id) in [("orders-plain", 1_u64), ("orders-keyed", 2)] {
        tb.broker::<PubSubTestBroker>()
            .message(&Order { id })
            .to(source)
            .publish()
            .await
            .expect("the harness accepts the injection");
    }
    tb.settle().await.expect("the handlers settle");

    // Both publishes carry the codec the mount site named, and the keyed one still carries its
    // key.
    let broker = tb.broker::<PubSubTestBroker>();
    broker
        .published::<Forwarded>("copies-plain")
        .assert_called_once()
        .with_codec(&MarkerCodec, &Forwarded { id: 1 });
    broker
        .published::<Forwarded>("copies-keyed")
        .assert_called_once()
        .with_codec(&MarkerCodec, &Forwarded { id: 2 })
        .with_header(PARTITION_KEY_HEADER, "order-2");

    tb.shutdown().await.expect("graceful shutdown");
}
