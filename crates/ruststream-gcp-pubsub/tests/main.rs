//! The integration suite, one test binary: each file under `tests/` is a module of it.

mod live;

mod asyncapi_pubsub;
mod conformance_pubsub;
mod deadlines_pubsub;
mod harness_manual;
mod harness_pubsub;
mod in_process_pubsub;
mod integration_pubsub;
mod prelude_pubsub;
mod publish_parts;
mod topology_pubsub;
