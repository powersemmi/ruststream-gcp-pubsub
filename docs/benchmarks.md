# Benchmarks

This crate sits between the Pub/Sub client and a service: it opens the streaming pull, turns each
delivery into a message with its headers, settles it, and publishes on the way back. This page says
what that costs, measured against the same work written by hand on `google-cloud-pubsub`.

Three loops in one process run the same scenario, and they differ in one thing each: what carries
the messages.

- **Raw client** drives `google-cloud-pubsub` directly.
- **Adapter** drives this crate and nothing above it: its subscription, the deliveries it yields
  and its publisher, pulled and settled by a loop with no service around it.
- **Service** is the whole thing you would write: a subscriber handler, the app and the runtime.

The adapter column against the raw one is what this crate costs, and it is the number this
repository answers for. The service column against the adapter one is what the runtime costs on top
of it, over Pub/Sub in particular.

Everything else is held equal - the endpoint and the credentials, the topology, the streaming-pull
settings, the position of the ack, the decode into the same type, the payload bytes, the tokio
runtime and the build. The procedure is the framework's own and is described on the
[RustStream benchmarks page](https://powersemmi.github.io/ruststream/latest/benchmarks/#methodology);
this page publishes what it produced here.

Every figure below was taken against the local emulator that `docker-compose.test.yml` starts, and
none of them against the hosted service.

## The numbers

Medians over interleaved pairs, with the observed spread in parentheses. Higher is better.

<div id="benchmark-results" data-benchmark-labels='{"loading": "Loading the published results...", "scenario": "Scenario", "raw": "Raw client", "adapter": "Adapter", "framework": "Service", "adapterOverhead": "Adapter overhead", "overhead": "Service overhead", "indistinguishable": "indistinguishable", "brokerBound": "broker-bound", "machine": "Machine", "os": "OS", "broker": "Broker", "roundTrip": "Round trip", "build": "Build", "versions": "Versions", "measured": "Measured", "unavailable": "No results could be read. They are published at {url}.", "unknownSchema": "The published results declare schema {schema}, which this page does not render."}'></div>

The table is read in your browser from the document the last run wrote, so nothing on this page is
a copy that could have gone stale.

The second scenario publishes under ordering keys, which is the delivery shape a subscription with
ordering enabled has to keep in order per key. It also buys the adapter one piece of work the first
scenario does not have: the key is carried into the delivery's headers, where a service that names
no broker reads it as a partition key. A loop on the client reads the field on the message and
copies nothing.

A row reported as `indistinguishable` is one whose two halves differ by less than the spread between
runs of either. That is the honest outcome wherever the transport costs far more than the dispatch.
A figure below the run-to-run noise would read as precision that was never measured, so none is
published.

The machine-readable form of the same run, which the framework's site reads to build its
cross-broker table, is at
[`benchmarks/results.json`](https://powersemmi.github.io/ruststream-gcp-pubsub/latest/benchmarks/results.json).

## The machine

<div id="benchmark-environment"></div>

The build flags are published with the numbers because they change them: a binary built with
`-C target-cpu=native` produces a figure no other machine can reproduce, so the recipe clears the
variable before it builds.

## What they do not mean

The emulator is not the service. It answers the same API over plaintext on the loopback, and it
leaves out the TLS handshake, the credential refresh and the round trip to a region. Both halves of
a pair talk to the same emulator, so the comparison between them holds; the rate itself is a local
number, and nothing here predicts what a subscription delivers from a region.

That is also why the `broker-bound` flag is worth reading first. A row carrying it was paced by the
emulator, so both halves spent the run in the same waiting and the difference between them is a
lower bound on what dispatch costs rather than a measurement of it. The flag is decided by
arithmetic: a probe outside the pairs times one round trip to the emulator, a delivery is charged
one of them for its acknowledgement, and the row is marked when that product covers at least half
the time one message took. The round trip is published with the machine below, so the arithmetic
can be checked. The two rows here fall on either side of that line by less than a percent, so the
flag's absence from the first one is not a claim that the consumer was ever the limit.

Flow control is the other thing the emulator does not reproduce. It delivers as fast as it can,
whatever limit the subscription carries, so neither half meets the pause a hosted subscription
applies to a consumer that falls behind.

This is one consumer, one subscription, a small body and an emulator on the loopback. It measures
what a delivery costs in this crate, not what Pub/Sub can carry, and a row here is not comparable
with a row published for another broker: the transports do different work per message.

The window a run measures opens at the first delivery and closes when the last one is settled, on
both halves alike.

The numbers are a snapshot of one machine on one day. They are re-measured on demand, never in CI:
a shared runner's noise is larger than the difference this page is about.

## Running it yourself

```bash
just bench
```

The recipe starts the emulator from `docker-compose.test.yml`, runs both scenarios, stops it again
and rewrites `docs/benchmarks/results.json` with what it measured. It takes about ten minutes and
wants the machine to itself. The message count is not fixed: a probe run sets it so that every
measured run lasts at least five seconds on whatever machine it is taken on.
