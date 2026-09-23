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

The best of three interleaved rounds, with the median round in parentheses. Higher is better.

<div id="benchmark-results" data-benchmark-labels='{"loading": "Loading the published results...", "scenario": "Scenario", "raw": "Raw client", "adapter": "Adapter", "framework": "Service", "adapterOverhead": "Adapter overhead", "overhead": "Service overhead", "indistinguishable": "indistinguishable", "brokerBound": "broker-bound", "machine": "Machine", "os": "OS", "broker": "Broker", "roundTrip": "Round trip", "build": "Build", "versions": "Versions", "measured": "Measured", "instructions": "Instructions per message", "allocations": "Allocations per message", "cold": "Cold start (instructions / allocations)", "unavailable": "No results could be read. They are published at {url}.", "unknownSchema": "The published results declare schema {schema}, which this page does not render."}'></div>

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

## The crate's own code

<div id="benchmark-code"></div>

The second table counts what one message costs on the service's thread rather than timing it:
instructions under callgrind and allocations under DHAT. Each scenario is the service a user
writes, built on `PubSubBroker` and started against the emulator of the same stand. The service
runs on one thread, and everything on that thread is counted: the framework's dispatch, this
crate's code and the `google-cloud-pubsub` client, whose streaming pull, acknowledgements, lease
extensions and publish requests run there. The emulator is another process and is not in the
number. The messages are published from another thread before the drain starts, and that work is
not in the number either.

Instructions and allocations are per message in the steady state: the slope between a run of 1000
deliveries and a run of 2000. The last column is what starting the service and taking the first
delivery cost once. The numbers are absolute, the client's work and the framework's included; the
core publishes the framework's cost alone on its
[benchmarks page](https://powersemmi.github.io/ruststream/latest/benchmarks/).

Most of every row is the client. The reply row is the largest: the runtime waits for each reply to
be confirmed before it takes the next delivery, so every reply leaves in a publish request of its
own. The cold start is almost all the client too: it builds two administration clients, each of
which reads and parses the machine's root certificates, so that column depends on the machine's
certificate store.

The client acknowledges in batches and extends leases on timers, so a count moves a little from
one run to the next: over three runs the instructions per message agreed within 1.1 percent, and
the allocations of the longest run within 40. `just bench-code` fails on an allocation above the
floor a scenario declares - the highest count seen plus a margin of 100 for those timers - and with
`--baseline=main` on more than two percent more instructions, and a pull request that changes the
cost cites its numbers.

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
can be checked. The first row sits within a few percent of that line and crosses it from one run
to the next, so whether it carries the flag says nothing about whether the consumer was ever the
limit.

Flow control is the other thing the emulator does not reproduce. It delivers as fast as it can,
whatever limit the subscription carries, so neither half meets the pause a hosted subscription
applies to a consumer that falls behind.

This is one consumer, one subscription, a small body and an emulator on the loopback. It measures
what a delivery costs in this crate, not what Pub/Sub can carry, and a row here is not comparable
with a row published for another broker: the transports do different work per message.

The window a run measures opens at the first delivery and closes when the last one is settled, on
both halves alike.

The numbers are a snapshot of one machine on one day. They are re-measured by hand, on a machine
given to the run alone: the difference this page is about is smaller than the noise of a shared one.

## Running it yourself

```bash
just bench
```

The recipe starts the emulator from `docker-compose.test.yml`, runs both scenarios, stops it again
and rewrites `docs/benchmarks/results.json` with what it measured. It takes a few minutes and
wants the machine to itself. The message count is not fixed: a probe run sets it so that every
measured run lasts at least five seconds on whatever machine it is taken on.

```bash
just bench-code
```

The recipe starts the emulator, counts the code table under valgrind, stops the emulator again and
rewrites the `code` section of the same document. It takes a few minutes. Besides Docker it needs
valgrind and the benchmark runner: `cargo install --locked gungraun-runner --version =0.19.4`.
