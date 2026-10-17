---
title: Methodology
description: "How ruststream-gcp-pubsub measures itself: the three loops, the run against the emulator, the broker-bound arithmetic and the code count."
---

# Methodology

How `ruststream-gcp-pubsub` measures itself: the three loops, the run against the emulator, the
broker-bound arithmetic and the code count. The procedure is the framework's own
([RustStream methodology](https://powersemmi.github.io/ruststream/latest/architecture/benchmarks/methodology/)),
and this page states what it is on Pub/Sub.

## The three loops

One process runs each scenario three times over, as three loops that differ in one thing: what
carries the messages.

- **Raw client.** `google-cloud-pubsub`, driven directly by a loop written by hand.
- **Broker crate.** This crate and nothing above it: its subscription, the deliveries it yields and
  its publisher, pulled and settled by a loop with no service around it.
- **RustStream.** The service a user writes: a `#[subscriber]` handler, the app and the runtime.

Everything else is equal across the three: the endpoint and the credentials, the topology, the
streaming-pull settings, the place of the ack, the decode into the same type, the payload bytes,
the tokio runtime and the build.

Every figure was taken against the local emulator that `docker-compose.test.yml` starts, and none
of them against the hosted service.

## The run

- **The window opens at the first delivery and closes when the last one is settled**, in every
  loop alike.
- **The message count comes from a probe**, which sets it so that every measured run lasts at least
  five seconds on whatever machine it is taken on.
- **The loops are interleaved**, three rounds. Each loop reports its best, median and worst round.

A row is `broker-bound` when it was paced by the emulator. That is arithmetic: a probe outside the
rounds times one round trip to the emulator, a delivery is charged one of them for its
acknowledgement, and the row is flagged when that product covers at least half the time one
message took. The round trip is published with the machine, so the arithmetic can be checked.

## The code count

Each scenario of the [cost chart](index.md#cost-of-the-code) is the service a user writes, built on
`PubSubBroker` and started against the emulator of the same stand. The service runs on one thread,
and everything on that thread is counted: the framework's dispatch, this crate's code and the
`google-cloud-pubsub` client, whose streaming pull, acknowledgements, lease extensions and publish
requests run there. The emulator is another process and is not in the number. The messages are
published from another thread before the drain starts, and that work is not in the number either.

Instructions come from callgrind and allocations from DHAT. Each is per message in the steady
state: the slope between a run of 1000 deliveries and a run of 2000. The cold start is what
starting the service and taking the first delivery cost once.

The client acknowledges in batches and extends leases on timers, so a count moves a little from
one run to the next: over four runs the instructions per message agreed within 1.2 percent, and
the allocations of the longest run within 640 on the reply row and within 20 on the other two.
`just bench-code` fails on an allocation above the floor a scenario declares: the highest count
seen plus a margin at least as large as that spread, and a tenth of a percent at the least. With
`--baseline=main` it also fails on more than two percent more instructions. A pull request that
changes the cost cites its numbers.

## Running it yourself

```bash
just bench
```

The recipe starts the emulator from `docker-compose.test.yml`, runs both scenarios, stops it again
and rewrites `docs/benchmarks/results.json` with what it measured. It takes a few minutes and
wants the machine to itself. It clears `RUSTFLAGS`, so the binary is not tied to the CPU it was
built on.

```bash
just bench-code
```

The recipe starts the emulator, counts the code chart under valgrind, stops the emulator again and
rewrites the `code` section of the same document. It takes a few minutes. Besides Docker it needs
valgrind. The benchmark runner has to be the `gungraun` release `Cargo.lock` pins: the recipe
installs it into `target/` on its first run and again after the release changes.

A number after the recipe name sets the deliveries per measured run (`just bench-code 500`); the
published numbers are measured at the default of 1000. Any other argument goes to the benchmark
runner, such as `--save-baseline=main` or `--baseline=main`. A run that breaches a limit still
measures every scenario and prints the table. Every breached limit follows the table, with the
value it was compared against next to the new one, and the run fails.
