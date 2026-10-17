---
title: Benchmarks
description: "What ruststream-gcp-pubsub costs on every message: throughput against the raw google-cloud-pubsub client, and instructions and allocations of its code."
---

import {MovedSections, PubSubEnvironment, PubSubResults} from '@site/src/components/Benchmarks';

export const labels = {"loading": "Loading the results...", "scenario": "Scenario", "raw": "Raw client", "adapter": "Broker crate", "framework": "RustStream", "overhead": "Overhead", "against": "({percent} over the client)", "indistinguishable": "indistinguishable", "brokerBound": "broker-bound", "measured": "measured", "details": "How the numbers were taken", "pending": "No results published yet: {brokers}.", "instructions": "Instructions", "allocations": "Allocations", "cold": "Cold start", "table": "Show the numbers as a table", "axis": "How far each loop falls below the raw client's best round", "rounds": "rounds, best to slowest", "best": "best round", "median": "median round", "noise": "spread of the rounds", "comparison": "Against a raw client", "code": "Cost of the code", "broker": "The broker", "plugin": "The {plugin} plugin", "pluginRaw": "Without the plugin", "pluginAdapter": "Plugin written by hand", "pluginFramework": "The crate's plugin", "pluginAxis": "How far each loop falls below the best round of the app without the plugin", "pluginAgainst": "({percent} over no plugin)", "pluginOverhead": "Over the plugin by hand", "pluginOverHand": "({percent} over by hand)"};

export const machine = {"cpu": "Processor", "architecture": "Architecture", "cpu_frequency": "Frequency", "cores": "Cores", "memory": "Memory", "memory_speed": "Memory speed", "os": "Operating system", "broker": "Broker", "rustc": "Compiler", "profile": "Build profile", "features": "Features", "rustflags": "RUSTFLAGS", "round_trip": "Round trip", "valgrind": "Valgrind"};

# Benchmarks

What `ruststream-gcp-pubsub` costs on every message: throughput against the raw
`google-cloud-pubsub` client, and instructions and allocations of its code.

## Against a raw client

The first chart runs each scenario three ways against the local Pub/Sub emulator. `Raw client`
drives `google-cloud-pubsub` directly. `Broker crate` drives this crate and nothing above it: its
subscription, the deliveries it yields and its publisher, pulled and settled by a loop with no
service around it. `RustStream` is the service a user writes: a `#[subscriber]` handler, the app
and the runtime. The first gap is what this crate costs over the client it wraps, and it is the
number this repository answers for. The second is what the runtime adds on top, over Pub/Sub in
particular.

Each loop is a bar from its best round to its slowest, with a dot at the best round and a notch at
the median. The zero of the axis is the raw client's best round, and a dot right of zero is slower.
A hollow dot is a loop whose difference from the raw client is smaller than the spread of the
rounds. The table under the chart prints the best round with the median in parentheses.

The second scenario publishes under ordering keys, the delivery shape a subscription with ordering
enabled keeps in order per key. It also gives this crate one piece of work the first scenario does
not have: the key is carried into the delivery's headers, where a service that names no broker
reads it as a partition key. A loop on the client reads the field on the message and copies
nothing.

## Cost of the code

The second chart counts what one message costs on the service's thread: instructions under
callgrind and allocations under DHAT, in the steady state. Each scenario is the service a user
writes, built on `PubSubBroker` and started against the emulator of the same stand. Everything on
the service's thread is counted: the framework's dispatch, this crate's code and the
`google-cloud-pubsub` client, whose streaming pull, acknowledgements, lease extensions and publish
requests run there. `Cold start` is what starting the service and taking the first delivery cost
once.

Most of every row is the client. The reply row is the largest: the runtime waits for each reply to
be confirmed before it takes the next delivery, so every reply leaves in a publish request of its
own. The cold start is almost all the client too: it builds two administration clients, each of
which reads and parses the machine's root certificates, so that column depends on the machine's
certificate store.

## Results

<PubSubResults labels={labels} details="/architecture/benchmarks/methodology/" />

## The machine

<PubSubEnvironment labels={machine} />

The build flags are published with the numbers because they change them: a binary built with
`-C target-cpu=native` produces a figure no other machine can reproduce, so the recipe clears the
variable before it builds.

The numbers come from `docs/benchmarks/results.json` of this version, written by the benchmark
recipe. The framework's site reads the same document, published at
[`benchmarks/results.json`](https://powersemmi.github.io/ruststream-gcp-pubsub/latest/benchmarks/results.json),
to build its table across brokers. [Methodology](methodology.md) says how the numbers were taken,
and [Numbers](numbers.md) what they allow you to conclude.

<MovedSections />
