---
title: Numbers
description: "What the benchmark numbers of ruststream-gcp-pubsub measure, and what they do not allow you to conclude."
---

# Numbers

What the benchmark numbers of `ruststream-gcp-pubsub` measure, and what they do not allow you to
conclude.

## The comparison against a raw client

The three loops of a row are comparable with each other, because they ran in one process,
interleaved, against one emulator. A row is not comparable with a row another broker crate
publishes: the transports do different work per message.

An overhead reported as `indistinguishable` is one where that loop and the raw client differ by less
than the spread between runs of either. That is the honest outcome wherever the transport costs far
more than the dispatch. A figure below the run-to-run noise would read as precision that was never
measured, so none is published.

A `broker-bound` row was paced by the emulator, so every loop spent the run in the same waiting. The
difference between them is then a lower bound on what dispatch costs, not a measurement of it. The
first row sits within a few percent of that line and crosses it from one run to the next, so
whether it carries the flag says nothing about whether the consumer was ever the limit.

## The cost of the code

The counts are absolute, the client's work and the framework's included; the core publishes the
framework's cost alone on its
[benchmarks page](https://powersemmi.github.io/ruststream/latest/architecture/benchmarks/). The same
count costs more time where it misses the cache, so a count is not a time.

Every figure is the steady state, the slope between a run of 1000 deliveries and one of 2000.
Starting the service and taking the first delivery are the cold start, paid once.

## What they do not mean

The emulator is not the service. It answers the same API over plaintext on the loopback, and it
leaves out the TLS handshake, the credential refresh and the round trip to a region. Every loop
talks to the same emulator, so the comparison between them holds; the rate itself is a local
number, and nothing here predicts what a subscription delivers from a region.

Flow control is the other thing the emulator does not reproduce. It delivers as fast as it can,
whatever limit the subscription carries, so no loop meets the pause a hosted subscription applies
to a consumer that falls behind.

A row is one consumer, one subscription, a small body and an emulator on the loopback. It measures
what a delivery costs in this crate, not what Pub/Sub can carry.

The numbers are a snapshot of one machine on one day, with the build recorded beside them. They are
measured again by hand, on a machine given to the run alone: the differences they show are smaller
than the noise of a shared one.
