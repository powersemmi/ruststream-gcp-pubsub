set shell := ["bash", "-eu", "-o", "pipefail", "-c"]
set dotenv-load := false

export PATH := env("HOME") + "/.cargo/bin:" + env("HOME") + "/.local/bin:" + env("PATH")

default: check

check:
    cargo fmt --all -- --check
    # The benchmark package is left out of the all-features legs on purpose: it is built with the
    # feature set a service ships, and the framework's harness feature is a compile error in it.
    # Its own leg follows each of them.
    cargo clippy --workspace --exclude ruststream-gcp-pubsub-bench --all-targets --all-features -- -D warnings
    cargo clippy -p ruststream-gcp-pubsub-bench --all-targets -- -D warnings
    cargo check --workspace --exclude ruststream-gcp-pubsub-bench --all-targets --all-features
    cargo check -p ruststream-gcp-pubsub-bench --all-targets
    cargo check --workspace --no-default-features

test:
    cargo test --workspace --all-features

brokers-up:
    docker compose -f docker-compose.test.yml up -d --wait

brokers-down:
    docker compose -f docker-compose.test.yml down -v

test-brokers: brokers-up
    #!/usr/bin/env bash
    set -euo pipefail
    trap 'just brokers-down' EXIT
    # This recipe starts the stand, so a gated test that skips itself here is a fault, not a
    # developer without a broker.
    PUBSUB_TEST_HOST=127.0.0.1:8085 \
    RUSTSTREAM_REQUIRE_LIVE=1 \
        cargo test --workspace --all-features -- --test-threads=1

# What this crate's consumer and publisher cost over the google-cloud-pubsub client they wrap: two
# scenarios, each run as a loop on this crate and as a loop on the client, against the emulator the
# tests use. On demand only - it takes minutes and it wants the machine to itself. The page it
# feeds is docs/benchmarks.md.
bench *ARGS: brokers-up
    #!/usr/bin/env bash
    set -euo pipefail
    trap 'just brokers-down' EXIT
    mkdir -p target
    # RUSTFLAGS is cleared so the numbers are not tied to this machine's CPU: a binary built with
    # `-C target-cpu=native` cannot be reproduced anywhere else.
    RUSTFLAGS="" PUBSUB_TEST_HOST=127.0.0.1:8085 \
    RUSTSTREAM_BENCH_OUT="$PWD/target/bench-paired.json" \
        cargo bench -p ruststream-gcp-pubsub-bench --bench paired {{ ARGS }}
    python3 scripts/bench_results.py target/bench-paired.json docs/benchmarks/results.json

# What a message costs a service on this crate, counted under valgrind: instructions through
# callgrind and allocations through DHAT, each scenario a service on the production broker against
# the emulator the tests use, with everything on the service's thread counted, the client's work
# included. It takes a few minutes and starts and stops the stand. The page it feeds is the code
# table of docs/benchmarks.md. RUSTFLAGS is cleared because valgrind aborts on the instructions a
# recent CPU advertises. Needs valgrind and the runner the benches pin:
# cargo install --locked gungraun-runner --version =0.19.4
# Extra arguments reach the runner: `just bench-code --save-baseline=main` records a baseline,
# `just bench-code --baseline=main` compares against it.
bench-code *ARGS: brokers-up
    #!/usr/bin/env bash
    set -euo pipefail
    trap 'just brokers-down' EXIT
    mkdir -p target
    RUSTFLAGS="" PUBSUB_TEST_HOST=127.0.0.1:8085 \
        cargo bench -p ruststream-gcp-pubsub-bench --bench consume --bench reply --bench batch \
        -- --output-format=json {{ ARGS }} > target/bench-code.json
    python3 scripts/bench_results.py --code target/bench-code.json docs/benchmarks/results.json

fmt:
    cargo fmt --all

build:
    cargo build --workspace --release

security: deny zizmor

# Dependency-graph checks (advisories, licenses, duplicates, sources).
# Needs cargo-deny: cargo install cargo-deny --locked
deny:
    cargo deny check

zizmor:
    uvx zizmor .github/workflows

typo:
    uvx codespell

clean:
    cargo clean

ci: check test typo security
