#!/usr/bin/env python3
"""Turn a benchmark run into the published results document.

`benches/paired.rs` reports the scenarios it measured and nothing else, because the machine and
the build are not its to describe. This script reads that summary, adds the environment the run
was taken in and the versions it was taken against, and writes the document the documentation
site serves at `benchmarks/results.json`.

The schema is the core's, declared at
https://powersemmi.github.io/ruststream/latest/benchmarks/#publishing-results. This crate
publishes the throughput table alone, so the document declares schema 1.

The target is the local emulator, and the environment block says so down to the SDK release it
ships in: a rate taken against it is a statement about dispatch cost and about nothing a hosted
subscription does around a delivery. The round trip the benchmark probed travels with it, because
that figure is what decides whether a row was paced by the transport.

A field the machine does not publish is written as `unknown` rather than guessed: memory speed
comes from the DMI tables, which most systems only let root read.

    python3 scripts/bench_results.py target/bench-paired.json docs/benchmarks/results.json
"""

import json
import re
import subprocess
import sys
from datetime import date
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
COMPOSE = REPO / "docker-compose.test.yml"
MANIFEST = REPO / "Cargo.toml"
LOCK = REPO / "Cargo.lock"

# What `just bench` builds the benchmark with. Both are recipe decisions rather than machine
# facts, so they are stated here next to the recipe rather than sniffed.
PROFILE = "bench, inheriting release (opt-level = 3, lto = false, codegen-units = 16)"
# The client version rides the build line rather than a field of its own: the schema fixes
# the set of environment keys, and what this crate wraps is part of what was built.
FEATURES = "ruststream-gcp-pubsub default (none), ruststream macros,json"
RUSTFLAGS = "none (the recipe clears RUSTFLAGS, so the numbers are not tied to this CPU)"


def run(*args: str) -> str:
    try:
        return subprocess.run(args, check=True, capture_output=True, text=True).stdout
    except (OSError, subprocess.CalledProcessError):
        return ""


def proc_field(path: str, key: str) -> str:
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        name, _, value = line.partition(":")
        if name.strip() == key:
            return value.strip()
    return ""


def lscpu() -> dict[str, str]:
    fields = {}
    for line in run("lscpu").splitlines():
        name, _, value = line.partition(":")
        fields[name.strip()] = value.strip()
    return fields


def cores(cpu: dict[str, str]) -> str:
    physical = cpu.get("Core(s) per socket", "")
    sockets = cpu.get("Socket(s)", "1")
    logical = cpu.get("CPU(s)", "")
    if not physical or not logical:
        return "unknown"
    return f"{int(physical) * int(sockets)} physical, {logical} logical"


def frequency(cpu: dict[str, str]) -> str:
    low, high = cpu.get("CPU min MHz", ""), cpu.get("CPU max MHz", "")
    if not low or not high:
        return "unknown"
    return f"{float(low.replace(',', '.')):.0f}-{float(high.replace(',', '.')):.0f} MHz"


def memory() -> str:
    total = proc_field("/proc/meminfo", "MemTotal")
    if not total.endswith(" kB"):
        return "unknown"
    return f"{int(total[:-3]) / (1024 * 1024):.1f} GiB"


def emulator_release() -> str:
    """The SDK release the running emulator ships in, asked of the container itself.

    The compose file pins a floating tag, so the tag alone does not identify what was measured.
    The stand is still up when this runs; when it is not, the tag has to stand on its own.
    """
    version = run(
        "docker", "compose", "-f", str(COMPOSE), "exec", "-T", "pubsub", "gcloud", "version"
    )
    match = re.search(r"Google Cloud SDK (\S+)", version)
    return f"Google Cloud SDK {match.group(1)}" if match else ""


def broker_image() -> str:
    match = re.search(r"^\s+image:\s*(\S+)", COMPOSE.read_text(encoding="utf-8"), re.M)
    if not match:
        return "unknown"
    release = emulator_release()
    detail = f", {release}" if release else ""
    return f"Pub/Sub emulator, {match.group(1)}{detail}, in Docker on localhost"


def crate_version() -> str:
    match = re.search(r'^version = "([^"]+)"', MANIFEST.read_text(encoding="utf-8"), re.M)
    return match.group(1) if match else "unknown"


def core_version() -> str:
    match = re.search(
        r'^name = "ruststream"\nversion = "([^"]+)"', LOCK.read_text(encoding="utf-8"), re.M
    )
    return match.group(1) if match else "unknown"


def client_version() -> str:
    match = re.search(
        r'^name = "google-cloud-pubsub"\nversion = "([^"]+)"',
        LOCK.read_text(encoding="utf-8"),
        re.M,
    )
    return match.group(1) if match else "unknown"


def environment(round_trip: str) -> dict[str, str]:
    cpu = lscpu()
    return {
        "cpu": proc_field("/proc/cpuinfo", "model name") or cpu.get("Model name", "unknown"),
        "architecture": cpu.get("Architecture", "unknown"),
        "cpu_frequency": frequency(cpu),
        "cores": cores(cpu),
        "memory": memory(),
        "memory_speed": "unknown",
        "os": f"Linux {run('uname', '-r').strip()}",
        "broker": broker_image(),
        # What decides the `broker_bound` flag on every row, published so a reader can redo the
        # arithmetic: round trips per delivery times this against the time one message took.
        "round_trip": round_trip,
        "rustc": run("rustc", "--version").replace("rustc", "").strip().split()[0],
        "profile": PROFILE,
        "features": f"{FEATURES}; google-cloud-pubsub {client_version()}",
        "rustflags": RUSTFLAGS,
    }


def main() -> int:
    if len(sys.argv) != 3:
        print(__doc__, file=sys.stderr)
        return 2
    summary = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    document = {
        "schema": 1,
        "crate": "ruststream-gcp-pubsub",
        "crate_version": crate_version(),
        "core_version": core_version(),
        "measured_at": date.today().isoformat(),
        "environment": environment(summary["round_trip"]),
        "scenarios": summary["scenarios"],
    }
    out = Path(sys.argv[2])
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
    print(f"wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
