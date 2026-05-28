# Pulse vs Mosquitto Benchmarks

This directory contains local-only benchmark tooling for comparing Pulse with
the official Eclipse Mosquitto broker installed through Homebrew. It is excluded
from Cargo packages with `exclude = ["benchmark/**"]`.

## Prerequisites

Install Mosquitto:

```sh
brew update
brew install mosquitto
```

Build Pulse in release mode:

```sh
cargo build --release
```

Homebrew installs the Mosquitto broker at
`/opt/homebrew/opt/mosquitto/sbin/mosquitto` on Apple Silicon. The benchmark
uses that path automatically when `mosquitto` is not on `PATH`.

## Run

Smoke test:

```sh
python3 benchmark/run.py --messages 100 --fanout-subscribers 5
```

Default run:

```sh
python3 benchmark/run.py
```

Useful options:

```sh
python3 benchmark/run.py \
  --messages 10000 \
  --payload-bytes 128 \
  --fanout-subscribers 100 \
  --pulse-bin target/release/Pulse \
  --mosquitto-bin /opt/homebrew/opt/mosquitto/sbin/mosquitto
```

## Scenarios

- `qos0-throughput`: one publisher, one subscriber, QoS 0 delivery.
- `qos1-throughput`: publisher waits for PUBACK and subscriber sends PUBACK.
- `qos2-throughput`: full PUBREC/PUBREL/PUBCOMP handshake on both sides.
- `retained-fanout`: publish one retained message, then measure retained replay
  to many already-connected subscribers.

The output reports elapsed seconds and deliveries per second for each scenario.
