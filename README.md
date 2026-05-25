![Pulse logo](doc/img/logo.png)
# Pulse

Fast, persistent MQTT v5 broker for Rust-powered systems.


Pulse is a compact MQTT broker built on top of
[rs-netty](https://github.com/PieceOfFall/rs-netty). It is designed for the
place where MQTT usually has to be boring in the best possible way: edge
gateways, device backplanes, lab networks, local-first products, and internal
message fabrics that need durable sessions without dragging in a giant service.

## Why Pulse

- MQTT v5 broker semantics with CONNECT, SUBSCRIBE, UNSUBSCRIBE, PUBLISH,
  PING, DISCONNECT, will messages, keep alive, topic aliases, reason strings,
  and packet-size limits.
- QoS 0, QoS 1, and QoS 2 delivery paths, including outbound inflight tracking,
  duplicate QoS 2 handling, redelivery after reconnect, and receive-maximum
  backpressure.
- Persistent sessions with clean start, session expiry, subscription recovery,
  offline queues, retained messages, and durable restart recovery.
- Storage choices for different stages of a project: in-memory by default,
  SQLite for simple durable deployments, and MySQL when you want shared
  operational infrastructure.
- Shared subscriptions, subscription identifiers, retained replay behavior,
  no-local handling, message expiry, and retained-store limits.
- Prometheus metrics and structured tracing, ready for dashboards instead of
  print-debug archaeology.
- Graceful shutdown through `rs-netty` server handles: Pulse listens for Ctrl-C,
  asks the server to stop accepting work, and waits for the server task to exit.

## Quick Start

Run Pulse locally:

```sh
cargo run -- --bind 127.0.0.1:1883 --log info
```

Use the sample config:

```sh
cargo run -- --config Broker.toml
```

Enable SQLite persistence:

```toml
[storage]
sqlite = "pulse.db"
```

Expose Prometheus metrics:

```toml
[observability]
metrics_bind = "127.0.0.1:9000"
```

Then run:

```sh
cargo run -- --config Broker.toml
```

## Configuration

Pulse reads configuration from `Broker.toml` by default when the file sits next
to the executable. You can also pass an explicit path:

```sh
cargo run -- --config ./Broker.toml
```

Configuration can come from four places, applied in this order:

1. Built-in defaults.
2. `Broker.toml`.
3. Environment variables such as `MQTT_RS_BIND`, `MQTT_RS_SQLITE`,
   `MQTT_RS_LOG`, and `MQTT_RS_METRICS_BIND`.
4. CLI flags such as `--bind`, `--sqlite`, `--mysql`, `--log`, and
   `--metrics-bind`.

The `MQTT_RS_*` environment prefix is intentionally kept for compatibility
while the broker moves under the Pulse name.

## What Works Today

Pulse already covers the core broker paths:

- MQTT connection lifecycle and keep alive.
- Persistent and clean-start sessions.
- Subscription storage and wildcard matching.
- Shared subscriptions with round-robin dispatch.
- Retained messages with expiry and store limits.
- QoS 1 and QoS 2 handshakes, inflight state, and reconnect redelivery.
- Offline queueing for persistent sessions.
- SQLite and MySQL-backed state recovery.
- Prometheus metrics for connections, publishes, subscriptions, queues,
  retained messages, inflight packets, parse errors, and delivery failures.

## Project Shape

```text
src/main.rs                         server startup and graceful shutdown
src/settings.rs                     file/env/CLI configuration
src/protocol.rs                     MQTT reason codes and topic matching
src/broker/runtime/connection       MQTT packet handler and lifecycle
src/broker/runtime/delivery         publish routing, QoS, offline queues
src/broker/runtime/subscription_tree subscriptions and shared groups
src/broker/storage                  in-memory, SQLite, and MySQL state
src/observability                   tracing and Prometheus metrics
```

## Development

Run the full test suite:

```sh
cargo test
```

Run formatting checks:

```sh
cargo fmt --check
```

The test suite exercises broker behavior over real TCP connections, including
CONNECT validation, malformed packets, will delivery, keep alive, persistent
session recovery, retained replay, QoS handshakes, SQLite restart recovery, and
message expiry.

## Roadmap

Pulse is still young. The next high-value areas are:

- Pluggable authentication and publish/subscribe authorization hooks.
- Inflight retransmission timers for QoS 1 and QoS 2.
- Graceful shutdown policy for active MQTT sessions.
- Interop testing with common MQTT v5 clients.
- Clear documentation of supported and intentionally unsupported MQTT v5
  features.

## The Pitch

Pulse aims to be the broker you can read, run, and reason about. Small enough to
understand, serious enough to preserve sessions, and Rust-native all the way
down.
