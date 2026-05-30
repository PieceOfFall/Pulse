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
- Storage choices for different stages of a project: in-memory by default on
  Unix-like builds, SQLite by default for Windows MSI installs, and MySQL when
  you want shared operational infrastructure.
- Shared subscriptions, subscription identifiers, retained replay behavior,
  no-local handling, message expiry, and retained-store limits.
- Prometheus metrics and structured tracing, ready for dashboards instead of
  print-debug archaeology.
- Graceful shutdown through `rs-netty` server handles: Pulse listens for Ctrl-C,
  asks the server to stop accepting work, and waits for the server task to exit.
- vNext groundwork for a higher-throughput broker core: sharded runtimes, a
  trie-based router index, and append-only durable log events are now present
  behind the current stable broker runtime.

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

Enable MQTT over TLS:

```toml
[server]
bind = "0.0.0.0:8883"

[server.tls]
enabled = true
certificate_chain = "/etc/pulse/server-chain.pem"
private_key = "/etc/pulse/server-key.pem"
```

Require client certificates for mTLS:

```toml
[server.tls]
enabled = true
certificate_chain = "/etc/pulse/server-chain.pem"
private_key = "/etc/pulse/server-key.pem"
client_auth = "required"
client_ca = "/etc/pulse/client-ca.pem"
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
   `MQTT_RS_LOG`, `MQTT_RS_METRICS_BIND`, and
   `MQTT_RS_TLS_CERTIFICATE_CHAIN`.
4. CLI flags such as `--bind`, `--sqlite`, `--mysql`, `--log`, and
   `--metrics-bind`.

The `MQTT_RS_*` environment prefix is intentionally kept for compatibility
while the broker moves under the Pulse name.

Operational knobs include `MQTT_RS_SHUTDOWN_DRAIN_TIMEOUT_MS` /
`--shutdown-drain-timeout-ms` and `MQTT_RS_INFLIGHT_RETRANSMIT_INTERVAL_MS` /
`--inflight-retransmit-interval-ms`.

vNext performance knobs are accepted by the configuration layer so deployments
can start standardizing on the next storage/routing model while the stable
runtime remains available:

```toml
[server]
worker_threads = 8

[storage]
engine = "wal"
wal_dir = "data/wal"
commit_policy = "balanced" # strict, balanced, or fast

[limits]
slow_consumer_policy = "throttle" # throttle, disconnect, or queue-offline
```

The matching CLI flags are `--worker-threads`, `--storage-engine`, `--wal-dir`,
`--storage-commit-policy`, and `--slow-consumer-policy`.

TLS can also be enabled without editing the TOML file:

```sh
cargo run -- \
  --bind 0.0.0.0:8883 \
  --tls \
  --tls-certificate-chain /etc/pulse/server-chain.pem \
  --tls-private-key /etc/pulse/server-key.pem
```

For mTLS, add `--tls-client-auth optional` or
`--tls-client-auth required` plus `--tls-client-ca /etc/pulse/client-ca.pem`.
The matching environment variables are `MQTT_RS_TLS_ENABLED`,
`MQTT_RS_TLS_CERTIFICATE_CHAIN`, `MQTT_RS_TLS_PRIVATE_KEY`,
`MQTT_RS_TLS_CLIENT_AUTH`, and `MQTT_RS_TLS_CLIENT_CA`.

Enable MQTT over WebSocket on a separate listener:

```toml
[websocket]
enabled = true
bind = "0.0.0.0:8083"
path = "/mqtt"
```

WebSocket clients must connect with `Sec-WebSocket-Protocol: mqtt` and send MQTT
control packets in binary frames. The legacy `mqttv3.1` subprotocol is also
accepted. To serve `wss://`, set `tls = true` under `[websocket]`; Pulse reuses
the certificate settings from `[server.tls]`.

Enable the static username/password and ACL backend:

```toml
[auth]
enabled = true

[[auth.users]]
username = "alice"
password = "secret"

[[auth.acl]]
username = "alice"
action = "publish"
topic_filter = "devices/alice/#"
```

When `auth.enabled = true`, ACLs are default-deny and passwords are stored as
plain text in this v1 static backend. Keep using TLS or mTLS for transport
security and prefer controlled deployments until a hashed or external
authenticator is configured.

Windows MSI installs do not install a default `Broker.toml`. They use SQLite at
`C:\ProgramData\Pulse\broker.db` by default and create that directory on first
startup. To customize settings on Windows, place a `Broker.toml` next to
`Pulse.exe`, set `MQTT_RS_*` environment variables, or pass `--config`.

## What Works Today

Pulse already covers the core broker paths:

- MQTT connection lifecycle and keep alive.
- Persistent and clean-start sessions.
- Subscription storage and wildcard matching.
- Shared subscriptions with round-robin dispatch.
- Retained messages with expiry and store limits.
- QoS 1 and QoS 2 handshakes, inflight state, and reconnect redelivery.
- Online QoS 1 and QoS 2 inflight retransmission timers.
- Offline queueing for persistent sessions.
- SQLite and MySQL-backed state recovery.
- Optional static username/password authentication and publish/subscribe ACLs.
- Prometheus metrics for connections, publishes, subscriptions, queues,
  retained messages, inflight packets, parse errors, and delivery failures.

## Project Shape

```text
src/main.rs                         server startup and graceful shutdown
src/settings.rs                     file/env/CLI configuration
src/protocol.rs                     MQTT reason codes and topic matching
src/broker/runtime/connection       TCP and WebSocket MQTT handlers
src/broker/runtime/delivery         publish routing, QoS, offline queues
src/broker/runtime/subscription_tree subscriptions and shared groups
src/broker/storage                  in-memory, SQLite, and MySQL state
src/broker/vnext                    sharded core, trie router, WAL event log
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

## Benchmarks

Pulse includes a local benchmark harness in `benchmark/`. The benchmark tooling
is excluded from Cargo packages and starts both brokers on localhost with
temporary persistent state. Pulse is run against binary WAL storage in `fast`
commit mode, while Mosquitto is run with built-in persistence enabled,
autosave-on-change enabled, and relaxed queue/inflight limits.

This single local run used:

```sh
python3 benchmark/run.py --messages 10000 --timeout 60
```

Environment: macOS 26.5 arm64, Python 3.9.6, Pulse 1.2.0 release build,
Pulse binary WAL temporary storage in `fast` commit mode, Mosquitto 2.1.2
temporary persistence, 128-byte payloads, 100 retained-fanout subscribers, and
10 ms RSS sampling.
RSS values are MiB for the broker process.

| Broker | Scenario | Count | Seconds | Rate/sec | Base RSS | Peak RSS | End RSS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Pulse-wal | qos0-throughput | 10000 | 0.1067 | 93731.44 | 3.30 | 3.75 | 3.75 |
| Pulse-wal | qos1-throughput | 10000 | 0.4401 | 22720.43 | 3.30 | 3.95 | 3.95 |
| Pulse-wal | qos2-throughput | 10000 | 0.8511 | 11749.59 | 3.30 | 3.95 | 3.95 |
| Pulse-wal | retained-fanout | 100 | 0.0115 | 8709.95 | 3.30 | 5.86 | 5.86 |
| Mosquitto-persist | qos0-throughput | 10000 | 0.1177 | 84970.82 | 4.44 | 5.44 | 5.44 |
| Mosquitto-persist | qos1-throughput | 10000 | 1.0956 | 9127.19 | 4.44 | 5.45 | 5.45 |
| Mosquitto-persist | qos2-throughput | 10000 | 1.5980 | 6257.68 | 4.44 | 5.48 | 5.48 |
| Mosquitto-persist | retained-fanout | 100 | 0.0101 | 9930.77 | 4.44 | 6.00 | 6.00 |

## Roadmap

Pulse is still young. The next high-value areas are:

- Hashed password storage and external authentication providers.
- Interop testing with common MQTT v5 clients.
- Wire `broker::vnext` into the live MQTT handler once the WAL recovery and
  sharded session model have completed crash/restart coverage.
- Clear documentation of supported and intentionally unsupported MQTT v5
  features.

## The Pitch

Pulse aims to be the broker you can read, run, and reason about. Small enough to
understand, serious enough to preserve sessions, and Rust-native all the way
down.
