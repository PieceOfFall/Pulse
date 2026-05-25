# mqtt-rs Feature TODO

This list orders the remaining MQTT v5 work by dependency and risk. Completed
items are marked with `[x]`.

## 0. Current Baseline

- [x] TCP broker built on `rs-netty::TcpServer` and `MqttCodec`
- [x] CONNECT / CONNACK
- [x] PINGREQ / PINGRESP
- [x] DISCONNECT handling
- [x] SUBSCRIBE / SUBACK
- [x] UNSUBSCRIBE / UNSUBACK
- [x] Basic wildcard topic matching
- [x] MQTT system topic matching rules for `$...`
- [x] Duplicate client id replaces and closes the previous connection
- [x] Retained messages
- [x] Will message publication on abnormal close
- [x] QoS 0 publish delivery
- [x] QoS 1 publish and delivery handshakes
- [x] QoS 2 publish and delivery handshakes
- [x] Retained QoS replay at subscriber maximum QoS

## 1. Protocol Correctness Foundations

- [ ] Enforce CONNECT validation rules
  - [x] Reject invalid protocol name/version at the codec layer.
  - [x] Reject invalid client id rules with precise MQTT v5 reason codes.
  - [x] Reject invalid will fields with precise MQTT v5 reason codes.
  - [x] Reject malformed or unsupported auth fields with precise MQTT v5 reason codes.
- [ ] Implement MQTT keep alive semantics
  - Track per-client keep alive from CONNECT and close idle MQTT sessions according to the MQTT 5 timeout rule.
- [ ] Normalize DISCONNECT reason handling
  - Distinguish normal disconnect, protocol errors, admin/server close, and will-triggering closes.
- [ ] Add packet identifier validation
  - [x] Reject missing packet ids where required at the codec layer.
  - [x] Reject zero packet ids at the codec layer.
  - [x] Reject QoS 0 PUBLISH packet ids at the codec layer.
  - [ ] Detect packet id reuse in broker state.
  - [ ] Detect unexpected ACK packets with correct reason codes.
- [x] Add focused integration tests for CONNECT validation error paths.
- [ ] Add focused integration tests for malformed packet and protocol error paths.

## 2. Session Model

- [ ] Model client sessions separately from live TCP connections
  - Split connection state from session state so reconnects can resume subscriptions and inflight messages.
- [ ] Implement Clean Start and Session Expiry Interval
  - Set `session_present` correctly and expire sessions according to MQTT v5 rules.
- [ ] Preserve subscriptions across reconnects when the session is persistent.
- [ ] Preserve QoS 1/2 outbound inflight state across reconnects.
- [ ] Redeliver pending QoS 1/2 messages with `dup = true` after reconnect.
- [ ] Add tests for clean start, persistent session resume, and session expiry.

## 3. MQTT v5 Properties

- [ ] Enforce Maximum Packet Size
  - Respect client/server limits and return Packet Too Large where appropriate.
- [ ] Enforce Receive Maximum
  - Limit concurrent QoS 1/2 inflight messages per client.
- [ ] Implement Message Expiry Interval
  - Expire queued, retained, and offline messages as required.
- [ ] Implement Topic Alias and Topic Alias Maximum.
- [ ] Preserve and forward User Property where MQTT v5 allows it.
- [ ] Support Response Topic and Correlation Data forwarding.
- [ ] Support Reason String and richer reason properties on ACK/DISCONNECT packets.

## 4. Subscription Features

- [ ] Implement retain handling mode `1` precisely
  - Send retained messages only when a subscription is newly created, not when an existing subscription is updated.
- [ ] Implement Subscription Identifier
  - Store subscription identifiers and attach them to matching PUBLISH packets.
- [ ] Implement shared subscriptions: `$share/{group}/{filter}`.
- [ ] Add stricter topic filter validation tests, including edge cases around `$`, empty levels, and shared subscription syntax.
- [ ] Add subscription quotas and clear error paths for quota exceeded.

## 5. Authentication And Authorization

- [ ] Add a pluggable authenticator trait.
- [ ] Support username/password authentication.
- [ ] Add ACL hooks for publish and subscribe authorization.
- [ ] Define behavior for unsupported enhanced AUTH.
- [ ] Add tests for rejected CONNECT, rejected SUBSCRIBE, and rejected PUBLISH.

## 6. Reliability And Backpressure

- [ ] Add per-client offline queues for persistent sessions.
- [ ] Add queue limits and slow-consumer policy.
- [ ] Add retained message limits.
- [ ] Add inflight retransmission timers for QoS 1/2.
- [ ] Add duplicate inbound QoS 2 handling that avoids double delivery.
- [ ] Decide and document ordering guarantees per client and per topic.

## 7. Persistence

- [ ] Define storage traits for sessions, subscriptions, retained messages, and inflight messages.
- [ ] Implement an in-memory storage backend as the default.
- [ ] Add an optional durable backend.
- [ ] Add crash/restart recovery tests for retained messages and persistent sessions.

## 8. Operations

- [ ] Replace `println!` startup output with structured tracing.
- [ ] Add metrics for connections, sessions, subscriptions, retained messages, inflight messages, and publish rates.
- [ ] Add configuration file support.
- [ ] Add graceful shutdown behavior for active sessions.
- [ ] Add benchmark scenarios for QoS 0/1/2 and retained fanout.

## 9. Compliance

- [ ] Build a protocol compatibility test matrix against MQTT 5 clients.
- [ ] Add interop tests with common clients such as `mosquitto_pub/sub` and `mqttx`.
- [ ] Add property-level conformance tests.
- [ ] Document supported and intentionally unsupported MQTT v5 features.
