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

- [x] Enforce CONNECT validation rules
  - [x] Reject invalid protocol name/version at the codec layer.
  - [x] Reject invalid client id rules with precise MQTT v5 reason codes.
  - [x] Reject invalid will fields with precise MQTT v5 reason codes.
  - [x] Reject malformed or unsupported auth fields with precise MQTT v5 reason codes.
- [x] Implement MQTT keep alive semantics
  - [x] Track per-client keep alive from CONNECT and close idle MQTT sessions according to the MQTT 5 timeout rule.
- [ ] Normalize DISCONNECT reason handling
  - [x] Distinguish normal client disconnects from will-triggering protocol errors.
  - [x] Publish will messages for protocol-error handler closes.
  - [x] Avoid will publication for normal client DISCONNECT packets.
  - [ ] Distinguish admin/server close reason codes.
- [x] Add packet identifier validation
  - [x] Reject missing packet ids where required at the codec layer.
  - [x] Reject zero packet ids at the codec layer.
  - [x] Reject QoS 0 PUBLISH packet ids at the codec layer.
  - [x] Detect packet id reuse in broker state.
  - [x] Detect unexpected ACK packets with correct reason codes.
- [x] Add focused integration tests for CONNECT validation error paths.
- [x] Add focused integration tests for protocol error will-handling paths.
- [x] Add focused integration tests for malformed packet paths.

## 2. Session Model

- [x] Model client sessions separately from live TCP connections
  - Split connection state from session state so reconnects can resume subscriptions and inflight messages.
- [x] Implement Clean Start and `session_present`.
- [x] Expire sessions according to MQTT v5 Session Expiry Interval rules.
- [x] Preserve subscriptions across reconnects when the session is persistent.
- [x] Preserve QoS 1/2 outbound inflight state across reconnects.
- [x] Redeliver pending QoS 1/2 messages with `dup = true` after reconnect.
- [x] Add tests for clean start and persistent session resume.
- [x] Add tests for session expiry.

## 3. MQTT v5 Properties

- [x] Enforce Maximum Packet Size
  - [x] Respect client/server limits and return Packet Too Large where appropriate.
- [x] Enforce Receive Maximum
  - [x] Limit concurrent QoS 1/2 inflight messages per client.
- [x] Implement Message Expiry Interval
  - [x] Expire queued, retained, and offline messages as required.
- [x] Implement Topic Alias and Topic Alias Maximum.
- [x] Preserve and forward User Property where MQTT v5 allows it.
- [x] Support Response Topic and Correlation Data forwarding.
- [x] Support Reason String and richer reason properties on ACK/DISCONNECT packets.

## 4. Subscription Features

- [x] Implement retain handling mode `1` precisely
  - Send retained messages only when a subscription is newly created, not when an existing subscription is updated.
- [x] Implement Subscription Identifier
  - [x] Store subscription identifiers and attach them to matching PUBLISH packets.
- [x] Implement shared subscriptions: `$share/{group}/{filter}`.
- [ ] Add stricter topic filter validation tests, including edge cases around `$`, empty levels, and shared subscription syntax.
- [x] Add subscription quotas and clear error paths for quota exceeded.

## 5. Authentication And Authorization

- [ ] Add a pluggable authenticator trait.
- [ ] Support username/password authentication.
- [ ] Add ACL hooks for publish and subscribe authorization.
- [ ] Define behavior for unsupported enhanced AUTH.
- [ ] Add tests for rejected CONNECT, rejected SUBSCRIBE, and rejected PUBLISH.

## 6. Reliability And Backpressure

- [x] Add per-client offline queues for persistent sessions.
- [x] Add queue limits and slow-consumer policy.
- [x] Add retained message limits.
- [ ] Add inflight retransmission timers for QoS 1/2.
- [x] Add duplicate inbound QoS 2 handling that avoids double delivery.
- [ ] Decide and document ordering guarantees per client and per topic.

## 7. Persistence

- [x] Define storage traits for sessions, subscriptions, retained messages, and inflight messages.
- [x] Implement an in-memory storage backend as the default.
- [x] Add an optional durable backend for sessions, subscriptions, and retained messages.
- [x] Persist QoS inflight and offline queue state in the durable backend.
- [x] Add restart recovery tests for retained messages.
- [x] Add restart recovery tests for persistent sessions after durable offline queues are implemented.

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
