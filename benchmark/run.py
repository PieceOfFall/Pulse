#!/usr/bin/env python3
"""Compare Pulse and Mosquitto on a small MQTT v5 benchmark set."""

from __future__ import annotations

import argparse
import os
import platform
import shutil
import socket
import struct
import subprocess
import tempfile
import threading
import time
from pathlib import Path
from typing import Optional


QOS0 = 0
QOS1 = 1
QOS2 = 2

PACKET_CONNECT = 1
PACKET_CONNACK = 2
PACKET_PUBLISH = 3
PACKET_PUBACK = 4
PACKET_PUBREC = 5
PACKET_PUBREL = 6
PACKET_PUBCOMP = 7
PACKET_SUBSCRIBE = 8
PACKET_SUBACK = 9
PACKET_DISCONNECT = 14

SUCCESS_LIMIT = 0x80

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PULSE_BIN = REPO_ROOT / "target" / "release" / "Pulse"
HOMEBREW_MOSQUITTO = Path("/opt/homebrew/opt/mosquitto/sbin/mosquitto")


class MqttError(RuntimeError):
    pass


class BrokerProcess:
    def __init__(self, name: str, process: subprocess.Popen[str], tempdir=None):
        self.name = name
        self.process = process
        self.tempdir = tempdir
        self.stdout = ""
        self.stderr = ""

    def stop(self) -> None:
        if self.process.poll() is None:
            self.process.terminate()
            try:
                out, err = self.process.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                out, err = self.process.communicate(timeout=5)
        else:
            out, err = self.process.communicate(timeout=1)

        self.stdout = out or ""
        self.stderr = err or ""
        if self.tempdir is not None:
            self.tempdir.cleanup()


class MqttClient:
    def __init__(self, host: str, port: int, client_id: str, timeout: float):
        self.sock = socket.create_connection((host, port), timeout=timeout)
        self.sock.settimeout(timeout)
        self.client_id = client_id
        self.connect(client_id)

    def close(self) -> None:
        try:
            self.send_packet(PACKET_DISCONNECT << 4, b"\x00")
        except OSError:
            pass
        try:
            self.sock.close()
        except OSError:
            pass

    def send_packet(self, header: int, body: bytes) -> None:
        self.sock.sendall(bytes([header]) + encode_varint(len(body)) + body)

    def read_packet(self) -> tuple[int, int, bytes]:
        header = read_exact(self.sock, 1)[0]
        remaining = read_varint_from_socket(self.sock)
        body = read_exact(self.sock, remaining)
        return header >> 4, header & 0x0F, body

    def connect(self, client_id: str) -> None:
        variable_header = (
            encode_utf8("MQTT")
            + b"\x05"
            + b"\x02"
            + struct.pack("!H", 60)
            + b"\x00"
        )
        payload = encode_utf8(client_id)
        self.send_packet(PACKET_CONNECT << 4, variable_header + payload)

        packet_type, _, body = self.read_packet()
        if packet_type != PACKET_CONNACK or len(body) < 2:
            raise MqttError(f"{client_id}: expected CONNACK, got packet {packet_type}")
        reason = body[1]
        if reason >= SUCCESS_LIMIT:
            raise MqttError(f"{client_id}: CONNACK rejected with reason 0x{reason:02x}")

    def subscribe(self, packet_id: int, topic: str, qos: int) -> None:
        body = struct.pack("!H", packet_id) + b"\x00" + encode_utf8(topic) + bytes([qos])
        self.send_packet((PACKET_SUBSCRIBE << 4) | 0x02, body)

        packet_type, _, response = self.read_packet()
        if packet_type != PACKET_SUBACK or len(response) < 4:
            raise MqttError(f"{self.client_id}: expected SUBACK, got packet {packet_type}")
        response_id = struct.unpack("!H", response[:2])[0]
        if response_id != packet_id:
            raise MqttError(f"{self.client_id}: SUBACK packet id mismatch")
        prop_len, index = decode_varint(response, 2)
        reason_index = index + prop_len
        if reason_index >= len(response):
            raise MqttError(f"{self.client_id}: SUBACK missing reason code")
        reason = response[reason_index]
        if reason >= SUCCESS_LIMIT:
            raise MqttError(f"{self.client_id}: SUBSCRIBE rejected with reason 0x{reason:02x}")

    def publish(self, topic: str, payload: bytes, qos: int, packet_id: int = 0, retain: bool = False) -> None:
        flags = (qos << 1) | (1 if retain else 0)
        body = encode_utf8(topic)
        if qos:
            body += struct.pack("!H", packet_id)
        body += b"\x00" + payload
        self.send_packet((PACKET_PUBLISH << 4) | flags, body)

        if qos == QOS1:
            self.expect_ack(PACKET_PUBACK, packet_id)
        elif qos == QOS2:
            self.expect_ack(PACKET_PUBREC, packet_id)
            self.send_packet((PACKET_PUBREL << 4) | 0x02, struct.pack("!H", packet_id))
            self.expect_ack(PACKET_PUBCOMP, packet_id)

    def receive_publish_and_ack(self) -> tuple[str, bytes, int, bool]:
        packet_type, flags, body = self.read_packet()
        if packet_type != PACKET_PUBLISH:
            raise MqttError(f"{self.client_id}: expected PUBLISH, got packet {packet_type}")
        topic, payload, packet_id, qos, retain = decode_publish(flags, body)

        if qos == QOS1:
            self.send_packet(PACKET_PUBACK << 4, struct.pack("!H", packet_id))
        elif qos == QOS2:
            self.send_packet(PACKET_PUBREC << 4, struct.pack("!H", packet_id))
            self.expect_ack(PACKET_PUBREL, packet_id)
            self.send_packet(PACKET_PUBCOMP << 4, struct.pack("!H", packet_id))

        return topic, payload, qos, retain

    def receive_publishes_and_ack(
        self, count: int, expected_topic: str, expected_payload: bytes
    ) -> int:
        received = 0
        pending_qos2_pubrel = set()

        while received < count or pending_qos2_pubrel:
            packet_type, flags, body = self.read_packet()
            if packet_type == PACKET_PUBLISH:
                topic, payload, packet_id, qos, _ = decode_publish(flags, body)
                if topic != expected_topic or payload != expected_payload:
                    raise MqttError("received unexpected PUBLISH payload")

                if qos == QOS1:
                    self.send_packet(PACKET_PUBACK << 4, struct.pack("!H", packet_id))
                elif qos == QOS2:
                    pending_qos2_pubrel.add(packet_id)
                    self.send_packet(PACKET_PUBREC << 4, struct.pack("!H", packet_id))

                received += 1
            elif packet_type == PACKET_PUBREL:
                packet_id = ack_packet_id("PUBREL", body)
                if packet_id not in pending_qos2_pubrel:
                    raise MqttError(f"{self.client_id}: unexpected PUBREL packet id {packet_id}")
                pending_qos2_pubrel.remove(packet_id)
                self.send_packet(PACKET_PUBCOMP << 4, struct.pack("!H", packet_id))
            else:
                raise MqttError(
                    f"{self.client_id}: expected PUBLISH or PUBREL, got packet {packet_type}"
                )

        return received

    def expect_ack(self, expected_type: int, packet_id: int) -> None:
        packet_type, _, body = self.read_packet()
        if packet_type != expected_type or len(body) < 2:
            raise MqttError(
                f"{self.client_id}: expected packet {expected_type}, got packet {packet_type}"
            )
        response_id = ack_packet_id("ack", body)
        if response_id != packet_id:
            raise MqttError(f"{self.client_id}: ack packet id mismatch")
        if len(body) >= 3 and body[2] >= SUCCESS_LIMIT:
            raise MqttError(
                f"{self.client_id}: ack rejected with reason 0x{body[2]:02x}"
            )


def encode_utf8(value: str) -> bytes:
    encoded = value.encode("utf-8")
    if len(encoded) > 0xFFFF:
        raise ValueError("MQTT UTF-8 string is too long")
    return struct.pack("!H", len(encoded)) + encoded


def encode_varint(value: int) -> bytes:
    if value < 0 or value > 268_435_455:
        raise ValueError("MQTT variable byte integer out of range")
    encoded = bytearray()
    while True:
        byte = value % 128
        value //= 128
        if value:
            byte |= 0x80
        encoded.append(byte)
        if not value:
            return bytes(encoded)


def decode_varint(data: bytes, index: int = 0) -> tuple[int, int]:
    multiplier = 1
    value = 0
    while True:
        if index >= len(data):
            raise MqttError("truncated MQTT variable byte integer")
        byte = data[index]
        index += 1
        value += (byte & 0x7F) * multiplier
        if byte & 0x80 == 0:
            return value, index
        multiplier *= 128
        if multiplier > 128 * 128 * 128:
            raise MqttError("malformed MQTT variable byte integer")


def read_varint_from_socket(sock: socket.socket) -> int:
    multiplier = 1
    value = 0
    while True:
        byte = read_exact(sock, 1)[0]
        value += (byte & 0x7F) * multiplier
        if byte & 0x80 == 0:
            return value
        multiplier *= 128
        if multiplier > 128 * 128 * 128:
            raise MqttError("malformed MQTT remaining length")


def read_exact(sock: socket.socket, size: int) -> bytes:
    chunks = bytearray()
    while len(chunks) < size:
        chunk = sock.recv(size - len(chunks))
        if not chunk:
            raise MqttError("socket closed while reading MQTT packet")
        chunks.extend(chunk)
    return bytes(chunks)


def decode_publish(flags: int, body: bytes) -> tuple[str, bytes, int, int, bool]:
    qos = (flags >> 1) & 0x03
    retain = bool(flags & 0x01)
    if qos == 0x03:
        raise MqttError("invalid PUBLISH QoS flags")

    if len(body) < 3:
        raise MqttError("truncated PUBLISH")
    topic_len = struct.unpack("!H", body[:2])[0]
    index = 2
    topic = body[index : index + topic_len].decode("utf-8")
    index += topic_len

    packet_id = 0
    if qos:
        packet_id = struct.unpack("!H", body[index : index + 2])[0]
        index += 2

    prop_len, index = decode_varint(body, index)
    index += prop_len
    if index > len(body):
        raise MqttError("truncated PUBLISH properties")
    return topic, body[index:], packet_id, qos, retain


def ack_packet_id(name: str, body: bytes) -> int:
    if len(body) < 2:
        raise MqttError(f"truncated {name} packet")
    return struct.unpack("!H", body[:2])[0]


def next_packet_id(value: int) -> int:
    return (value % 65535) + 1


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def wait_for_port(host: str, port: int, process: subprocess.Popen[str], timeout: float) -> None:
    deadline = time.time() + timeout
    last_error = None
    while time.time() < deadline:
        if process.poll() is not None:
            out, err = process.communicate(timeout=1)
            raise RuntimeError(
                f"broker exited before listening on {host}:{port}\nstdout:\n{out}\nstderr:\n{err}"
            )
        try:
            with socket.create_connection((host, port), timeout=0.2):
                return
        except OSError as error:
            last_error = error
            time.sleep(0.05)
    raise RuntimeError(f"timed out waiting for {host}:{port}: {last_error}")


def start_pulse(pulse_bin: Path, port: int) -> BrokerProcess:
    if not pulse_bin.exists():
        raise FileNotFoundError(
            f"Pulse binary not found at {pulse_bin}. Run `cargo build --release` or pass --pulse-bin."
        )
    process = subprocess.Popen(
        [
            str(pulse_bin),
            "--bind",
            f"127.0.0.1:{port}",
            "--log",
            "error",
            "--inflight-retransmit-interval-ms",
            "0",
        ],
        cwd=REPO_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    wait_for_port("127.0.0.1", port, process, timeout=10)
    return BrokerProcess("Pulse", process)


def start_mosquitto(mosquitto_bin: Path, port: int) -> BrokerProcess:
    if not mosquitto_bin.exists():
        raise FileNotFoundError(f"mosquitto binary not found at {mosquitto_bin}")

    tempdir = tempfile.TemporaryDirectory(prefix="pulse-mosquitto-")
    config_path = Path(tempdir.name) / "mosquitto.conf"
    config_path.write_text(
        "\n".join(
            [
                f"listener {port} 127.0.0.1",
                "allow_anonymous true",
                "persistence false",
                "max_inflight_bytes 0",
                "max_inflight_messages 0",
                "max_queued_bytes 0",
                "max_queued_messages 0",
                "message_size_limit 0",
                "log_type error",
                "connection_messages false",
                "",
            ]
        ),
        encoding="utf-8",
    )

    process = subprocess.Popen(
        [str(mosquitto_bin), "-c", str(config_path), "-q"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    wait_for_port("127.0.0.1", port, process, timeout=10)
    return BrokerProcess("Mosquitto", process, tempdir=tempdir)


def run_throughput(
    broker_name: str,
    port: int,
    qos: int,
    messages: int,
    payload_bytes: int,
    timeout: float,
) -> dict[str, object]:
    topic = f"pulse/benchmark/{broker_name.lower()}/qos{qos}/{os.getpid()}"
    payload = b"x" * payload_bytes
    subscriber = MqttClient("127.0.0.1", port, f"{broker_name}-sub-qos{qos}", timeout)
    publisher = MqttClient("127.0.0.1", port, f"{broker_name}-pub-qos{qos}", timeout)
    subscriber.subscribe(1, topic, qos)

    errors = []
    received = 0

    def receive_loop() -> None:
        nonlocal received
        try:
            received = subscriber.receive_publishes_and_ack(messages, topic, payload)
        except BaseException as error:
            errors.append(error)

    thread = threading.Thread(target=receive_loop, name=f"{broker_name}-qos{qos}-subscriber")
    thread.start()
    start = time.perf_counter()
    try:
        packet_id = 1
        for _ in range(messages):
            publisher.publish(topic, payload, qos, packet_id if qos else 0)
            packet_id = next_packet_id(packet_id)
        thread.join(timeout=max(timeout, 30))
        elapsed = time.perf_counter() - start
        if thread.is_alive():
            raise TimeoutError("subscriber did not receive all messages before timeout")
        if errors:
            raise errors[0]
        if received != messages:
            raise MqttError(f"received {received} messages, expected {messages}")
        return {
            "broker": broker_name,
            "scenario": f"qos{qos}-throughput",
            "count": messages,
            "seconds": elapsed,
            "rate": messages / elapsed if elapsed else float("inf"),
        }
    finally:
        publisher.close()
        subscriber.close()


def run_retained_fanout(
    broker_name: str,
    port: int,
    subscribers: int,
    payload_bytes: int,
    timeout: float,
) -> dict[str, object]:
    topic = f"pulse/benchmark/{broker_name.lower()}/retained/{os.getpid()}"
    payload = b"r" * payload_bytes
    publisher = MqttClient("127.0.0.1", port, f"{broker_name}-retained-pub", timeout)
    publisher.publish(topic, payload, QOS0, retain=True)
    publisher.close()

    clients = [
        MqttClient("127.0.0.1", port, f"{broker_name}-retained-sub-{index}", timeout)
        for index in range(subscribers)
    ]
    errors = []
    received = 0
    received_lock = threading.Lock()

    def subscribe_and_receive(index: int, client: MqttClient) -> None:
        nonlocal received
        try:
            client.subscribe(index + 1, topic, QOS0)
            received_topic, received_payload, _, retain = client.receive_publish_and_ack()
            if received_topic != topic or received_payload != payload or not retain:
                raise MqttError("received unexpected retained PUBLISH")
            with received_lock:
                received += 1
        except BaseException as error:
            errors.append(error)

    threads = [
        threading.Thread(
            target=subscribe_and_receive,
            args=(index, client),
            name=f"{broker_name}-retained-{index}",
        )
        for index, client in enumerate(clients)
    ]

    start = time.perf_counter()
    try:
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=max(timeout, 30))
        elapsed = time.perf_counter() - start
        alive = [thread.name for thread in threads if thread.is_alive()]
        if alive:
            raise TimeoutError(f"retained fanout subscribers timed out: {', '.join(alive[:3])}")
        if errors:
            raise errors[0]
        if received != subscribers:
            raise MqttError(f"received {received} retained messages, expected {subscribers}")
        return {
            "broker": broker_name,
            "scenario": "retained-fanout",
            "count": subscribers,
            "seconds": elapsed,
            "rate": subscribers / elapsed if elapsed else float("inf"),
        }
    finally:
        for client in clients:
            client.close()


def run_benchmark_for_broker(name: str, start_fn, args) -> tuple[list[dict[str, object]], str]:
    port = find_free_port()
    broker = start_fn(port)
    try:
        results = [
            run_throughput(name, port, QOS0, args.messages, args.payload_bytes, args.timeout),
            run_throughput(name, port, QOS1, args.messages, args.payload_bytes, args.timeout),
            run_throughput(name, port, QOS2, args.messages, args.payload_bytes, args.timeout),
            run_retained_fanout(
                name,
                port,
                args.fanout_subscribers,
                args.payload_bytes,
                args.timeout,
            ),
        ]
        return results, ""
    finally:
        broker.stop()


def resolve_mosquitto_bin(value: Optional[str]) -> Path:
    if value:
        return Path(value)
    found = shutil.which("mosquitto")
    if found:
        return Path(found)
    return HOMEBREW_MOSQUITTO


def version_output(command: list[str]) -> str:
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=5, check=False)
    except OSError as error:
        return f"unavailable: {error}"
    output = "\n".join(part for part in [result.stdout.strip(), result.stderr.strip()] if part)
    for line in output.splitlines():
        if line.startswith("mosquitto version"):
            return line.strip()
    for line in output.splitlines():
        if "version" in line.lower():
            return line.strip()
    return output.splitlines()[0].strip() if output else "unknown"


def pulse_version() -> str:
    cargo_toml = REPO_ROOT / "Cargo.toml"
    for line in cargo_toml.read_text(encoding="utf-8").splitlines():
        if line.startswith("version = "):
            return f"Pulse {line.split('=', 1)[1].strip().strip(chr(34))}"
    return "Pulse unknown"


def print_results(results: list[dict[str, object]]) -> None:
    print()
    print(f"{'Broker':<11} {'Scenario':<18} {'Count':>10} {'Seconds':>10} {'Rate/sec':>12}")
    print("-" * 68)
    for result in results:
        print(
            f"{result['broker']:<11} "
            f"{result['scenario']:<18} "
            f"{result['count']:>10} "
            f"{result['seconds']:>10.4f} "
            f"{result['rate']:>12.2f}"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--messages", type=int, default=10_000)
    parser.add_argument("--payload-bytes", type=int, default=128)
    parser.add_argument("--fanout-subscribers", type=int, default=100)
    parser.add_argument("--pulse-bin", type=Path, default=DEFAULT_PULSE_BIN)
    parser.add_argument("--mosquitto-bin")
    parser.add_argument("--timeout", type=float, default=10.0)
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    if args.messages <= 0:
        raise ValueError("--messages must be greater than 0")
    if args.payload_bytes < 0:
        raise ValueError("--payload-bytes must not be negative")
    if args.fanout_subscribers <= 0:
        raise ValueError("--fanout-subscribers must be greater than 0")
    if args.timeout <= 0:
        raise ValueError("--timeout must be greater than 0")


def main() -> int:
    args = parse_args()
    validate_args(args)
    mosquitto_bin = resolve_mosquitto_bin(args.mosquitto_bin)

    print("Environment")
    print(f"  platform: {platform.platform()}")
    print(f"  python: {platform.python_version()}")
    print(f"  pulse: {pulse_version()} ({args.pulse_bin})")
    print(f"  mosquitto: {version_output([str(mosquitto_bin), '-h'])} ({mosquitto_bin})")
    print(f"  messages: {args.messages}")
    print(f"  payload_bytes: {args.payload_bytes}")
    print(f"  fanout_subscribers: {args.fanout_subscribers}")

    all_results: list[dict[str, object]] = []
    pulse_results, _ = run_benchmark_for_broker(
        "Pulse", lambda port: start_pulse(args.pulse_bin, port), args
    )
    all_results.extend(pulse_results)
    mosquitto_results, _ = run_benchmark_for_broker(
        "Mosquitto", lambda port: start_mosquitto(mosquitto_bin, port), args
    )
    all_results.extend(mosquitto_results)
    print_results(all_results)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(130)
