#!/usr/bin/env python3
"""Measure PulseQ publish throughput and latency end to end over HTTP + WebSocket.

Only the standard library is used so the benchmark runs anywhere. It reports three
scenarios against a running broker:

  sequential   one publisher thread, no consumer (shows raw HTTP request cost)
  drained      many publisher threads with a WebSocket consumer attached, so the
               queue is continuously emptied (shows sustainable publish rate)
  saturated    many publisher threads with no consumer, which fills the queue and
               triggers the bounded backpressure window and HTTP 429 responses

Usage:

    java -Xmx64m -jar pulseq-server/target/pulseq-server-0.1.0.jar
    python3 tools/bench.py --threads 16 --per-thread 200
"""

import argparse
import base64
import json
import os
import socket
import statistics
import struct
import threading
import time
import urllib.error
import urllib.request

BASE = "http://localhost:8080"


def publish(topic, index):
    body = json.dumps({"payload": "bench-%d" % index}).encode()
    request = urllib.request.Request(
        "%s/publish/%s" % (BASE, topic), data=body,
        headers={"Content-Type": "application/json"})
    started = time.perf_counter()
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            response.read()
        status = 200
    except urllib.error.HTTPError as e:
        e.read()
        status = e.code
    return status, (time.perf_counter() - started) * 1000.0


def run(topic, threads, per_thread):
    latencies = []
    statuses = {}
    lock = threading.Lock()

    def worker(index):
        local = []
        local_statuses = {}
        for i in range(per_thread):
            status, elapsed = publish(topic, index * per_thread + i)
            local.append(elapsed)
            local_statuses[status] = local_statuses.get(status, 0) + 1
        with lock:
            latencies.extend(local)
            for code, count in local_statuses.items():
                statuses[code] = statuses.get(code, 0) + count

    started = time.perf_counter()
    pool = [threading.Thread(target=worker, args=(t,)) for t in range(threads)]
    for thread in pool:
        thread.start()
    for thread in pool:
        thread.join()
    elapsed = time.perf_counter() - started

    total = threads * per_thread
    latencies.sort()
    print("topic=%s threads=%d messages=%d wall=%.2fs" % (topic, threads, total, elapsed))
    print("  aggregate throughput = %.0f msg/s" % (total / elapsed))
    print("  latency p50=%.2f ms  p95=%.2f ms  p99=%.2f ms" % (
        latencies[int(total * 0.50)],
        latencies[int(total * 0.95)],
        latencies[min(len(latencies) - 1, int(total * 0.99))]))
    print("  status codes = %s" % statuses)
    return total / elapsed


def ws_connect(path):
    """Open a minimal WebSocket connection and return (socket, file object)."""
    host, port = "localhost", 8080
    key = base64.b64encode(os.urandom(16)).decode()
    sock = socket.create_connection((host, port), timeout=20)
    handshake = (
        "GET %s HTTP/1.1\r\nHost: %s:%d\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
        "Sec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n\r\n" % (path, host, port, key)
    )
    sock.sendall(handshake.encode())
    stream = sock.makefile("rb")
    while True:
        line = stream.readline()
        if line in (b"\r\n", b"\n", b""):
            break
    return sock, stream


def ws_send(sock, text):
    payload = text.encode()
    mask = os.urandom(4)
    masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
    header = bytearray([0x81])
    length = len(payload)
    if length < 126:
        header.append(0x80 | length)
    else:
        header.append(0x80 | 126)
        header += struct.pack(">H", length)
    sock.sendall(bytes(header) + mask + masked)


def ws_read_frame(stream):
    first = stream.read(2)
    if len(first) < 2:
        return None
    length = first[1] & 0x7F
    if length == 126:
        length = struct.unpack(">H", stream.read(2))[0]
    elif length == 127:
        length = struct.unpack(">Q", stream.read(8))[0]
    payload = stream.read(length) if length else b""
    return first[0] & 0x0F, payload


def consumer(topic, stop, counter):
    """Attach a WebSocket subscriber that acknowledges everything it receives."""
    sock, stream = ws_connect("/subscribe/%s" % topic)
    while not stop.is_set():
        sock.settimeout(1.0)
        try:
            frame = ws_read_frame(stream)
        except socket.timeout:
            continue
        except OSError:
            break
        if frame is None:
            break
        opcode, payload = frame
        if opcode == 0x8:
            break
        if opcode != 0x1:
            continue
        try:
            message_id = json.loads(payload.decode())["id"]
        except (ValueError, KeyError):
            continue
        try:
            ws_send(sock, "ACK %s" % message_id)
        except OSError:
            break
        counter["acked"] += 1
    try:
        sock.close()
    except OSError:
        pass


def run_drained(topic, threads, per_thread):
    stop = threading.Event()
    counter = {"acked": 0}
    thread = threading.Thread(target=consumer, args=(topic, stop, counter))
    thread.start()
    try:
        run(topic, threads, per_thread)
    finally:
        stop.set()
        thread.join(timeout=5)
    print("  acknowledged by consumer = %d" % counter["acked"])


def main():
    global BASE
    parser = argparse.ArgumentParser()
    parser.add_argument("--threads", type=int, default=16)
    parser.add_argument("--per-thread", type=int, default=200)
    parser.add_argument("--base", default=BASE)
    args = parser.parse_args()

    BASE = args.base

    print("=== sequential (1 publisher thread, no consumer) ===")
    run("bench-sequential", 1, args.per_thread)

    print()
    print("=== drained (publishers + WebSocket consumer) ===")
    run_drained("bench-drained", args.threads, args.per_thread)

    print()
    print("=== saturated (no consumer: exercises backpressure and HTTP 429) ===")
    run("bench-saturated", args.threads, args.per_thread)


if __name__ == "__main__":
    main()