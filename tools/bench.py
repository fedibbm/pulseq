#!/usr/bin/env python3
"""Measure PulseQ publish throughput and latency end to end over HTTP + WebSocket.

Only the standard library is used so the benchmark runs anywhere. It reports three
scenarios against a running broker:

  sequential   one publisher thread, no consumer (shows raw HTTP request cost)
  drained      many publisher threads with a WebSocket consumer attached, so the
               queue is continuously emptied (shows sustainable publish rate). Each
               run uses its own topic and waits for every message to be acknowledged
               before stopping, so a run that cannot drain is reported as such
               instead of being counted as throughput
  saturated    many publisher threads with no consumer, which fills the queue and
               triggers the bounded backpressure window and HTTP 429 responses.
               This measures the publish timeout, NOT broker throughput

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
            try:
                status, elapsed = publish(topic, index * per_thread + i)
            except (urllib.error.URLError, OSError) as e:
                # A refused/reset connection is a result to report, not a crash: the whole
                # point of the drained scenario is to notice when the broker gives up.
                status = "error:%s" % type(e).__name__
                elapsed = float("nan")
            if status != 200 or elapsed != elapsed:
                local_statuses[status] = local_statuses.get(status, 0) + 1
            else:
                local.append(elapsed)
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
    accepted = len(latencies)
    print("topic=%s threads=%d messages=%d wall=%.2fs" % (topic, threads, total, elapsed))
    if accepted == 0:
        print("  no request succeeded; broker was unreachable or refused every publish")
        print("  status codes = %s" % statuses)
        return 0.0, 0, total
    print("  aggregate throughput = %.0f msg/s (accepted %d of %d)"
          % (accepted / elapsed, accepted, total))
    print("  latency p50=%.2f ms  p95=%.2f ms  p99=%.2f ms" % (
        latencies[min(accepted - 1, int(accepted * 0.50))],
        latencies[min(accepted - 1, int(accepted * 0.95))],
        latencies[min(accepted - 1, int(accepted * 0.99))]))
    print("  status codes = %s" % statuses)
    return accepted / elapsed, accepted, total


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


def run_drained(topic, threads, per_thread, drain_timeout=120.0):
    """Publish with a consumer attached, then wait for the queue to fully drain.

    Two things matter for the number to be honest:

    * The consumer stays attached until every accepted message has been acknowledged. Stopping
      it as soon as the publishers finish would leave unacknowledged in-flight messages that the
      next run inherits, so a later run measures recovery rather than throughput.
    * The reported rate is the publish rate over the publishing phase, and the drain time is
      reported separately. Hiding a slow drain inside the publish figure is how a benchmark ends
      up flattering a store that cannot keep up.
    """
    stop = threading.Event()
    counter = {"acked": 0}
    expected = threads * per_thread
    thread = threading.Thread(target=consumer, args=(topic, stop, counter))
    thread.start()
    try:
        time.sleep(0.2)  # let the subscription register before the first publish
        rate, accepted, requested = run(topic, threads, per_thread)
        # Only accepted messages can ever be acknowledged, so they set the drain target.
        expected = accepted
        drain_started = time.perf_counter()
        deadline = drain_started + drain_timeout
        while counter["acked"] < expected and time.perf_counter() < deadline:
            time.sleep(0.05)
        drain_elapsed = time.perf_counter() - drain_started
    finally:
        stop.set()
        thread.join(timeout=5)

    drained = counter["acked"] >= expected
    print("  acknowledged by consumer = %d / %d accepted" % (counter["acked"], expected))
    if accepted < requested:
        print("  %d of %d publishes were refused (see status codes); the drain target is "
              "the accepted count" % (requested - accepted, requested))
    print("  drain time = %.2fs%s" % (
        drain_elapsed, "" if drained else "  (TIMED OUT, queue did not fully drain)"))
    if accepted == 0:
        print("  NOTE: nothing was accepted, so this run carries no throughput figure.")
        return None
    if not drained:
        print("  NOTE: this run is NOT a sustainable-throughput figure; the store could not "
              "drain the backlog.")
        return None
    return rate


def main():
    global BASE
    parser = argparse.ArgumentParser()
    parser.add_argument("--threads", type=int, default=16)
    parser.add_argument("--per-thread", type=int, default=200)
    parser.add_argument("--base", default=BASE)
    parser.add_argument("--label", default="",
                        help="label recorded in the output, e.g. 'postgres'")
    parser.add_argument("--repeat", type=int, default=1,
                        help="run the drained scenario N times to show the range")
    parser.add_argument("--skip-saturated", action="store_true",
                        help="skip the saturated scenario; it measures the publish "
                             "timeout, not broker throughput")
    args = parser.parse_args()

    BASE = args.base
    if args.label:
        print("### store=%s ###" % args.label)

    print("=== sequential (1 publisher thread, no consumer) ===")
    run("bench-sequential", 1, args.per_thread)  # tuple ignored: raw request cost

    print()
    rates = []
    for attempt in range(args.repeat):
        # A fresh topic per run: reusing one would let a previous run's residue decide what
        # this run measures.
        topic = "bench-drained-%d" % (attempt + 1)
        print("=== drained, run %d of %d (publishers + WebSocket consumer) ==="
              % (attempt + 1, args.repeat))
        rates.append(run_drained(topic, args.threads, args.per_thread))
        print()

    valid = [r for r in rates if r is not None]
    if len(valid) > 1:
        low, high = min(valid), max(valid)
        print("drained throughput across %d fully-drained runs: %.0f - %.0f msg/s"
              % (len(valid), low, high))
        if len(valid) < len(rates):
            print("  (%d of %d runs excluded because the backlog did not drain)"
                  % (len(rates) - len(valid), len(rates)))
        print()

    if not args.skip_saturated:
        print("=== saturated (no consumer: measures the 2s publish timeout, "
              "NOT broker throughput) ===")
        run("bench-saturated", args.threads, args.per_thread)  # tuple ignored


if __name__ == "__main__":
    main()