#!/usr/bin/env python3
"""Kill the broker under load and check that no accepted message is lost.

The test runs against the Docker Compose stack (server + PostgreSQL):

  1. Publishers send N messages, each with its own id, while a WebSocket consumer
     acknowledges them as they arrive.
  2. Once half of the messages have been accepted, the server container is killed
     with SIGKILL: no shutdown hook runs and nothing is flushed.
  3. The container is started again. Publishers retry every request that failed
     during the outage with the same message id, and the consumer reconnects.
  4. When publishing is over, the test waits until every accepted message has been
     delivered, then checks the guarantee: every message the broker answered 200 for
     must be delivered at least once. Duplicates are allowed (at-least-once) and
     counted.

Usage (from the repository root, with `docker compose up -d` running):

    python3 tools/crash_test.py --messages 3000 --threads 8
"""

import argparse
import base64
import collections
import json
import os
import select
import socket
import struct
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from bench import ws_send  # noqa: E402

BASE = "http://localhost:8080"
CONTAINER = "message-broker-server-1"


def publish_once(topic, message_id):
    body = json.dumps({"payload": "crash test %s" % message_id, "messageId": message_id}).encode()
    request = urllib.request.Request("%s/publish/%s" % (BASE, topic), data=body,
                                     headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            response.read()
            return 200
    except urllib.error.HTTPError as e:
        e.read()
        return e.code
    except (urllib.error.URLError, OSError):
        return None  # broker unreachable: retry later


def healthy():
    try:
        with urllib.request.urlopen(BASE + "/health", timeout=2) as response:
            return response.status == 200
    except (urllib.error.URLError, OSError):
        return False


def ws_connect_raw(path):
    """WebSocket handshake on a raw socket; returns the socket and any bytes read past it."""
    key = base64.b64encode(os.urandom(16)).decode()
    sock = socket.create_connection(("localhost", 8080), timeout=10)
    sock.sendall(("GET %s HTTP/1.1\r\nHost: localhost:8080\r\nUpgrade: websocket\r\n"
                  "Connection: Upgrade\r\nSec-WebSocket-Key: %s\r\n"
                  "Sec-WebSocket-Version: 13\r\n\r\n" % (path, key)).encode())
    data = b""
    while b"\r\n\r\n" not in data:
        chunk = sock.recv(4096)
        if not chunk:
            raise ConnectionError("handshake failed")
        data += chunk
    head, _, rest = data.partition(b"\r\n\r\n")
    if b" 101 " not in head.split(b"\r\n", 1)[0]:
        raise ConnectionError("handshake refused")
    sock.settimeout(None)
    return sock, rest


class FrameReader:
    """Parse WebSocket frames from a raw socket without losing bytes.

    Reading through a buffered file object with a socket timeout can drop data when
    the timeout fires mid-read, so the socket is polled with select and frames are
    parsed from an explicit buffer instead.
    """

    def __init__(self, sock):
        self.sock = sock
        self.buf = b""

    def _need(self, n, stop):
        while len(self.buf) < n:
            if stop.is_set():
                return False
            ready, _, _ = select.select([self.sock], [], [], 0.5)
            if not ready:
                continue
            chunk = self.sock.recv(65536)
            if not chunk:
                raise ConnectionError("closed")
            self.buf += chunk
        return True

    def next_frame(self, stop):
        if not self._need(2, stop):
            return None
        b0, b1 = self.buf[0], self.buf[1]
        length, offset = b1 & 0x7F, 2
        if length == 126:
            if not self._need(4, stop):
                return None
            length, offset = struct.unpack(">H", self.buf[2:4])[0], 4
        elif length == 127:
            if not self._need(10, stop):
                return None
            length, offset = struct.unpack(">Q", self.buf[2:10])[0], 10
        if not self._need(offset + length, stop):
            return None
        payload = self.buf[offset:offset + length]
        self.buf = self.buf[offset + length:]
        return b0 & 0x0F, payload


def consumer(topic, received, lock, stop):
    """Acknowledge every delivery, reconnecting whenever the connection drops."""
    while not stop.is_set():
        try:
            sock, leftover = ws_connect_raw("/subscribe/%s" % topic)
        except OSError:
            time.sleep(0.5)
            continue
        reader = FrameReader(sock)
        reader.buf = leftover  # bytes after the handshake belong to the first frames
        while not stop.is_set():
            try:
                frame = reader.next_frame(stop)
            except (OSError, ConnectionError):
                break
            if frame is None or frame[0] == 0x8:
                break
            if frame[0] != 0x1:
                continue
            try:
                message_id = json.loads(frame[1].decode())["id"]
            except (ValueError, KeyError):
                continue
            with lock:
                received[message_id] += 1
            try:
                ws_send(sock, "ACK %s" % message_id)
            except OSError:
                break
        try:
            sock.close()
        except OSError:
            pass
        time.sleep(0.5)


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--messages", type=int, default=3000)
    parser.add_argument("--threads", type=int, default=8)
    args = parser.parse_args()

    run = int(time.time())
    topic = "crash-%d" % run
    ids = ["crash-%d-%05d" % (run, i) for i in range(args.messages)]

    lock = threading.Lock()
    received = collections.Counter()
    accepted = set()
    retries = collections.Counter()
    stop = threading.Event()
    crash = {}

    consumer_thread = threading.Thread(target=consumer, args=(topic, received, lock, stop), daemon=True)
    consumer_thread.start()
    time.sleep(1.0)

    def publisher(chunk):
        for message_id in chunk:
            while True:
                status = publish_once(topic, message_id)
                if status in (200, 409):
                    # 409: the broker already holds this id, accepted by an earlier
                    # attempt whose response was lost in the crash.
                    with lock:
                        accepted.add(message_id)
                    break
                retries[status] += 1
                time.sleep(0.3)

    def killer():
        while True:
            with lock:
                if len(accepted) >= args.messages // 2:
                    break
            time.sleep(0.01)
        with lock:
            crash["accepted_before"] = len(accepted)
            crash["received_before"] = sum(1 for c in received.values() if c)
        crash["at"] = time.time()
        subprocess.run(["docker", "kill", "--signal", "KILL", CONTAINER],
                       check=True, capture_output=True)
        time.sleep(1.0)
        subprocess.run(["docker", "start", CONTAINER], check=True, capture_output=True)
        while not healthy():
            time.sleep(0.2)
        crash["downtime"] = time.time() - crash["at"]

    started = time.time()
    chunks = [ids[i::args.threads] for i in range(args.threads)]
    publishers = [threading.Thread(target=publisher, args=(c,)) for c in chunks]
    killer_thread = threading.Thread(target=killer)
    killer_thread.start()
    for t in publishers:
        t.start()
    for t in publishers:
        t.join()
    killer_thread.join()
    publish_done = time.time() - started

    deadline = time.time() + 120
    while time.time() < deadline:
        with lock:
            missing = [m for m in accepted if received[m] == 0]
        if not missing:
            break
        time.sleep(0.5)
    stop.set()

    with lock:
        missing = sorted(m for m in accepted if received[m] == 0)
        duplicates = sum(c - 1 for c in received.values() if c > 1)
        unique = sum(1 for c in received.values() if c)
    print("topic                    %s" % topic)
    print("messages published       %d (%d publisher threads)" % (args.messages, args.threads))
    print("killed after             %d accepted, %d delivered" % (crash["accepted_before"], crash["received_before"]))
    print("broker downtime          %.1f s (SIGKILL, then restart and recovery)" % crash["downtime"])
    print("publish retries          %s" % dict(retries))
    print("accepted by the broker   %d" % len(accepted))
    print("delivered (unique)       %d" % unique)
    print("delivered twice or more  %d extra deliveries" % duplicates)
    print("accepted but never seen  %d" % len(missing))
    print("total time               %.1f s" % (time.time() - started))
    print("RESULT                   %s" % ("PASS: no accepted message was lost" if not missing
                                          else "FAIL: %d accepted messages lost" % len(missing)))
    return 0 if not missing else 1


if __name__ == "__main__":
    sys.exit(main())
