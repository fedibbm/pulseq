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
import collections
import json
import os
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from bench import ws_connect, ws_read_frame, ws_send  # noqa: E402

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


def consumer(topic, received, lock, stop):
    """Acknowledge every delivery, reconnecting whenever the connection drops."""
    while not stop.is_set():
        try:
            sock, stream = ws_connect("/subscribe/%s" % topic)
        except OSError:
            time.sleep(0.5)
            continue
        sock.settimeout(1.0)
        while not stop.is_set():
            try:
                frame = ws_read_frame(stream)
            except socket.timeout:
                continue
            except OSError:
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
