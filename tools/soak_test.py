#!/usr/bin/env python3
"""Run the broker under steady load for a long period and watch its memory.

Publishers send messages at a steady rate while a WebSocket consumer acknowledges
them. Every sampling interval the script records the server container's memory
(from `docker stats`), the topic's queue depth and the publish and delivery counts.
At the end it checks that every accepted message was delivered, and prints the
samples so that memory growth, if any, is visible.

Usage (from the repository root, with `docker compose up -d` running):

    python3 tools/soak_test.py --minutes 30 --rate 200
"""

import argparse
import collections
import json
import os
import subprocess
import sys
import threading
import time
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from crash_test import consumer, publish_once  # noqa: E402

BASE = "http://localhost:8080"
CONTAINER = "message-broker-server-1"


def container_memory_mib():
    out = subprocess.run(["docker", "stats", "--no-stream", "--format", "{{.MemUsage}}", CONTAINER],
                         capture_output=True, text=True).stdout.strip()
    used = out.split("/")[0].strip()
    value = float(used[:-3])
    return value * 1024 if used.endswith("GiB") else value if used.endswith("MiB") else value / 1024


def depth(topic):
    with urllib.request.urlopen(BASE + "/metrics", timeout=5) as response:
        return json.load(response)["queueDepths"].get(topic, 0)


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--minutes", type=float, default=30)
    parser.add_argument("--rate", type=int, default=200, help="target messages per second")
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--sample", type=int, default=60, help="seconds between samples")
    args = parser.parse_args()

    topic = "soak-%d" % int(time.time())
    lock = threading.Lock()
    received = collections.Counter()
    accepted = set()
    failures = collections.Counter()
    stop_consumer = threading.Event()
    stop_publishing = threading.Event()

    threading.Thread(target=consumer, args=(topic, received, lock, stop_consumer), daemon=True).start()
    time.sleep(1.0)

    per_thread_interval = args.threads / float(args.rate)

    def publisher(index):
        n = 0
        next_at = time.perf_counter()
        while not stop_publishing.is_set():
            message_id = "%s-%d-%d" % (topic, index, n)
            status = publish_once(topic, message_id)
            if status in (200, 409):
                with lock:
                    accepted.add(message_id)
                n += 1
            else:
                failures[status] += 1
            next_at += per_thread_interval
            delay = next_at - time.perf_counter()
            if delay > 0:
                time.sleep(delay)

    started = time.time()
    threads = [threading.Thread(target=publisher, args=(i,)) for i in range(args.threads)]
    for t in threads:
        t.start()

    samples = []
    end = started + args.minutes * 60
    while time.time() < end:
        time.sleep(min(args.sample, max(0, end - time.time())))
        with lock:
            acc = len(accepted)
            got = sum(1 for c in received.values() if c)
        samples.append((time.time() - started, container_memory_mib(), depth(topic), acc, got))
        m, mem, d, a, g = samples[-1]
        print("t=%5.0fs  memory=%7.1f MiB  depth=%5d  accepted=%8d  delivered=%8d" % (m, mem, d, a, g),
              flush=True)

    stop_publishing.set()
    for t in threads:
        t.join()
    deadline = time.time() + 120
    while time.time() < deadline:
        with lock:
            missing = [m for m in accepted if received[m] == 0]
        if not missing:
            break
        time.sleep(1)
    stop_consumer.set()

    with lock:
        missing = [m for m in accepted if received[m] == 0]
        duplicates = sum(c - 1 for c in received.values() if c > 1)
    elapsed = time.time() - started
    memories = [s[1] for s in samples]
    half = len(memories) // 2
    print()
    print("duration                 %.1f min" % (elapsed / 60))
    print("accepted                 %d (%.0f msg/s)" % (len(accepted), len(accepted) / elapsed))
    print("publish failures         %s" % dict(failures))
    print("delivered twice or more  %d extra deliveries" % duplicates)
    print("accepted but never seen  %d" % len(missing))
    print("memory min / max         %.1f / %.1f MiB" % (min(memories), max(memories)))
    print("memory, 1st half mean    %.1f MiB" % (sum(memories[:half]) / max(half, 1)))
    print("memory, 2nd half mean    %.1f MiB" % (sum(memories[half:]) / max(len(memories) - half, 1)))
    print("RESULT                   %s" % ("PASS: no accepted message was lost" if not missing
                                          else "FAIL: %d accepted messages lost" % len(missing)))
    return 0 if not missing else 1


if __name__ == "__main__":
    sys.exit(main())
