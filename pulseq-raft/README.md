# pulseq-raft

Raft consensus for PulseQ: leader election and log replication, written from scratch.
PulseQ's cluster mode is built on it (see "Cluster mode" in the main README).

## Design

`RaftNode` has no threads, clock or network inside. Its owner calls `tick()` at a steady
rate, hands incoming messages to `step()`, and sends what `drainOutbox()` returns. Because
nothing in the node is timing-dependent, a whole cluster can run in one thread over a
simulated network, and any failure can be replayed from its seed.

| Class | Role |
|---|---|
| `RaftNode` | The algorithm: elections, replication, commit |
| `RaftStorage` / `MemoryRaftStorage` | What must survive a crash: term, vote, log |
| `StateMachine` | Receives committed commands in log order |
| `RaftMessage`, `Envelope`, `LogEntry`, `RaftConfig` | Messages, log entries, timing |
| `RaftRuntime` | Runs one node on its own thread; `propose()` returns a future that completes on commit |
| `LeadershipListener` | Told when the node leads *and* has applied everything earlier leaders committed |
| `Transport` / `LocalNetwork` | How messages travel; `LocalNetwork` links nodes inside one JVM |
| `TcpTransport` | Messages between processes over TCP, one connection per peer, reopened after a failure |
| `FileRaftStorage` | Log on disk: append-only with a checksum per record, cut at the first damaged record on opening |
| `RaftCodec` | Binary format of entries and messages, on disk and on the wire |

## Tests

`SimCluster` (test code) runs a cluster over a network that can delay, drop, duplicate and
reorder messages, split into partitions, and crash and restart nodes. On every tick it
checks the safety properties from the Raft paper:

- **Election safety**: at most one leader per term.
- **Log matching**: two logs that agree on the term at an index are identical up to it.
- **Leader completeness**: a new leader holds every entry committed in an earlier term.
- **State machine safety**: no two nodes apply different commands at the same index.

`RaftSimulationTest` has one named scenario per test, including the Figure 8 scenario from
the paper. `RaftFuzzTest` runs 600 random fault schedules (300 on 3 nodes, 300 on 5), then
stops the faults and requires every node to reach the same state. A further 20 schedules run
with the log on disk, closing the files on every crash and reopening them on restart.

`RaftRuntimeTest` runs three nodes on real threads and timers: commit, rejection on a
follower, takeover when the leader stops, and a command stranded on a cut-off leader.

```
mvn -pl pulseq-raft test                          # about 10 seconds
mvn -pl pulseq-raft test -Draft.fuzz.seeds=5000   # longer fuzz run
```

The tests were checked by breaking the algorithm on purpose. Each of these six changes makes
at least one test fail: voting without the log check, voting twice in a term, skipping the
previous-entry check, committing past the matched index, accepting a minority as a quorum,
and committing an earlier-term entry by counting replicas.

## Not implemented yet

- Membership changes (the cluster's members are fixed at start)
- Log compaction and snapshots (the log grows without bound)
- Linearizable reads, pre-vote, leadership transfer
- Authentication and encryption between nodes
