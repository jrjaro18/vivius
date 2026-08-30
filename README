# vivius

A from-scratch implementation of the Raft consensus algorithm in Go, providing a **replicated** key-value store.

> **Terminology note:** vivius is a *replicated* KV store, not a *distributed* (partitioned/sharded) one. Raft replicates a single log across nodes for fault tolerance — it does not partition the keyspace; every node holds the full dataset. Systems like Aerospike, Kafka, CockroachDB, and TiKV additionally partition data across independent replication groups for horizontal scale — sharding is a separate concern layered on top of replication, not something Raft itself provides, and vivius deliberately doesn't do it.

## What's implemented

- Leader election — randomized timeouts, term-based safety, election restriction (§5.4.1: candidate's log must be at least as up-to-date, compared by last-entry term first, then index).
- Log replication via `AppendEntries` — log matching property, conflict resolution (truncate-and-overwrite of uncommitted suffixes).
- Commit-index advancement gated by majority replication, correctly handling the §5.4.2 rule that a leader can only directly commit entries from its own current term.
- A simple KV state machine (`Put`/`Delete`), applied strictly in committed log order.
- Persistent state (`currentTerm`, `votedFor`, `log`) via a swappable `PersistentStore` interface (in-memory implementation included).
- A simulated, in-process `Transport`, enabling deterministic fault injection (partition/heal) in tests without real sockets.
- A failure-scenario test suite exercising split-brain and stale-leader safety properties under partition.

## Running it

```bash
go run ./cmd/main.go          # starts a 3-node cluster, elects a leader
go test ./...                 # run all tests
go test -count=10 ./cluster/... # repeat to check for flakiness in timing-sensitive election tests
```

## Explicitly out of scope, and why

| Feature | Why it's excluded |
|---|---|
| Log compaction / snapshotting (§7) | Requires atomically persisting state-machine state alongside a corresponding log index — a real correctness problem, detailed below. Log grows unboundedly; fine at test scale, not at production scale. |
| Linearizable reads / read-index / leader leases (§8) | A partitioned former-leader can serve stale reads with no signal anything is wrong — detailed below. |
| Dynamic membership / joint consensus (§6) | Cluster membership is static, fixed at startup. Changing it safely mid-flight requires joint consensus to avoid two groups both believing they hold a majority under different configurations. |
| Sharding / multiple Raft groups | Out of scope by definition — this is what would make vivius "distributed" in the partitioning sense. It remains single-shard. |
| Real network transport (gRPC/TCP) | Correctness and fault-injection testing were prioritized over network plumbing. The `Transport` interface boundary means a real implementation could be added without touching the Raft core. |

---

## Design notes

The sections below go deeper into *why* certain things are built the way they are — useful if you want to understand the reasoning, not just the code.

### Architecture

```
Node (Raft core: role, term, votedFor, commitIndex, lastApplied)
 ├── PersistentStore  — durable log + term/vote (swappable: in-memory today)
 ├── DataStore        — the actual KV state, rebuilt purely by log replay
 └── Transport         — RPC delivery (swappable: in-process simulation today)
```

The Raft core has no knowledge of *how* messages are transported or *how* state is persisted — both are interfaces, so fault injection and future backends (real disk, real network) can be added without touching election, replication, or safety logic.

### Why the store is never persisted directly

The KV store is intentionally **not** durable on its own. On restart, a node's store is rebuilt from scratch by replaying every committed log entry from index 1, via the same `applyEntries()` path used during normal operation.

This is deliberate, not an oversight: persisting the store directly requires also persisting `lastApplied` *atomically* alongside it — otherwise a crash between a store write and a `lastApplied` update either re-applies an entry or skips one. Solving this correctly is exactly what log compaction/snapshotting (§7) exists to do, bundling store state and a corresponding log index into one atomically-persisted unit. Since compaction is out of scope, persisting the store directly without it would introduce a real consistency-drift risk for no benefit at this project's scale.

### Why `commitIndex`/`lastApplied` are volatile, not persisted

Per Figure 2, these are correctly volatile state. A restarting node resets both to 0 and recovers the correct value not from its own memory, but from the current leader's next `AppendEntries` (via `LeaderCommit`). This is safe because replaying the same command sequence is deterministic — reapplying committed `Put`/`Delete` operations from scratch always converges to the identical final state, regardless of when a node restarts.

### Tested failure scenarios

Five scenarios exercise the safety properties central to Raft's design, using the simulated transport's partition/heal fault injection:

1. **Minority-partition leader cannot commit.** A leader isolated in a minority can never advance its commit index — writes submitted to it fail outright, since it can never gather enough acknowledgments.
2. **Majority partition elects a new leader and continues making progress.** The majority side continues accepting and committing writes normally while a minority is unreachable.
3. **Partition heals and the cluster converges.** All nodes — including previously-isolated ones — converge on identical, correct store state via log-matching/conflict-resolution and catch-up (`nextIndex`/`matchIndex` backoff).
4. **A stale leader's uncommitted entry is discarded on rejoin.** An entry written by a deposed leader that was never replicated to a majority is correctly overwritten once that leader rejoins and receives the legitimate leader's conflicting log.
5. **An old leader cannot disrupt an ongoing/completed election.** A node that resurfaces after being isolated correctly steps down and does not regain or retain leadership improperly.

### Known limitations (named deliberately, not oversights)

**Stale reads from a partitioned former leader.** If a client can still reach a node that was recently deposed as leader (e.g., isolated in a minority partition but on the client's same side), a naive `Get` answers from local state without confirming current leadership. The former leader has no way to know it's been superseded until it next receives a message carrying a higher term — so it can serve an honestly-stale answer with no indication anything is wrong. This is precisely the gap read-index or leader-lease mechanisms (§8) close; both are out of scope, and the limitation is real, not theoretical.

**Unbounded log growth.** Without compaction, every node retains its full history indefinitely — fine at this project's test scale, not viable long-running in production.

**Static cluster membership.** The node set must be fixed and identical at startup. Growing/shrinking requires a full restart with new configuration, not a live membership change — doing that safely requires joint consensus (§6), nontrivial correctness-critical work outside this project's scope.

**Follower catch-up retries synchronously, one round-trip at a time.** Functionally correct, but not optimized beyond what a single retry naturally sends.

### Why Raft, and why from scratch

Raft was chosen over Paxos-family alternatives for a solo, from-scratch implementation specifically because it was designed for understandability — its decomposition into independently-reasoned-about subproblems (leader election, log replication, safety) made it possible to build and verify incrementally, one mechanism at a time.

This project doesn't use `hashicorp/raft` or any existing library — the value is in having reasoned through, and being able to defend, every core safety argument: why the election restriction compares term before index, why a leader can't directly commit entries from a prior term by replica count alone (§5.4.2 / Figure 8), why persisted and volatile state are split the way they are, and why appended and committed are meaningfully different states for a log entry.