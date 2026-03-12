# TODO

## Correctness

- **Handle coordinator crash mid-2PC** — transactions left in PREPARING or COMMIT_WAIT are permanently stuck on shards; need a recovery protocol or coordinator-side WAL _(13 pts)_
- **Abandoned transaction reaper** — a client that crashes mid-transaction holds write locks on shards forever; need a heartbeat/TTL mechanism so coordinators can detect and abort orphaned active transactions _(8 pts)_
- **Shard-side prepared-entry timeout** — a shard holding a PREPARED entry for a crashed coordinator is blocked until the coordinator recovers; shards should auto-abort prepared entries after a configurable TTL, unblocking conflicting writers _(5 pts)_
- **gRPC deadline propagation to shard RPCs** — `join_all(prepare_futs)` in the coordinator commit path has no deadline; a single hung shard blocks the coordinator indefinitely, stalling all transactions touching that shard _(3 pts)_
- **TxIdGen sequence continuity across coordinator restarts** — `TxIdGen` resets its sequence to 0 on every restart; if new transactions from seq=0 arrive at a shard that still has in-flight or aborted entries from a prior epoch, the `prune_aborted` watermark can incorrectly discard or retain entries, risking stale-abort suppression; fix by time-seeding or persisting the high-water sequence _(3 pts)_

## Performance

- **Replace per-shard `Mutex<ShardState>` with fine-grained concurrency** — current design serializes all shard operations; could use per-key locking or an async actor model to allow genuine parallelism _(8 pts)_
- **Pipeline coordinator lock acquisitions** — the coordinator `Mutex<CoordinatorState>` is acquired multiple times per transaction; batching or a lock-free structure would raise the coordinator throughput ceiling _(5 pts)_
- **MVCC version compaction** — `versions[key]` grows without bound as a key receives repeated writes; GC old versions that predate the earliest active snapshot (min `start_ts` across all in-flight transactions) to bound both memory and binary-search work _(8 pts)_

## Durability

- **Write-ahead log (WAL) for shards** — shard state is in-memory only; a crash loses all MVCC history _(13 pts)_
- **Coordinator state persistence** — coordinator transaction table is in-memory; crash recovery requires replaying the WAL or a persistent log of commit decisions _(8 pts)_

## Operability

- **Multi-machine deployment** — coordinator→shard and coordinator→coordinator addressing is hardcoded to localhost; needs a service discovery mechanism or config-driven address resolution _(5 pts)_
- **Shard rebalancing** — consistent hashing minimises disruption on shard add/remove but there is no tooling to actually migrate versions to the new owner _(13 pts)_
- **Metrics and observability** — no latency histograms, queue depths, or conflict rate counters exposed from the server processes themselves _(5 pts)_

## Protocol extensions

- **Read-only transactions** — currently unmodelled and unimplemented; read-only txns don't need 2PC and can be significantly cheaper _(5 pts)_
- **Multi-key atomic reads** — the current Read RPC is per-key; a snapshot read of multiple keys requires multiple round trips with no atomicity guarantee across them _(8 pts)_

## Other

- **Integration test harness** — all 219 tests exercise components in isolation; there are zero tests that spawn coordinator + shard servers over gRPC and run a full multi-shard transaction end-to-end; a tokio-based harness using random free ports would catch protocol and routing bugs that unit tests miss _(8 pts)_
- **Client transaction library** — clients must currently hand-roll gRPC calls against the raw `CoordinatorService` proto; a thin `HeltesTx` handle wrapping `Begin/Read/Update/Commit/Abort` with proper error propagation would make the system usable and unblock the Frontend task _(5 pts)_
- **Frontend** - add a CLI frontend for running some limited SQL-like statements
