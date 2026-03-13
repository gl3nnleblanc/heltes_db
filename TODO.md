# TODO

## Correctness

- **Lamport clock not propagated in cross-coordinator Inquire replies** — `resolve_inquiry` receives `CommittedAt(ts)` from a peer coordinator but never advances the local coordinator's clock to `max(current, ts)`; coordinator A at clock=100 can then assign start_ts=101 on the next Begin and miss all writes committed by coordinator B at ts=102–5000 before the transaction started in real time, violating SI snapshot boundaries across coordinator boundaries; fix by adding a `coordinator_clock` field to `InquireReply` and advancing the caller's clock on receipt _(5 pts)_
- **2PC Prepare phase waits for all shards before surfacing first abort** — `join_all(prepare_futs)` blocks until every participating shard responds before the coordinator inspects any reply; if shard 1 returns Abort immediately but shard 2 is slow (or hits `shard_rpc_timeout`), the abort isn't surfaced until all futures settle, adding the slowest-shard latency to every aborted transaction; replace with `FuturesUnordered` so the coordinator detects and acts on the first Abort reply immediately _(3 pts)_
- **No admission control for concurrent coordinator transactions** — `Begin` accepts unlimited concurrent calls, each allocating a tx_id entry and eventually spawning shard RPC tasks; under a client flood the transaction table and tokio task queue grow without bound, exhausting coordinator memory before any individual transaction times out; add a semaphore-gated `max_concurrent_txs` limit in `begin()` that returns an explicit backpressure error rather than silently queueing _(3 pts)_
- **Handle coordinator crash mid-2PC** — transactions left in PREPARING or COMMIT_WAIT are permanently stuck on shards; need a recovery protocol or coordinator-side WAL _(13 pts)_
- **Abandoned transaction reaper** — a client that crashes mid-transaction holds write locks on shards forever; need a heartbeat/TTL mechanism so coordinators can detect and abort orphaned active transactions _(8 pts)_
- **COMMIT delivery is not retried on shard RPC timeout** — when a shard's COMMIT RPC times out, the coordinator marks the transaction committed locally but that shard never installs the write; other shards may have committed, leaving the dataset permanently inconsistent; add best-effort in-memory COMMIT retry with bounded exponential backoff so a transient timeout does not silently drop a committed write _(8 pts)_
- **`resolve_inquiry` has no per-call timeout** — the cross-coordinator `Inquire` gRPC call inside `resolve_inquiry` has no per-call deadline; only the outer `read_loop_timeout` bounds it; a single peer coordinator that accepts TCP connections but never responds blocks the reader for the full `read_loop_timeout` (up to 30 s by default) before aborting; wrap the `connect` and `inquire` calls in `shard_rpc_timeout` so one hung coordinator causes fast abort rather than prolonged stall _(3 pts)_
- **`write_buff` is unbounded per transaction** — a misbehaving client or very large transaction can call `Update` on an unlimited number of keys, growing the shard's `write_buff` and holding `write_key_owner` locks on all of them, potentially exhausting shard memory; add a configurable `max_writes_per_tx` limit enforced in `handle_update` that immediately aborts the transaction when exceeded _(3 pts)_
- **`handle_fast_commit` advances `s_clock` before checking `write_buff`** — lines 458-459 of `src/shard/mod.rs` unconditionally execute `clock += 1` before `write_buff.remove(&tx_id)`; a retry of FastCommit (e.g. coordinator crash+replay) passes the `aborted` guard, burns a second timestamp, and returns a different `commit_ts` than the first call — the spec guarantees idempotency but the code silently breaks it; check `write_buff` first and return the already-committed timestamp if the entry is gone _(5 pts)_

## Verification & Credibility

- **Elle/Jepsen fault-injection correctness testing** — the TLA+ spec and unit tests verify correctness under sequential execution; Elle (Kyle Kingsbury's checker) can verify SI and serializability anomaly-freedom under real faults — process kills, network partitions, clock skew — against a running cluster; passing Elle's SI checker would be the strongest public proof of correctness achievable and puts this in the same tier as production databases that have undergone Jepsen testing _(13 pts)_
- **Mechanized liveness proof with TLAPS** — the current TLA+ spec enforces safety invariants only; TLC cannot check liveness at scale; use TLAPS (the TLA+ Proof System) to write a mechanized proof that committed transactions eventually become durable and reads eventually terminate under fair scheduling; almost no open-source distributed systems carry a machine-checked liveness proof _(13 pts)_

## Benchmarking & Validation ← CURRENT SPRINT — work only from this section until fully drained

- **CI throughput regression gate** — run a fixed workload in CI on every PR and fail if throughput drops >10% or p99 latency increases >20% relative to the committed baseline; this prevents accidentally shipping regressions under the banner of "improvements" _(5 pts)_

## Performance

- **Replace per-shard `Mutex<ShardState>` with fine-grained concurrency** — current design serializes all shard operations; could use per-key locking or an async actor model to allow genuine parallelism _(8 pts)_
- **PreparedConflict O(P·W) scan on every update** — `handle_update`'s conflict check iterates all `s_prepared` entries for each key being written; with many concurrent 2PC transactions this becomes O(prepared_count × writes_per_tx) per update RPC; add an inverted index `prepared_by_key: HashMap<Key, BTreeSet<TxId>>` so conflict detection is O(1) per key _(3 pts)_
- **`expire_prepared` O(P) scan on every RPC** — called on every read, update, and prepare RPC to evict timed-out prepared entries; iterates all `s_prepared` entries each call; replace with a `BinaryHeap` ordered by `prepare_time` so expiry is O(log P) amortised and the common case (nothing expired) is O(1) _(3 pts)_
- **Pipeline coordinator lock acquisitions** — the coordinator `Mutex<CoordinatorState>` is acquired multiple times per transaction; batching or a lock-free structure would raise the coordinator throughput ceiling _(5 pts)_

## Durability

- **Write-ahead log (WAL) for shards** — shard state is in-memory only; a crash loses all MVCC history _(13 pts)_
- **Coordinator state persistence** — coordinator transaction table is in-memory; crash recovery requires replaying the WAL or a persistent log of commit decisions _(8 pts)_

## Operability

- **Multi-machine deployment** — coordinator→shard and coordinator→coordinator addressing is hardcoded to localhost; needs a service discovery mechanism or config-driven address resolution _(5 pts)_
- **Shard rebalancing** — consistent hashing minimises disruption on shard add/remove but there is no tooling to actually migrate versions to the new owner _(13 pts)_
- **Metrics and observability** — no latency histograms, queue depths, or conflict rate counters exposed from the server processes themselves _(5 pts)_
- **Graceful shutdown with in-flight drain** — SIGTERM kills coordinator and shard processes immediately; clients see connection resets and in-flight coordinator transactions are never aborted cleanly; install a signal handler that stops accepting new RPCs, waits for active RPCs to complete or time out, then exits _(5 pts)_
- **gRPC keepalive for shard channel health** — coordinator channels to shards have no HTTP/2 keepalive probes; a shard that restarts silently isn't detected until the next RPC times out; configure `connect_lazy` endpoints with keepalive so coordinators detect dead shard connections promptly _(2 pts)_
- **Coordinator transaction table is never evicted** — committed and aborted transaction entries accumulate in the coordinator's `transactions: HashMap` indefinitely with no eviction; under continuous load this is an unbounded memory leak that will eventually exhaust coordinator memory; add a background reaper that removes terminal entries after a configurable retention window long enough to answer any outstanding shard inquiry about recently committed transactions _(5 pts)_

## Protocol extensions

- **Read-only transactions** — currently unmodelled and unimplemented; read-only txns don't need 2PC and can be significantly cheaper _(5 pts)_
- **Multi-key atomic reads** — the current Read RPC is per-key; a snapshot read of multiple keys requires multiple round trips with no atomicity guarantee across them _(8 pts)_
- **Snapshot Isolation permits write-skew anomalies** — two concurrent transactions can each read an overlapping set of keys, then write to disjoint keys based on what they read, producing a result impossible under serializability (classic: two doctors both check "is another doctor on call?" and both go off-call); the current implementation has no read-set tracking and cannot detect anti-dependencies; spec and implement Serializable Snapshot Isolation (SSI) via read-key tracking in the coordinator and cycle detection at commit time _(13 pts)_

## Other

- **Client transaction library** — clients must currently hand-roll gRPC calls against the raw `CoordinatorService` proto; a thin `HeltesTx` handle wrapping `Begin/Read/Update/Commit/Abort` with proper error propagation would make the system usable and unblock the Frontend task _(5 pts)_
- **Frontend** - add a CLI frontend for running some limited SQL-like statements
