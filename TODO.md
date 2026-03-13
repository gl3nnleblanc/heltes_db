# TODO

## Correctness

- **Handle coordinator crash mid-2PC** — transactions left in PREPARING or COMMIT_WAIT are permanently stuck on shards; need a recovery protocol or coordinator-side WAL _(13 pts)_
- **Abandoned transaction reaper** — a client that crashes mid-transaction holds write locks on shards forever; need a heartbeat/TTL mechanism so coordinators can detect and abort orphaned active transactions _(8 pts)_
- **Circular inquiry deadlock between coordinators** — the `resolve_inquiry` path forwards NeedsInquiry results across coordinators; if coordinator A awaits B's transaction status while B concurrently awaits A's, both block forever; add a hop counter or visited-set to detect cycles and abort one side _(5 pts)_
- **COMMIT delivery is not retried on shard RPC timeout** — when a shard's COMMIT RPC times out, the coordinator marks the transaction committed locally but that shard never installs the write; other shards may have committed, leaving the dataset permanently inconsistent; add best-effort in-memory COMMIT retry with bounded exponential backoff so a transient timeout does not silently drop a committed write _(8 pts)_
- **Orphaned aborted-entry accumulation from dead coordinator ports** — `prune_aborted()` only fires when a new transaction arrives from the same coordinator port; if a coordinator crashes and never restarts, its aborted tx_ids accumulate on shards indefinitely; add a shard-side background task that TTL-evicts per-port aborted entries after a configurable quiescence period _(5 pts)_

## Benchmarking & Validation ← do these before any Performance work

- **End-to-end benchmark binary with configurable workload** — no reproducible benchmark exists; build a `bench` binary that spins up an in-process cluster (like the integration tests), runs N concurrent client goroutines for T seconds, and emits JSON with throughput (tx/s), latency percentiles (p50/p95/p99 in µs), and abort rate; must be fully scriptable so CI and humans get identical numbers _(5 pts)_
- **Workload profiles covering the conflict spectrum** — a single throughput number is meaningless without knowing the workload shape; the benchmark must support: (a) conflict rate via hot-key skew (Zipf α parameter), (b) read/write ratio, (c) single-shard vs multi-shard transaction mix; these are the knobs that determine whether a performance change is real or an artefact of the test _(3 pts)_
- **Committed baselines for all Performance TODOs** — before touching any Performance item, run the benchmark across at least three workload profiles and commit the results to `bench/baselines/<change-name>.json`; the PR that implements the change must include a before/after table; without this, performance claims are unverifiable _(3 pts)_
- **Criterion.rs micro-benchmarks for shard hot paths** — hot-path operations (`handle_read`, `handle_update`, `handle_commit`, `compact_versions`) have no isolated timing; add criterion.rs benchmarks so algorithmic changes can be validated independently of network noise _(3 pts)_
- **CI throughput regression gate** — run a fixed workload in CI on every PR and fail if throughput drops >10% or p99 latency increases >20% relative to the committed baseline; this prevents accidentally shipping regressions under the banner of "improvements" _(5 pts)_

## Performance

- **Replace per-shard `Mutex<ShardState>` with fine-grained concurrency** — current design serializes all shard operations; could use per-key locking or an async actor model to allow genuine parallelism _(8 pts)_
- **Pipeline coordinator lock acquisitions** — the coordinator `Mutex<CoordinatorState>` is acquired multiple times per transaction; batching or a lock-free structure would raise the coordinator throughput ceiling _(5 pts)_
- **Compaction scans all keys on every tick** — `compact_versions()` iterates over the entire `versions` map every 100 ms regardless of recent write activity; on a shard with many keys but few active writers this is O(N_keys) wasted work per tick; track a `dirty_keys` set populated on each write/commit and restrict compaction to only those keys _(3 pts)_
- **NeedsInquiry retry loop spins without backoff** — the read inquiry loop retries at full speed until `read_loop_timeout`; under high prepared-writer contention this generates a thundering herd of redundant Inquire RPCs; add exponential backoff with jitter between retries _(2 pts)_

## Durability

- **Write-ahead log (WAL) for shards** — shard state is in-memory only; a crash loses all MVCC history _(13 pts)_
- **Coordinator state persistence** — coordinator transaction table is in-memory; crash recovery requires replaying the WAL or a persistent log of commit decisions _(8 pts)_

## Operability

- **Multi-machine deployment** — coordinator→shard and coordinator→coordinator addressing is hardcoded to localhost; needs a service discovery mechanism or config-driven address resolution _(5 pts)_
- **Shard rebalancing** — consistent hashing minimises disruption on shard add/remove but there is no tooling to actually migrate versions to the new owner _(13 pts)_
- **Metrics and observability** — no latency histograms, queue depths, or conflict rate counters exposed from the server processes themselves _(5 pts)_
- **Graceful shutdown with in-flight drain** — SIGTERM kills coordinator and shard processes immediately; clients see connection resets and in-flight coordinator transactions are never aborted cleanly; install a signal handler that stops accepting new RPCs, waits for active RPCs to complete or time out, then exits _(5 pts)_
- **gRPC keepalive for shard channel health** — coordinator channels to shards have no HTTP/2 keepalive probes; a shard that restarts silently isn't detected until the next RPC times out; configure `connect_lazy` endpoints with keepalive so coordinators detect dead shard connections promptly _(2 pts)_

## Protocol extensions

- **Read-only transactions** — currently unmodelled and unimplemented; read-only txns don't need 2PC and can be significantly cheaper _(5 pts)_
- **Multi-key atomic reads** — the current Read RPC is per-key; a snapshot read of multiple keys requires multiple round trips with no atomicity guarantee across them _(8 pts)_

## Other

- **Client transaction library** — clients must currently hand-roll gRPC calls against the raw `CoordinatorService` proto; a thin `HeltesTx` handle wrapping `Begin/Read/Update/Commit/Abort` with proper error propagation would make the system usable and unblock the Frontend task _(5 pts)_
- **Frontend** - add a CLI frontend for running some limited SQL-like statements
