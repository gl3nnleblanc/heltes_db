# TODO

## Correctness

- **Handle coordinator crash mid-2PC** — transactions left in PREPARING or COMMIT_WAIT are permanently stuck on shards; need a recovery protocol or coordinator-side WAL _(13 pts)_
- **Abandoned transaction reaper** — a client that crashes mid-transaction holds write locks on shards forever; need a heartbeat/TTL mechanism so coordinators can detect and abort orphaned active transactions _(8 pts)_
- **COMMIT delivery is not retried on shard RPC timeout** — when a shard's COMMIT RPC times out, the coordinator marks the transaction committed locally but that shard never installs the write; other shards may have committed, leaving the dataset permanently inconsistent; add best-effort in-memory COMMIT retry with bounded exponential backoff so a transient timeout does not silently drop a committed write _(8 pts)_
- **`resolve_inquiry` has no per-call timeout** — the cross-coordinator `Inquire` gRPC call inside `resolve_inquiry` has no per-call deadline; only the outer `read_loop_timeout` bounds it; a single peer coordinator that accepts TCP connections but never responds blocks the reader for the full `read_loop_timeout` (up to 30 s by default) before aborting; wrap the `connect` and `inquire` calls in `shard_rpc_timeout` so one hung coordinator causes fast abort rather than prolonged stall _(3 pts)_
- **`write_buff` is unbounded per transaction** — a misbehaving client or very large transaction can call `Update` on an unlimited number of keys, growing the shard's `write_buff` and holding `write_key_owner` locks on all of them, potentially exhausting shard memory; add a configurable `max_writes_per_tx` limit enforced in `handle_update` that immediately aborts the transaction when exceeded _(3 pts)_

## Benchmarking & Validation ← CURRENT SPRINT — work only from this section until fully drained

- **End-to-end benchmark binary with configurable workload** — no reproducible benchmark exists; build a `bench` binary that spins up an in-process cluster (like the integration tests), runs N concurrent client goroutines for T seconds, and emits JSON with throughput (tx/s), latency percentiles (p50/p95/p99 in µs), and abort rate; must be fully scriptable so CI and humans get identical numbers _(5 pts)_
- **Workload profiles covering the conflict spectrum** — a single throughput number is meaningless without knowing the workload shape; the benchmark must support: (a) conflict rate via hot-key skew (Zipf α parameter), (b) read/write ratio, (c) single-shard vs multi-shard transaction mix; these are the knobs that determine whether a performance change is real or an artefact of the test _(3 pts)_
- **Committed baselines for all Performance TODOs** — before touching any Performance item, run the benchmark across at least three workload profiles and commit the results to `bench/baselines/<change-name>.json`; the PR that implements the change must include a before/after table; without this, performance claims are unverifiable _(3 pts)_
- **Criterion.rs micro-benchmarks for shard hot paths** — hot-path operations (`handle_read`, `handle_update`, `handle_commit`, `compact_versions`) have no isolated timing; add criterion.rs benchmarks so algorithmic changes can be validated independently of network noise _(3 pts)_
- **CI throughput regression gate** — run a fixed workload in CI on every PR and fail if throughput drops >10% or p99 latency increases >20% relative to the committed baseline; this prevents accidentally shipping regressions under the banner of "improvements" _(5 pts)_
- **NeedsInquiry end-to-end integration test** — the NeedsInquiry → coordinator inquiry → resolved-status → retry → correct version path is the most complex part of the SI protocol and has no integration-level test; add a test that prepares T2 (without committing) then starts T3 reading the same key, verifies T3 blocks and receives the pre-T2 version once inquiry resolves T2 as active _(3 pts)_

## Performance

- **Replace per-shard `Mutex<ShardState>` with fine-grained concurrency** — current design serializes all shard operations; could use per-key locking or an async actor model to allow genuine parallelism _(8 pts)_
- **PreparedConflict O(P·W) scan on every update** — `handle_update`'s conflict check iterates all `s_prepared` entries for each key being written; with many concurrent 2PC transactions this becomes O(prepared_count × writes_per_tx) per update RPC; add an inverted index `prepared_by_key: HashMap<Key, BTreeSet<TxId>>` so conflict detection is O(1) per key _(3 pts)_
- **`expire_prepared` O(P) scan on every RPC** — called on every read, update, and prepare RPC to evict timed-out prepared entries; iterates all `s_prepared` entries each call; replace with a `BinaryHeap` ordered by `prepare_time` so expiry is O(log P) amortised and the common case (nothing expired) is O(1) _(3 pts)_
- **Coordinator channels recreated per Inquire RPC** — `resolve_inquiry()` calls `CoordinatorServiceClient::connect(uri).await` on every cross-coordinator inquiry, incurring a full TCP + HTTP/2 handshake per call; cache clients in a `coordinator_clients: HashMap<SocketAddr, CoordinatorServiceClient>` the same way `shard_clients` are cached at coordinator construction _(2 pts)_
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

## Other

- **Client transaction library** — clients must currently hand-roll gRPC calls against the raw `CoordinatorService` proto; a thin `HeltesTx` handle wrapping `Begin/Read/Update/Commit/Abort` with proper error propagation would make the system usable and unblock the Frontend task _(5 pts)_
- **Frontend** - add a CLI frontend for running some limited SQL-like statements
