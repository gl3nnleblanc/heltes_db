//! In-process benchmark harness for HeltesDB.
//!
//! Spins up N shards + one coordinator in-process, drives a configurable
//! transaction workload for a fixed duration, and returns throughput / latency /
//! abort-rate metrics as a `BenchResult`.
//!
//! Workload profiles (see also spec/HeltesDB.tla — "Benchmark workload profiles"):
//!
//! | Profile              | Description                                         |
//! |----------------------|-----------------------------------------------------|
//! | uniform-write        | All writes, uniform key distribution, single shard  |
//! | hot-key              | Zipf α=2.0, high conflict rate                      |
//! | read-heavy           | Mostly read-only transactions                       |
//! | multi-shard          | All writes, every tx spans ≥2 shards (full 2PC)     |

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use futures::stream;
use tonic::transport::{Channel, Server};

use crate::coordinator::{
    routing::ConsistentHashRouter,
    server::{CoordinatorServer, CoordinatorServiceServer},
    CoordinatorState, ReadRetryPolicy,
};
use crate::proto::{
    coordinator_service_client::CoordinatorServiceClient, TxAbortRequest, TxBeginRequest,
    TxCommitRequest, TxReadRequest, TxUpdateRequest, TxWriteAndCommitRequest,
};
use crate::shard::{
    server::{ShardServer, ShardServiceServer},
    ShardState,
};

// ── Configuration ─────────────────────────────────────────────────────────────

/// Workload configuration for a single benchmark run.
#[derive(Clone, Debug)]
pub struct WorkloadConfig {
    /// Number of coordinator processes to spin up in-process.
    pub coordinators: usize,
    /// Number of shard processes to spin up in-process.
    pub shards: usize,
    /// Number of concurrent async worker tasks.
    pub workers: usize,
    /// Measurement window in seconds (warmup is excluded from stats).
    pub duration_secs: u64,
    /// Warmup period in seconds (transactions run but metrics are discarded).
    pub warmup_secs: u64,
    /// Number of distinct keys in the key space.
    pub keyspace: u64,
    /// Zipf skew parameter: 0.0 = uniform; values > 1.0 create hot-key pressure.
    pub zipf_alpha: f64,
    /// Fraction of transactions that are read-only (Begin → Read → Abort).
    pub read_fraction: f64,
    /// Fraction of write transactions that span multiple shards (triggers 2PC).
    pub multi_shard_fraction: f64,
    /// Number of keys updated per write transaction.
    pub updates_per_tx: usize,
}

impl Default for WorkloadConfig {
    fn default() -> Self {
        Self {
            coordinators: 1,
            shards: 4,
            workers: 50,
            duration_secs: 10,
            warmup_secs: 2,
            keyspace: 10_000,
            zipf_alpha: 0.0,
            read_fraction: 0.0,
            multi_shard_fraction: 0.0,
            updates_per_tx: 1,
        }
    }
}

impl WorkloadConfig {
    /// All writes, uniform key distribution, single-shard fast path.
    pub fn uniform_write() -> Self {
        Self::default()
    }

    /// High Zipf skew (α=2.0) to generate write-write conflicts.
    pub fn hot_key() -> Self {
        Self {
            zipf_alpha: 2.0,
            ..Self::default()
        }
    }

    /// 90% read-only transactions; minimal conflict pressure.
    pub fn read_heavy() -> Self {
        Self {
            read_fraction: 0.9,
            ..Self::default()
        }
    }

    /// All write transactions span two shards, exercising the full 2PC path.
    pub fn multi_shard() -> Self {
        Self {
            multi_shard_fraction: 1.0,
            ..Self::default()
        }
    }
}

// ── Result ────────────────────────────────────────────────────────────────────

/// Metrics from a completed benchmark run.
#[derive(Debug)]
pub struct BenchResult {
    /// Configuration used for this run.
    pub config: WorkloadConfig,
    /// Transactions committed per second (measurement window only).
    pub throughput_tps: f64,
    /// Median end-to-end latency in microseconds (committed txs only).
    pub latency_p50_us: u64,
    /// 95th-percentile latency in microseconds.
    pub latency_p95_us: u64,
    /// 99th-percentile latency in microseconds.
    pub latency_p99_us: u64,
    /// Fraction of write transactions that were aborted by the server (conflicts).
    /// Read-only transactions are not included (their abort is intentional).
    pub abort_rate: f64,
    /// Total transactions that completed successfully (writes committed + reads done).
    pub total_committed: u64,
    /// Total write transactions aborted by the server (conflict).
    pub total_aborted: u64,
    /// Total transactions that failed with a gRPC transport error.
    pub total_errors: u64,
}

impl BenchResult {
    /// Serialise to a JSON string (no external dependencies).
    pub fn to_json(&self) -> String {
        format!(
            "{{\n\
             \x20 \"config\": {{\n\
             \x20   \"coordinators\": {coordinators},\n\
             \x20   \"shards\": {shards},\n\
             \x20   \"workers\": {workers},\n\
             \x20   \"duration_secs\": {duration},\n\
             \x20   \"warmup_secs\": {warmup},\n\
             \x20   \"keyspace\": {keyspace},\n\
             \x20   \"zipf_alpha\": {alpha:.2},\n\
             \x20   \"read_fraction\": {read_frac:.2},\n\
             \x20   \"multi_shard_fraction\": {multi_shard:.2},\n\
             \x20   \"updates_per_tx\": {updates}\n\
             \x20 }},\n\
             \x20 \"results\": {{\n\
             \x20   \"throughput_tps\": {tps:.1},\n\
             \x20   \"latency_p50_us\": {p50},\n\
             \x20   \"latency_p95_us\": {p95},\n\
             \x20   \"latency_p99_us\": {p99},\n\
             \x20   \"abort_rate\": {abort:.4},\n\
             \x20   \"total_committed\": {committed},\n\
             \x20   \"total_aborted\": {aborted},\n\
             \x20   \"total_errors\": {errors}\n\
             \x20 }}\n\
             }}",
            coordinators = self.config.coordinators,
            shards = self.config.shards,
            workers = self.config.workers,
            duration = self.config.duration_secs,
            warmup = self.config.warmup_secs,
            keyspace = self.config.keyspace,
            alpha = self.config.zipf_alpha,
            read_frac = self.config.read_fraction,
            multi_shard = self.config.multi_shard_fraction,
            updates = self.config.updates_per_tx,
            tps = self.throughput_tps,
            p50 = self.latency_p50_us,
            p95 = self.latency_p95_us,
            p99 = self.latency_p99_us,
            abort = self.abort_rate,
            committed = self.total_committed,
            aborted = self.total_aborted,
            errors = self.total_errors,
        )
    }
}

// ── Baseline & regression check ───────────────────────────────────────────────

/// Minimum acceptable performance stored in `bench_baseline.json`.
///
/// The baseline is extracted from the `results` block of a full `BenchResult`
/// JSON, so a result file produced with `--out` can be used directly as a
/// baseline with `--check-baseline`.
#[derive(Debug, Clone)]
pub struct BenchBaseline {
    /// Minimum acceptable throughput (tx/s).
    pub throughput_tps: f64,
    /// Maximum acceptable p99 latency (µs).
    pub latency_p99_us: u64,
}

/// Outcome of a regression check.
#[derive(Debug)]
pub struct RegressionResult {
    /// current_tps / baseline_tps  (< 1.0 means slower).
    pub tps_ratio: f64,
    /// current_p99 / baseline_p99  (> 1.0 means higher latency).
    pub p99_ratio: f64,
    /// True when tps_ratio < (1.0 - tps_threshold).
    pub tps_regressed: bool,
    /// True when p99_ratio > (1.0 + p99_threshold).
    pub p99_regressed: bool,
}

impl RegressionResult {
    /// True if any metric regressed.
    pub fn any_regression(&self) -> bool {
        self.tps_regressed || self.p99_regressed
    }
}

impl BenchBaseline {
    /// Parse from the JSON format produced by `BenchResult::to_json()`.
    ///
    /// Accepts either the full result envelope (with a `"results"` key) or a
    /// flat JSON object containing `throughput_tps` and `latency_p99_us` directly.
    pub fn from_json(s: &str) -> Result<Self, String> {
        let tps = Self::extract_f64(s, "throughput_tps")
            .ok_or_else(|| "missing \"throughput_tps\" in baseline JSON".to_string())?;
        let p99 = Self::extract_u64(s, "latency_p99_us")
            .ok_or_else(|| "missing \"latency_p99_us\" in baseline JSON".to_string())?;
        Ok(BenchBaseline {
            throughput_tps: tps,
            latency_p99_us: p99,
        })
    }

    /// Serialise to a minimal JSON string suitable for committing as a baseline.
    pub fn to_json(&self) -> String {
        format!(
            "{{\n  \"throughput_tps\": {:.1},\n  \"latency_p99_us\": {}\n}}",
            self.throughput_tps, self.latency_p99_us
        )
    }

    /// Check `result` against this baseline.
    ///
    /// `tps_drop_threshold` is the fractional drop that counts as a regression
    /// (e.g. `0.10` means "fail if tps falls below 90 % of baseline").
    ///
    /// `p99_rise_threshold` is the fractional rise that counts as a regression
    /// (e.g. `0.20` means "fail if p99 exceeds 120 % of baseline").
    pub fn check(
        &self,
        result: &BenchResult,
        tps_drop_threshold: f64,
        p99_rise_threshold: f64,
    ) -> RegressionResult {
        let tps_ratio = if self.throughput_tps > 0.0 {
            result.throughput_tps / self.throughput_tps
        } else {
            1.0
        };
        let p99_ratio = if self.latency_p99_us > 0 {
            result.latency_p99_us as f64 / self.latency_p99_us as f64
        } else {
            1.0
        };
        RegressionResult {
            tps_ratio,
            p99_ratio,
            tps_regressed: tps_ratio < (1.0 - tps_drop_threshold),
            p99_regressed: p99_ratio > (1.0 + p99_rise_threshold),
        }
    }

    // ── JSON extraction helpers ───────────────────────────────────────────────

    fn extract_f64(json: &str, key: &str) -> Option<f64> {
        let needle = format!("\"{}\":", key);
        let start = json.find(&needle)? + needle.len();
        let rest = json[start..].trim_start();
        // Consume an optional leading minus and digits/dot/exponent.
        let end = rest
            .find(|c: char| !matches!(c, '0'..='9' | '.' | '-' | '+' | 'e' | 'E'))
            .unwrap_or(rest.len());
        rest[..end].trim().parse().ok()
    }

    fn extract_u64(json: &str, key: &str) -> Option<u64> {
        let needle = format!("\"{}\":", key);
        let start = json.find(&needle)? + needle.len();
        let rest = json[start..].trim_start();
        let end = rest
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(rest.len());
        rest[..end].trim().parse().ok()
    }
}

impl From<&BenchResult> for BenchBaseline {
    fn from(r: &BenchResult) -> Self {
        BenchBaseline {
            throughput_tps: r.throughput_tps,
            latency_p99_us: r.latency_p99_us,
        }
    }
}

// ── Zipf sampler ──────────────────────────────────────────────────────────────

/// Zipf distribution sampler using the CDF-inversion method.
///
/// Samples integers in `[0, n)` with probability proportional to
/// `1 / (i + 1)^alpha`.  `alpha = 0.0` degenerates to uniform.
pub struct ZipfSampler {
    /// Cumulative probability distribution: `cdf[i] = P(X ≤ i)`.
    cdf: Vec<f64>,
}

impl ZipfSampler {
    /// Build a sampler for `n` elements with Zipf parameter `alpha`.
    pub fn new(n: u64, alpha: f64) -> Self {
        assert!(n > 0, "ZipfSampler: n must be positive");
        let mut sum = 0.0f64;
        let weights: Vec<f64> = (0..n)
            .map(|i| {
                let w = if alpha == 0.0 {
                    1.0
                } else {
                    1.0 / (i as f64 + 1.0).powf(alpha)
                };
                sum += w;
                w
            })
            .collect();
        let mut running = 0.0f64;
        let mut cdf: Vec<f64> = weights
            .into_iter()
            .map(|w| {
                running += w / sum;
                running
            })
            .collect();
        *cdf.last_mut().unwrap() = 1.0; // guard against floating-point underrun
        ZipfSampler { cdf }
    }

    /// Sample an index in `[0, n)` from a uniform `u` in `[0, 1)`.
    pub fn sample(&self, u: f64) -> u64 {
        let u = u.clamp(0.0, 1.0 - f64::EPSILON);
        let idx = match self
            .cdf
            .binary_search_by(|&x| x.partial_cmp(&u).unwrap_or(std::cmp::Ordering::Less))
        {
            Ok(i) | Err(i) => i,
        };
        idx.min(self.cdf.len() - 1) as u64
    }
}

// ── PRNG ──────────────────────────────────────────────────────────────────────

/// Minimal xorshift64 PRNG.  No external dependencies; suitable for workload
/// key sampling where statistical quality matters more than cryptographic
/// strength.
pub struct Xorshift64 {
    state: u64,
}

impl Xorshift64 {
    pub fn new(seed: u64) -> Self {
        // Avoid the degenerate all-zero state.
        let state = if seed == 0 { 0x9e3779b97f4a7c15 } else { seed };
        Self { state }
    }

    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    /// Uniform float in `[0, 1)`.
    pub fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
}

// ── Worker ────────────────────────────────────────────────────────────────────

struct WorkerStats {
    committed: u64,
    aborted: u64,
    errors: u64,
    /// End-to-end latencies (µs) for committed writes and completed reads.
    latencies_us: Vec<u64>,
}

enum TxOutcome {
    /// Write committed or read completed; latency should be recorded.
    Completed,
    /// Write transaction conflict-aborted by the server.
    Aborted,
    /// gRPC transport failure.
    Error,
}

async fn run_worker(
    mut client: CoordinatorServiceClient<Channel>,
    config: WorkloadConfig,
    all_keys: Vec<u64>,
    cross_shard_pairs: Vec<(u64, u64)>,
    worker_id: u64,
    warmup_end: Instant,
    bench_end: Instant,
) -> WorkerStats {
    let mut rng = Xorshift64::new(worker_id.wrapping_mul(0x9e3779b97f4a7c15).wrapping_add(1));
    let zipf = ZipfSampler::new(all_keys.len() as u64, config.zipf_alpha);
    let cross_zipf = if !cross_shard_pairs.is_empty() {
        Some(ZipfSampler::new(
            cross_shard_pairs.len() as u64,
            config.zipf_alpha,
        ))
    } else {
        None
    };
    let mut stats = WorkerStats {
        committed: 0,
        aborted: 0,
        errors: 0,
        latencies_us: Vec::new(),
    };

    loop {
        let now = Instant::now();
        if now >= bench_end {
            break;
        }
        let is_warmup = now < warmup_end;

        let is_read_only = rng.next_f64() < config.read_fraction;
        let is_multi_shard =
            !is_read_only && cross_zipf.is_some() && rng.next_f64() < config.multi_shard_fraction;

        let tx_start = Instant::now();
        let outcome = execute_transaction(
            &mut client,
            &mut rng,
            &zipf,
            &all_keys,
            &cross_shard_pairs,
            cross_zipf.as_ref(),
            is_read_only,
            is_multi_shard,
            config.updates_per_tx,
        )
        .await;
        let elapsed_us = tx_start.elapsed().as_micros() as u64;

        if !is_warmup {
            match outcome {
                TxOutcome::Completed => {
                    stats.committed += 1;
                    stats.latencies_us.push(elapsed_us);
                }
                TxOutcome::Aborted => {
                    stats.aborted += 1;
                }
                TxOutcome::Error => {
                    stats.errors += 1;
                }
            }
        }
    }

    stats
}

#[allow(clippy::too_many_arguments)]
async fn execute_transaction(
    client: &mut CoordinatorServiceClient<Channel>,
    rng: &mut Xorshift64,
    zipf: &ZipfSampler,
    all_keys: &[u64],
    cross_shard_pairs: &[(u64, u64)],
    cross_zipf: Option<&ZipfSampler>,
    is_read_only: bool,
    is_multi_shard: bool,
    updates_per_tx: usize,
) -> TxOutcome {
    use crate::proto::tx_commit_reply::Result as CR;
    use crate::proto::tx_update_reply::Result as UR;
    use crate::proto::Abort;

    let begin = match client.begin(TxBeginRequest {}).await {
        Ok(r) => r.into_inner(),
        Err(_) => return TxOutcome::Error,
    };
    let tx_id = begin.tx_id;

    // ── Read-only path ────────────────────────────────────────────────────────
    if is_read_only {
        let key_idx = zipf.sample(rng.next_f64()) as usize % all_keys.len();
        let key = all_keys[key_idx];
        // Result is intentionally ignored — we just exercise the read path.
        let _ = client.read(TxReadRequest { tx_id, key }).await;
        let _ = client.abort(TxAbortRequest { tx_id }).await;
        return TxOutcome::Completed;
    }

    // ── Write path ────────────────────────────────────────────────────────────
    let keys_to_write: Vec<u64> = if is_multi_shard && !cross_shard_pairs.is_empty() {
        let cz = cross_zipf.unwrap();
        let pair_idx = cz.sample(rng.next_f64()) as usize % cross_shard_pairs.len();
        let (k1, k2) = cross_shard_pairs[pair_idx];
        vec![k1, k2]
    } else {
        (0..updates_per_tx.max(1))
            .map(|_| all_keys[zipf.sample(rng.next_f64()) as usize % all_keys.len()])
            .collect()
    };

    // ── WriteAndCommit fast path: single-write single-shard ───────────────────
    // Collapses Begin+Update+Commit (5 hops) into Begin+WriteAndCommit (3 hops).
    // Only used when the transaction writes exactly one key and is not explicitly
    // multi-shard (multi-shard txs need 2PC, which WriteAndCommit does not support).
    if keys_to_write.len() == 1 && !is_multi_shard {
        use crate::proto::tx_write_and_commit_reply::Result as WR;
        let key = keys_to_write[0];
        let val = rng.next_u64() % 1_000_000;
        let reply = match client
            .write_and_commit(TxWriteAndCommitRequest {
                tx_id,
                key,
                value: val,
            })
            .await
        {
            Ok(r) => r.into_inner(),
            Err(_) => return TxOutcome::Error,
        };
        return match reply.result {
            Some(WR::CommitTs(_)) => TxOutcome::Completed,
            Some(WR::Abort(_)) => TxOutcome::Aborted,
            None => TxOutcome::Error,
        };
    }

    // ── Standard Update + Commit path: multi-write or multi-shard ────────────
    for &key in &keys_to_write {
        let val = rng.next_u64() % 1_000_000;
        let reply = match client
            .update(TxUpdateRequest {
                tx_id,
                key,
                value: val,
            })
            .await
        {
            Ok(r) => r.into_inner(),
            Err(_) => {
                let _ = client.abort(TxAbortRequest { tx_id }).await;
                return TxOutcome::Error;
            }
        };
        if matches!(reply.result, Some(UR::Abort(Abort {}))) {
            return TxOutcome::Aborted;
        }
    }

    let commit = match client.commit(TxCommitRequest { tx_id }).await {
        Ok(r) => r.into_inner(),
        Err(_) => return TxOutcome::Error,
    };
    match commit.result {
        Some(CR::CommitTs(_)) => TxOutcome::Completed,
        Some(CR::Abort(_)) => TxOutcome::Aborted,
        None => TxOutcome::Error,
    }
}

// ── In-process cluster helpers ────────────────────────────────────────────────

async fn spawn_shard_server() -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let incoming = stream::unfold(listener, |l| async move {
        let r = l.accept().await.map(|(s, _)| s);
        Some((r, l))
    });
    tokio::spawn(
        Server::builder()
            .add_service(ShardServiceServer::new(ShardServer::new(ShardState::new())))
            .serve_with_incoming(incoming),
    );
    addr
}

/// Spawn N coordinators with full peer awareness, return one client per coordinator.
///
/// All coordinator ports are bound upfront so each coordinator is constructed
/// with the complete peer list — matching the pattern in `src/bin/cluster.rs`.
async fn spawn_coordinator_cluster(
    n: usize,
    shard_addrs: Vec<SocketAddr>,
) -> Vec<CoordinatorServiceClient<Channel>> {
    assert!(n > 0);
    // Bind all listeners upfront so every coordinator knows its peers' addresses.
    let mut listeners = Vec::with_capacity(n);
    let mut coord_addrs = Vec::with_capacity(n);
    for _ in 0..n {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        coord_addrs.push(listener.local_addr().unwrap());
        listeners.push(listener);
    }

    let mut clients = Vec::with_capacity(n);
    for (i, listener) in listeners.into_iter().enumerate() {
        let addr = coord_addrs[i];
        let port = addr.port();
        let peers: Vec<SocketAddr> = coord_addrs
            .iter()
            .enumerate()
            .filter(|&(j, _)| j != i)
            .map(|(_, &a)| a)
            .collect();
        let incoming = stream::unfold(listener, |l| async move {
            let r = l.accept().await.map(|(s, _)| s);
            Some((r, l))
        });
        let server = CoordinatorServer::new(
            CoordinatorState::new(),
            port,
            shard_addrs.clone(),
            peers,
            Duration::from_secs(5),
            Duration::from_secs(10),
            ReadRetryPolicy::default_policy(),
        )
        .unwrap();
        server.sync_clock_from_shards().await;
        tokio::spawn(
            Server::builder()
                .add_service(CoordinatorServiceServer::new(server))
                .serve_with_incoming(incoming),
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
        clients.push(
            CoordinatorServiceClient::connect(format!("http://{addr}"))
                .await
                .unwrap(),
        );
    }
    clients
}

// ── Entry point ───────────────────────────────────────────────────────────────

/// Spin up an in-process cluster, run the workload, and return metrics.
///
/// The cluster is torn down implicitly when all spawned tokio tasks complete
/// (the tokio runtime drops the server futures when no more clients hold refs).
pub async fn run_benchmark(config: WorkloadConfig) -> BenchResult {
    assert!(config.coordinators > 0, "need at least one coordinator");
    assert!(config.shards > 0, "need at least one shard");
    assert!(config.workers > 0, "need at least one worker");
    assert!(config.keyspace > 0, "keyspace must be positive");

    // ── Spawn shards ──────────────────────────────────────────────────────────
    let mut shard_addrs = Vec::with_capacity(config.shards);
    for _ in 0..config.shards {
        shard_addrs.push(spawn_shard_server().await);
    }
    // Give shards a moment to start accepting before coordinators sync clocks.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // ── Spawn coordinators (peer-aware) ───────────────────────────────────────
    let coord_clients = spawn_coordinator_cluster(config.coordinators, shard_addrs.clone()).await;

    // ── Build key partition ───────────────────────────────────────────────────
    let router = ConsistentHashRouter::new(shard_addrs.iter().copied());
    let all_keys: Vec<u64> = (0..config.keyspace).collect();

    // Group keys by the shard they hash to.
    let mut shard_key_map: HashMap<SocketAddr, Vec<u64>> = HashMap::new();
    for &key in &all_keys {
        if let Some(addr) = router.shard_for_key(key) {
            shard_key_map.entry(addr).or_default().push(key);
        }
    }

    // Build cross-shard pairs: one key from each of the two most-populated shards.
    let cross_shard_pairs: Vec<(u64, u64)> = if shard_key_map.len() >= 2 {
        let mut lists: Vec<Vec<u64>> = shard_key_map.into_values().collect();
        // Sort to make the partition deterministic.
        lists.sort_by_key(|v| v[0]);
        lists[0]
            .iter()
            .zip(lists[1].iter())
            .map(|(&k1, &k2)| (k1, k2))
            .collect()
    } else {
        vec![]
    };

    // ── Launch workers (round-robin across coordinators) ──────────────────────
    let bench_start = Instant::now();
    let warmup_end = bench_start + Duration::from_secs(config.warmup_secs);
    let bench_end = warmup_end + Duration::from_secs(config.duration_secs);

    let mut handles = Vec::with_capacity(config.workers);
    for worker_id in 0..config.workers as u64 {
        let coord_client = coord_clients[worker_id as usize % coord_clients.len()].clone();
        handles.push(tokio::spawn(run_worker(
            coord_client,
            config.clone(),
            all_keys.clone(),
            cross_shard_pairs.clone(),
            worker_id,
            warmup_end,
            bench_end,
        )));
    }

    // ── Collect results ───────────────────────────────────────────────────────
    let mut total_committed = 0u64;
    let mut total_aborted = 0u64;
    let mut total_errors = 0u64;
    let mut all_latencies: Vec<u64> = Vec::new();

    for handle in handles {
        let stats = handle.await.unwrap();
        total_committed += stats.committed;
        total_aborted += stats.aborted;
        total_errors += stats.errors;
        all_latencies.extend(stats.latencies_us);
    }

    // ── Compute percentiles ───────────────────────────────────────────────────
    all_latencies.sort_unstable();
    let n = all_latencies.len();
    let percentile = |p: usize| -> u64 {
        if n == 0 {
            0
        } else {
            all_latencies[(n * p / 100).min(n - 1)]
        }
    };

    let throughput_tps = total_committed as f64 / config.duration_secs as f64;
    let abort_rate = if total_committed + total_aborted > 0 {
        total_aborted as f64 / (total_committed + total_aborted) as f64
    } else {
        0.0
    };

    BenchResult {
        config,
        throughput_tps,
        latency_p50_us: percentile(50),
        latency_p95_us: percentile(95),
        latency_p99_us: percentile(99),
        abort_rate,
        total_committed,
        total_aborted,
        total_errors,
    }
}

// ── BenchBaseline tests ───────────────────────────────────────────────────────

#[cfg(test)]
mod baseline_tests {
    use super::*;

    fn fake_result(tps: f64, p99: u64) -> BenchResult {
        BenchResult {
            config: WorkloadConfig::default(),
            throughput_tps: tps,
            latency_p50_us: 0,
            latency_p95_us: 0,
            latency_p99_us: p99,
            abort_rate: 0.0,
            total_committed: 0,
            total_aborted: 0,
            total_errors: 0,
        }
    }

    fn baseline(tps: f64, p99: u64) -> BenchBaseline {
        BenchBaseline {
            throughput_tps: tps,
            latency_p99_us: p99,
        }
    }

    // RT1: parse minimal JSON → fields extracted correctly.
    #[test]
    fn parse_minimal_json() {
        let json = r#"{"throughput_tps": 12345.6, "latency_p99_us": 9876}"#;
        let b = BenchBaseline::from_json(json).unwrap();
        assert!((b.throughput_tps - 12345.6).abs() < 0.1);
        assert_eq!(b.latency_p99_us, 9876);
    }

    // RT2: parse full BenchResult JSON (nested under "results") → fields found.
    #[test]
    fn parse_full_bench_result_json() {
        let result = fake_result(5000.0, 3000);
        let json = result.to_json();
        let b = BenchBaseline::from_json(&json).unwrap();
        assert!((b.throughput_tps - 5000.0).abs() < 1.0);
        assert_eq!(b.latency_p99_us, 3000);
    }

    // RT3: throughput drops exactly 10% → NOT a regression (strict inequality).
    #[test]
    fn tps_drop_exactly_at_threshold_is_not_regression() {
        let b = baseline(10000.0, 1000);
        let r = fake_result(9000.0, 1000); // exactly 10% drop
        let reg = b.check(&r, 0.10, 0.20);
        assert!(
            !reg.tps_regressed,
            "exactly 10% drop is on the boundary — not a regression"
        );
        assert!((reg.tps_ratio - 0.9).abs() < 1e-9);
    }

    // RT4: throughput drops more than 10% → regression.
    #[test]
    fn tps_drop_over_threshold_is_regression() {
        let b = baseline(10000.0, 1000);
        let r = fake_result(8999.0, 1000); // 10.01% drop
        let reg = b.check(&r, 0.10, 0.20);
        assert!(reg.tps_regressed, "drop > 10% must be flagged");
    }

    // RT5: throughput drops < 10% → no regression.
    #[test]
    fn tps_drop_within_threshold_no_regression() {
        let b = baseline(10000.0, 1000);
        let r = fake_result(9500.0, 1000); // 5% drop — fine
        let reg = b.check(&r, 0.10, 0.20);
        assert!(!reg.tps_regressed);
    }

    // RT6: throughput improves → no regression.
    #[test]
    fn tps_improvement_is_not_regression() {
        let b = baseline(10000.0, 1000);
        let r = fake_result(15000.0, 1000);
        let reg = b.check(&r, 0.10, 0.20);
        assert!(!reg.tps_regressed);
        assert!(reg.tps_ratio > 1.0);
    }

    // RT7: p99 rises exactly 20% → NOT a regression (strict inequality).
    #[test]
    fn p99_rise_exactly_at_threshold_is_not_regression() {
        let b = baseline(10000.0, 1000);
        let r = fake_result(10000.0, 1200); // exactly 20% higher
        let reg = b.check(&r, 0.10, 0.20);
        assert!(
            !reg.p99_regressed,
            "exactly 20% rise is on the boundary — not a regression"
        );
        assert!((reg.p99_ratio - 1.2).abs() < 1e-9);
    }

    // RT8: p99 rises more than 20% → regression.
    #[test]
    fn p99_rise_over_threshold_is_regression() {
        let b = baseline(10000.0, 1000);
        let r = fake_result(10000.0, 1201); // 20.1% higher
        let reg = b.check(&r, 0.10, 0.20);
        assert!(reg.p99_regressed, "p99 rise > 20% must be flagged");
    }

    // RT9: p99 rises < 20% → no regression.
    #[test]
    fn p99_rise_within_threshold_no_regression() {
        let b = baseline(10000.0, 1000);
        let r = fake_result(10000.0, 1100); // 10% higher — fine
        let reg = b.check(&r, 0.10, 0.20);
        assert!(!reg.p99_regressed);
    }

    // RT10: both tps and p99 regress → any_regression = true.
    #[test]
    fn any_regression_true_when_both_regress() {
        let b = baseline(10000.0, 1000);
        let r = fake_result(8000.0, 1300);
        let reg = b.check(&r, 0.10, 0.20);
        assert!(reg.tps_regressed);
        assert!(reg.p99_regressed);
        assert!(reg.any_regression());
    }

    // RT11: neither regresses → any_regression = false.
    #[test]
    fn any_regression_false_when_both_ok() {
        let b = baseline(10000.0, 1000);
        let r = fake_result(10000.0, 1000);
        let reg = b.check(&r, 0.10, 0.20);
        assert!(!reg.any_regression());
    }

    // RT12: missing field → parse error.
    #[test]
    fn parse_error_on_missing_tps_field() {
        let json = r#"{"latency_p99_us": 1000}"#;
        assert!(BenchBaseline::from_json(json).is_err());
    }

    // RT13: to_json / from_json round-trip.
    #[test]
    fn json_round_trip() {
        let b = baseline(32000.5, 2500);
        let json = b.to_json();
        let b2 = BenchBaseline::from_json(&json).unwrap();
        assert!((b2.throughput_tps - b.throughput_tps).abs() < 0.1);
        assert_eq!(b2.latency_p99_us, b.latency_p99_us);
    }

    // RT14: From<&BenchResult> extracts the right fields.
    #[test]
    fn from_bench_result_extracts_fields() {
        let r = fake_result(7777.0, 4321);
        let b = BenchBaseline::from(&r);
        assert!((b.throughput_tps - 7777.0).abs() < 0.1);
        assert_eq!(b.latency_p99_us, 4321);
    }
}
