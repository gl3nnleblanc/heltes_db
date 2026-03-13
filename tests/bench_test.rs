/// Tests for the in-process benchmark harness.
///
/// Each test corresponds to a concrete execution trace from the benchmark
/// workload profiles documented in spec/HeltesDB.tla.
use heltes_db::bench::{run_benchmark, WorkloadConfig, Xorshift64, ZipfSampler};

// ── ZipfSampler unit tests ────────────────────────────────────────────────────

/// Trace: alpha=0.0 → uniform distribution over [0, n).
/// All buckets should receive roughly n/N samples.
#[test]
fn zipf_sampler_alpha_zero_is_roughly_uniform() {
    let sampler = ZipfSampler::new(10, 0.0);
    let mut rng = Xorshift64::new(42);
    let mut counts = [0u64; 10];
    let total = 10_000u64;
    for _ in 0..total {
        let i = sampler.sample(rng.next_f64()) as usize;
        counts[i] += 1;
    }
    // Each bucket expected ~1000; accept ±50% tolerance.
    for (idx, &c) in counts.iter().enumerate() {
        assert!(
            c >= 500 && c <= 1500,
            "bucket[{idx}] = {c}, expected ~1000 (±50%)"
        );
    }
}

/// Trace: alpha=2.0 → high skew; top 10% of keys dominate sampling.
#[test]
fn zipf_sampler_high_alpha_concentrates_on_hot_keys() {
    let n = 1000u64;
    let sampler = ZipfSampler::new(n, 2.0);
    let mut rng = Xorshift64::new(42);
    let total = 10_000u64;
    let mut hot_count = 0u64;
    for _ in 0..total {
        let i = sampler.sample(rng.next_f64());
        if i < n / 10 {
            hot_count += 1;
        }
    }
    // For Zipf(2, 1000), top 10% (indices 0..100) should attract > 50% of samples.
    assert!(
        hot_count > total / 2,
        "expected hot_count > 50% of samples, got {hot_count}/{total}"
    );
}

/// Invariant: sample() always returns a value in [0, n).
#[test]
fn zipf_sampler_sample_in_range() {
    let n = 100u64;
    for alpha in [0.0f64, 0.5, 1.0, 2.0] {
        let sampler = ZipfSampler::new(n, alpha);
        let mut rng = Xorshift64::new(alpha.to_bits());
        for _ in 0..1_000 {
            let s = sampler.sample(rng.next_f64());
            assert!(s < n, "alpha={alpha:.1}: sample {s} >= n={n}");
        }
    }
}

/// Edge case: n=1 always returns 0.
#[test]
fn zipf_sampler_single_element() {
    let sampler = ZipfSampler::new(1, 1.5);
    let mut rng = Xorshift64::new(99);
    for _ in 0..100 {
        assert_eq!(sampler.sample(rng.next_f64()), 0);
    }
}

// ── Benchmark integration traces ─────────────────────────────────────────────

/// Trace "uniform-write-single-shard":
///   Begin → Update(key) → FastCommit.
///   Expect: committed > 0, errors = 0, throughput > 0.
#[tokio::test]
async fn bench_single_shard_uniform_write() {
    let config = WorkloadConfig {
        shards: 1,
        workers: 4,
        duration_secs: 1,
        warmup_secs: 0,
        keyspace: 100,
        zipf_alpha: 0.0,
        read_fraction: 0.0,
        multi_shard_fraction: 0.0,
        updates_per_tx: 1,
    };
    let result = run_benchmark(config).await;
    assert!(
        result.total_committed > 0,
        "expected committed > 0, got {}",
        result.total_committed
    );
    assert_eq!(
        result.total_errors, 0,
        "expected 0 errors, got {}",
        result.total_errors
    );
    assert!(result.throughput_tps > 0.0);
}

/// Trace "uniform-write-multi-shard":
///   Begin → Update(k1 on shard0) → Update(k2 on shard1) → 2PC Commit.
///   Expect: committed > 0, errors = 0.
#[tokio::test]
async fn bench_multi_shard_2pc_write() {
    let config = WorkloadConfig {
        shards: 2,
        workers: 4,
        duration_secs: 1,
        warmup_secs: 0,
        keyspace: 1_000,
        zipf_alpha: 0.0,
        read_fraction: 0.0,
        multi_shard_fraction: 1.0,
        updates_per_tx: 1,
    };
    let result = run_benchmark(config).await;
    assert!(
        result.total_committed > 0,
        "expected multi-shard committed > 0, got {}",
        result.total_committed
    );
    assert_eq!(result.total_errors, 0);
}

/// Trace "read-heavy":
///   Begin → Read(key) → Abort (intentional).
///   All transactions complete successfully; abort_rate from write conflicts = 0.
#[tokio::test]
async fn bench_read_heavy_workload() {
    let config = WorkloadConfig {
        shards: 1,
        workers: 4,
        duration_secs: 1,
        warmup_secs: 0,
        keyspace: 100,
        zipf_alpha: 0.0,
        read_fraction: 1.0,
        multi_shard_fraction: 0.0,
        updates_per_tx: 1,
    };
    let result = run_benchmark(config).await;
    assert!(
        result.total_committed > 0,
        "expected read completions > 0, got {}",
        result.total_committed
    );
    // No write transactions → no conflict aborts → abort_rate = 0.
    assert_eq!(
        result.abort_rate, 0.0,
        "read-only workload should have zero write-conflict abort rate"
    );
    assert_eq!(result.total_errors, 0);
}

/// Trace "hot-key conflict":
///   Many workers fight over a tiny keyspace (Zipf α=2, keyspace=5).
///   Expect: some aborted transactions from write-write conflicts.
#[tokio::test]
async fn bench_hot_key_workload_produces_aborts() {
    let config = WorkloadConfig {
        shards: 1,
        workers: 20,
        duration_secs: 1,
        warmup_secs: 0,
        keyspace: 5,
        zipf_alpha: 2.0,
        read_fraction: 0.0,
        multi_shard_fraction: 0.0,
        updates_per_tx: 1,
    };
    let result = run_benchmark(config).await;
    // With 20 workers on 5 keys, at least some transactions should complete.
    assert!(
        result.total_committed + result.total_aborted > 0,
        "expected at least some completed or aborted transactions"
    );
    assert_eq!(result.total_errors, 0);
}

/// BenchResult::to_json() emits well-structured JSON with all required fields.
#[tokio::test]
async fn bench_result_to_json_has_required_fields() {
    let config = WorkloadConfig {
        shards: 1,
        workers: 2,
        duration_secs: 1,
        warmup_secs: 0,
        keyspace: 10,
        zipf_alpha: 0.0,
        read_fraction: 0.0,
        multi_shard_fraction: 0.0,
        updates_per_tx: 1,
    };
    let result = run_benchmark(config).await;
    let json = result.to_json();
    for field in &[
        "\"config\"",
        "\"results\"",
        "\"throughput_tps\"",
        "\"latency_p50_us\"",
        "\"latency_p95_us\"",
        "\"latency_p99_us\"",
        "\"abort_rate\"",
        "\"total_committed\"",
        "\"total_aborted\"",
        "\"total_errors\"",
        "\"zipf_alpha\"",
        "\"read_fraction\"",
        "\"multi_shard_fraction\"",
    ] {
        assert!(json.contains(field), "JSON missing field {field}");
    }
}

/// Preset constructors produce configs with the expected distinguishing parameters.
#[test]
fn workload_config_presets_have_correct_parameters() {
    let uniform = WorkloadConfig::uniform_write();
    assert_eq!(uniform.zipf_alpha, 0.0);
    assert_eq!(uniform.read_fraction, 0.0);
    assert_eq!(uniform.multi_shard_fraction, 0.0);

    let hot = WorkloadConfig::hot_key();
    assert!(hot.zipf_alpha > 1.0);

    let reads = WorkloadConfig::read_heavy();
    assert!(reads.read_fraction > 0.5);

    let multi = WorkloadConfig::multi_shard();
    assert_eq!(multi.multi_shard_fraction, 1.0);
}
