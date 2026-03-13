//! Criterion micro-benchmarks for ShardState hot paths.
//!
//! Each benchmark group corresponds to a spec action documented in
//! spec/HeltesDB.tla under "Criterion micro-benchmark hot paths".
//!
//! Groups:
//!   handle_read         — binary-search cost over N MVCC versions (ShardRead)
//!   prepared_conflict   — O(P) PreparedConflict scan in handle_update (ShardUpdate)
//!   handle_commit       — install W writes + update write_keys index (ShardCommit)
//!   compact_versions    — watermark-gated MVCC pruning (ShardCompactVersions)

use std::collections::HashMap;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use heltes_db::shard::{InquiryStatus, ShardState, Value};

// ── Setup helpers (mirror tests/shard_bench_traces.rs) ───────────────────────

fn state_with_n_versions(key: u64, n_versions: u64) -> ShardState {
    let mut state = ShardState::new();
    for v in 1..=n_versions {
        let tx_id = v * 10_000;
        let start_ts = v * 1_000 - 500;
        let commit_ts = v * 1_000;
        state.handle_update(tx_id, start_ts, key, Value(v * 1_000));
        state.handle_prepare(tx_id);
        state.handle_commit(tx_id, commit_ts);
    }
    state
}

fn state_with_prepared_txs_on_other_keys(n_prepared: u64) -> ShardState {
    let mut state = ShardState::new();
    for i in 0..n_prepared {
        let tx_id = i + 1;
        let key = i + 100;
        state.handle_update(tx_id, 0, key, Value(i));
        state.handle_prepare(tx_id);
    }
    state
}

// ── B1: handle_read — binary-search over N MVCC versions ─────────────────────

fn bench_handle_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("handle_read");
    let no_inquiry: HashMap<u64, InquiryStatus> = HashMap::new();

    for n in [1u64, 10, 100, 1_000] {
        group.bench_with_input(BenchmarkId::new("versions", n), &n, |b, &n| {
            let mut state = state_with_n_versions(0, n);
            // start_ts above all committed versions → binary search touches all N entries.
            let start_ts = n * 1_000 + 100;
            b.iter(|| state.handle_read(99_999, start_ts, 0, &no_inquiry));
        });
    }
    group.finish();
}

// ── B2: handle_update — O(P) PreparedConflict scan, no-conflict path ─────────

fn bench_handle_update_prepared_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("prepared_conflict_scan");

    for n_prepared in [0u64, 10, 100, 500] {
        group.bench_with_input(
            BenchmarkId::new("prepared_txs", n_prepared),
            &n_prepared,
            |b, &n_prepared| {
                // Rebuild state for each sample so we don't accumulate unbounded writes.
                b.iter_batched(
                    || state_with_prepared_txs_on_other_keys(n_prepared),
                    |mut state| {
                        // Key 0 is never prepared → scan traverses all P entries, finds no conflict.
                        state.handle_update(99_999, 0, 0, Value(42))
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

// ── B3: handle_commit — install W writes ─────────────────────────────────────

fn bench_handle_commit(c: &mut Criterion) {
    let mut group = c.benchmark_group("handle_commit");

    for n_writes in [1usize, 10, 100] {
        group.bench_with_input(
            BenchmarkId::new("writes", n_writes),
            &n_writes,
            |b, &n_writes| {
                b.iter_batched(
                    || {
                        let mut state = ShardState::new();
                        for key in 0..n_writes as u64 {
                            state.handle_update(1, 0, key, Value(key * 7));
                        }
                        state.handle_prepare(1);
                        state
                    },
                    |mut state| state.handle_commit(1, 5_000),
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

// ── B4: compact_versions — watermark-gated pruning over K keys ───────────────

fn bench_compact_versions(c: &mut Criterion) {
    let mut group = c.benchmark_group("compact_versions");

    for n_keys in [10u64, 100] {
        group.bench_with_input(
            BenchmarkId::new("keys", n_keys),
            &n_keys,
            |b, &n_keys| {
                b.iter_batched(
                    || {
                        let mut state = ShardState::new();
                        for v in 1u64..=5 {
                            for key in 0..n_keys {
                                let tx_id = (v - 1) * n_keys + key + 1;
                                let start_ts = v * 1_000 - 500;
                                let commit_ts = v * 1_000 + key;
                                state.handle_update(tx_id, start_ts, key, Value(v * 1_000 + key));
                                state.handle_prepare(tx_id);
                                state.handle_commit(tx_id, commit_ts);
                            }
                        }
                        // Sentinel active tx establishes a non-trivial watermark.
                        let sentinel_seq = 5 * n_keys + 1;
                        state.handle_update(sentinel_seq, 10_000, 99_999, Value(0));
                        state
                    },
                    |mut state| state.compact_versions(),
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_handle_read,
    bench_handle_update_prepared_scan,
    bench_handle_commit,
    bench_compact_versions,
);
criterion_main!(benches);
