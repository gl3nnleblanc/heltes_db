/// Tests derived from the Criterion micro-benchmark execution traces documented
/// in spec/HeltesDB.tla (see "Criterion micro-benchmark hot paths" comment).
///
/// Each test encodes the correctness invariant that the corresponding benchmark
/// exercises for performance.  Passing here means the benchmarks are measuring
/// real work, not degenerate cases that short-circuit.
use std::collections::HashMap;

use heltes_db::shard::{InquiryStatus, ReadResult, ShardState, UpdateResult, Value};

// ── Shared setup helpers (mirror the bench setup functions) ───────────────────

/// Install `n_versions` committed versions of `key` using the prepare/commit
/// path with explicit timestamps spaced 1 000 apart.
///
/// Version v → commit_ts = v * 1_000, value = Value(v * 1_000).
/// start_ts = v * 1_000 - 500 lies strictly between (v-1)*1_000 and v*1_000,
/// so no CommittedConflict fires between successive rounds.
fn state_with_n_versions(key: u64, n_versions: u64) -> ShardState {
    let mut state = ShardState::new();
    for v in 1..=n_versions {
        let tx_id = v * 10_000;
        let start_ts = v * 1_000 - 500; // 500, 1500, 2500, …
        let commit_ts = v * 1_000; // 1000, 2000, 3000, …
        state.handle_update(tx_id, start_ts, key, Value(v * 1_000));
        state.handle_prepare(tx_id);
        state.handle_commit(tx_id, commit_ts);
    }
    state
}

/// Build a state with `n_prepared` transactions, each prepared on a distinct
/// key in the range [100, 100+n_prepared).  Key 0 is untouched.
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

// ── T1: handle_read binary-search correctness ─────────────────────────────────

/// Trace T1a — read at start_ts after all N versions → returns latest value.
/// Verifies the O(log N) partition_point terminates at the correct position.
#[test]
fn handle_read_returns_latest_version_before_start_ts() {
    for n in [1u64, 10, 100, 1_000] {
        let mut state = state_with_n_versions(0, n);
        let no_inquiry: HashMap<u64, InquiryStatus> = HashMap::new();

        // start_ts = n*1000 + 100 is strictly above the last commit_ts = n*1000.
        let result = state.handle_read(99_999, n * 1_000 + 100, 0, &no_inquiry);
        assert_eq!(
            result,
            ReadResult::Value(Value(n * 1_000)),
            "n={n}: expected Value({})",
            n * 1_000
        );
    }
}

/// Trace T1b — read at start_ts = 1 (before first version at ts=1000) → NotFound.
/// partition_point returns 0 and the pos>0 guard short-circuits to None.
#[test]
fn handle_read_returns_not_found_when_no_version_before_start_ts() {
    let mut state = state_with_n_versions(0, 5); // versions at ts 1000…5000
    let no_inquiry: HashMap<u64, InquiryStatus> = HashMap::new();

    // start_ts=1: all versions have ts ≥ 1000 > 1, so partition_point = 0 → NotFound.
    let result = state.handle_read(99_999, 1, 0, &no_inquiry);
    assert_eq!(result, ReadResult::NotFound);
}

/// Trace T1c — read at an intermediate start_ts returns the correct version.
/// With 10 versions at ts 1000…10000, start_ts=5100 should return Value(5000)
/// (the version at ts=5000, the latest one strictly before 5100).
#[test]
fn handle_read_returns_correct_version_at_intermediate_start_ts() {
    let mut state = state_with_n_versions(0, 10); // versions at ts 1000,2000,…,10000
    let no_inquiry: HashMap<u64, InquiryStatus> = HashMap::new();

    // start_ts=5100: versions at 1000–5000 are < 5100; 6000 is not.
    // partition_point returns 5; versions[4] = ts=5000, Value(5000).
    let result = state.handle_read(99_999, 5_100, 0, &no_inquiry);
    assert_eq!(result, ReadResult::Value(Value(5_000)));
}

// ── T2: handle_update PreparedConflict scan — no-conflict path ────────────────

/// Trace T2a — P prepared txs on distinct keys; update key=0 succeeds.
/// Verifies that the O(P) scan terminates without a false positive.
#[test]
fn handle_update_succeeds_with_many_prepared_txs_on_different_keys() {
    for n_prepared in [0u64, 10, 100, 500] {
        let mut state = state_with_prepared_txs_on_other_keys(n_prepared);
        let result = state.handle_update(99_999, 0, 0, Value(42));
        assert_eq!(
            result,
            UpdateResult::Ok,
            "n_prepared={n_prepared}: expected Ok"
        );
    }
}

// ── T3: handle_update PreparedConflict scan — conflict path ───────────────────

/// Trace T3 — prepared tx holds key=0 with prep_t >= start_ts of new writer →
/// new writer is aborted (PreparedConflict).
#[test]
fn handle_update_aborts_on_prepared_conflict() {
    let mut state = ShardState::new();
    // Tx 1 writes key=0 at start_ts=0, then prepares (prep_t = clock+1 = 1).
    state.handle_update(1, 0, 0, Value(100));
    state.handle_prepare(1);
    // Tx 2 tries to write key=0 with start_ts=0.  Tx1's prep_t=1 >= start_ts=0 →
    // PreparedConflict → Abort.
    let result = state.handle_update(2, 0, 0, Value(200));
    assert_eq!(result, UpdateResult::Abort);
}

// ── T4: handle_commit installs N writes ───────────────────────────────────────

/// Trace T4 — buffer W writes, prepare, commit at ts=5000; verify all versions
/// installed at the correct timestamp with correct values.
#[test]
fn handle_commit_installs_all_buffered_writes() {
    use heltes_db::shard::CommitResult;
    for n_writes in [1usize, 10, 100] {
        let mut state = ShardState::new();
        let tx_id = 1u64;
        let commit_ts = 5_000u64;

        for key in 0..n_writes as u64 {
            let r = state.handle_update(tx_id, 0, key, Value(key * 7));
            assert_eq!(r, UpdateResult::Ok, "n_writes={n_writes} key={key}");
        }
        state.handle_prepare(tx_id);

        let cr = state.handle_commit(tx_id, commit_ts);
        assert_eq!(cr, CommitResult::Ok, "n_writes={n_writes}");

        for key in 0..n_writes as u64 {
            let versions = state.versions.get(&key).expect("key must have versions");
            assert_eq!(
                versions.len(),
                1,
                "n_writes={n_writes} key={key}: expected 1 version"
            );
            assert_eq!(versions[0].timestamp, commit_ts);
            assert_eq!(versions[0].value, Value(key * 7));
        }
    }
}

// ── T5: compact_versions prunes old MVCC history ─────────────────────────────

/// Trace T5a — N keys × 5 versions each, sentinel active tx at high start_ts;
/// compact_versions prunes versions 1–4 per key, leaving only the latest.
#[test]
fn compact_versions_prunes_old_versions_leaving_latest() {
    for n_keys in [10u64, 100] {
        let mut state = ShardState::new();

        // Commit 5 versions per key.
        // Round v: start_ts = v*1000-500, commit_ts = v*1000+key.
        // This ensures no CommittedConflict between rounds (start_ts for round v
        // is strictly above max commit_ts of round v-1 = (v-1)*1000+n_keys-1).
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

        // Confirm 5 versions per key before compaction.
        for key in 0..n_keys {
            let len = state.versions.get(&key).map_or(0, |v| v.len());
            assert_eq!(
                len, 5,
                "before compact: key={key} expected 5 versions, got {len}"
            );
        }

        // Record the max (latest) timestamp per key — this is what should survive.
        let max_ts: Vec<u64> = (0..n_keys)
            .map(|key| {
                state
                    .versions
                    .get(&key)
                    .map(|vs| vs.iter().map(|v| v.timestamp).max().unwrap())
                    .unwrap_or(0)
            })
            .collect();

        // Sentinel active tx at start_ts=10_000 >> max commit_ts (5000+n_keys-1).
        // Sets write_start_ts so compact_versions computes a non-trivial watermark.
        let sentinel_seq = 5 * n_keys + 1;
        state.handle_update(sentinel_seq, 10_000, 99_999, Value(0));

        assert!(
            !state.dirty_keys.is_empty(),
            "dirty_keys must be non-empty before compaction"
        );

        state.compact_versions();

        // After compaction each key has exactly 1 version — the latest one.
        for key in 0..n_keys {
            let versions = state.versions.get(&key).expect("key must still exist");
            assert_eq!(
                versions.len(),
                1,
                "after compact: key={key} expected 1 version, got {}",
                versions.len()
            );
            assert_eq!(
                versions[0].timestamp, max_ts[key as usize],
                "surviving version should be the latest"
            );
        }
    }
}

/// Trace T5b — compact_versions is a no-op when there are no active transactions.
/// Verifies the early-return branch (no watermark) leaves versions untouched.
#[test]
fn compact_versions_no_op_without_active_transactions() {
    let mut state = state_with_n_versions(0, 5);
    // No write_start_ts or read_start_ts entries after all commits → watermark None.
    let count_before = state.versions.get(&0).map_or(0, |v| v.len());
    state.compact_versions();
    let count_after = state.versions.get(&0).map_or(0, |v| v.len());
    assert_eq!(
        count_before, count_after,
        "no-op: version count should not change"
    );
}
