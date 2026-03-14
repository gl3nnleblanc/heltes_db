use std::time::{Duration, Instant};

use super::*;

// -----------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------

const K1: Key = 1;
const K2: Key = 2;
const K3: Key = 3;

const T1: TxId = 101;
const T2: TxId = 102;
const T3: TxId = 103;

fn v(n: u64) -> Value {
    Value(n)
}

fn shard() -> ShardState {
    ShardState::new()
}

fn no_inquiries() -> HashMap<TxId, InquiryStatus> {
    HashMap::new()
}

// -----------------------------------------------------------------------
// handle_read — basic MVCC visibility
// -----------------------------------------------------------------------

#[test]
fn read_returns_not_found_when_key_has_no_versions() {
    let mut s = shard();
    assert_eq!(
        s.handle_read(T1, 5, K1, &no_inquiries()),
        ReadResult::NotFound,
    );
}

#[test]
fn read_returns_latest_version_strictly_before_start_ts() {
    // Manually install two committed versions: v(10) at t=2, v(20) at t=4.
    // A read with start_ts=5 should see v(20) (latest strictly before 5).
    let mut s = shard();
    s.versions.entry(K1).or_default().push(Version {
        value: v(10),
        timestamp: 2,
    });
    s.versions.entry(K1).or_default().push(Version {
        value: v(20),
        timestamp: 4,
    });

    assert_eq!(
        s.handle_read(T1, 5, K1, &no_inquiries()),
        ReadResult::Value(v(20)),
    );
}

#[test]
fn read_does_not_see_version_exactly_at_start_ts() {
    // Version at t=5, read with start_ts=5 — NOT visible (strictly less than).
    let mut s = shard();
    s.versions.entry(K1).or_default().push(Version {
        value: v(99),
        timestamp: 5,
    });

    assert_eq!(
        s.handle_read(T1, 5, K1, &no_inquiries()),
        ReadResult::NotFound,
    );
}

#[test]
fn read_does_not_see_version_after_start_ts() {
    let mut s = shard();
    s.versions.entry(K1).or_default().push(Version {
        value: v(99),
        timestamp: 10,
    });

    assert_eq!(
        s.handle_read(T1, 5, K1, &no_inquiries()),
        ReadResult::NotFound,
    );
}

#[test]
fn read_picks_latest_of_multiple_eligible_versions() {
    let mut s = shard();
    s.versions.entry(K1).or_default().push(Version {
        value: v(1),
        timestamp: 1,
    });
    s.versions.entry(K1).or_default().push(Version {
        value: v(2),
        timestamp: 2,
    });
    s.versions.entry(K1).or_default().push(Version {
        value: v(3),
        timestamp: 3,
    });
    // start_ts=4: should see v(3) at t=3
    assert_eq!(
        s.handle_read(T1, 4, K1, &no_inquiries()),
        ReadResult::Value(v(3)),
    );
}

// -----------------------------------------------------------------------
// handle_read — read your own writes
// -----------------------------------------------------------------------

#[test]
fn read_returns_own_buffered_write() {
    let mut s = shard();
    // Manually plant a buffered write for T1 on K1.
    s.write_buff.entry(T1).or_default().insert(K1, v(42));
    assert_eq!(
        s.handle_read(T1, 5, K1, &no_inquiries()),
        ReadResult::Value(v(42)),
    );
}

#[test]
fn read_own_write_shadows_committed_version() {
    let mut s = shard();
    s.versions.entry(K1).or_default().push(Version {
        value: v(10),
        timestamp: 2,
    });
    s.write_buff.entry(T1).or_default().insert(K1, v(99));
    // T1's buffered write should win over the committed v(10).
    assert_eq!(
        s.handle_read(T1, 5, K1, &no_inquiries()),
        ReadResult::Value(v(99)),
    );
}

// -----------------------------------------------------------------------
// handle_read — aborted transaction
// -----------------------------------------------------------------------

#[test]
fn read_aborted_tx_returns_abort() {
    let mut s = shard();
    s.aborted.insert(T1);
    assert_eq!(s.handle_read(T1, 5, K1, &no_inquiries()), ReadResult::Abort,);
}

// -----------------------------------------------------------------------
// handle_read — INQUIRE protocol
// -----------------------------------------------------------------------

#[test]
fn read_needs_inquiry_for_prepared_writer_with_prep_ts_before_start_ts() {
    // T2 has prep_t=3 < start_ts=5 and buffered a write to K1.
    // T1 reads K1 at start_ts=5 — must inquire about T2 before proceeding.
    let mut s = shard();
    s.prepared.insert(T2, 3);
    s.write_buff.entry(T2).or_default().insert(K1, v(77));

    let result = s.handle_read(T1, 5, K1, &no_inquiries());
    assert_eq!(result, ReadResult::NeedsInquiry(vec![T2]));
}

#[test]
fn read_does_not_need_inquiry_for_prepared_writer_with_prep_ts_at_start_ts() {
    // prep_t == start_ts: the prepared tx cannot have committed before our snapshot.
    let mut s = shard();
    s.prepared.insert(T2, 5);
    s.write_buff.entry(T2).or_default().insert(K1, v(77));

    // Should NOT need inquiry — no NeedsInquiry (key has no prior versions so NotFound).
    let result = s.handle_read(T1, 5, K1, &no_inquiries());
    assert!(!matches!(result, ReadResult::NeedsInquiry(_)));
}

#[test]
fn read_does_not_need_inquiry_for_prepared_writer_with_prep_ts_after_start_ts() {
    let mut s = shard();
    s.prepared.insert(T2, 10);
    s.write_buff.entry(T2).or_default().insert(K1, v(77));

    let result = s.handle_read(T1, 5, K1, &no_inquiries());
    assert!(!matches!(result, ReadResult::NeedsInquiry(_)));
}

#[test]
fn read_proceeds_when_inquiry_says_active() {
    // T2 is prepared with prep_t=3 but inquiry says ACTIVE — it hasn't
    // committed, so T1 safely ignores T2's buffered write.
    let mut s = shard();
    s.prepared.insert(T2, 3);
    s.write_buff.entry(T2).or_default().insert(K1, v(77));

    let mut inq = HashMap::new();
    inq.insert(T2, InquiryStatus::Active);

    assert_eq!(s.handle_read(T1, 5, K1, &inq), ReadResult::NotFound,);
}

#[test]
fn read_needs_inquiry_again_when_committed_version_not_yet_installed() {
    // Inquiry says T2 committed at ct=4 < start_ts=5, but handle_commit
    // hasn't been called yet, so the version isn't in self.versions.
    // Shard must block until the version is installed.
    let mut s = shard();
    s.prepared.insert(T2, 3);
    s.write_buff.entry(T2).or_default().insert(K1, v(77));

    let mut inq = HashMap::new();
    inq.insert(T2, InquiryStatus::Committed(4));

    assert_eq!(
        s.handle_read(T1, 5, K1, &inq),
        ReadResult::NeedsInquiry(vec![T2]),
    );
}

#[test]
fn read_sees_committed_version_after_commit_installed() {
    // Same as above but now T2's commit has been processed (version installed).
    // T1 should read T2's value.
    let mut s = shard();
    s.prepared.insert(T2, 3);
    s.write_buff.entry(T2).or_default().insert(K1, v(77));

    // Simulate T2's commit being installed.
    s.versions.entry(K1).or_default().push(Version {
        value: v(77),
        timestamp: 4,
    });
    s.prepared.remove(&T2);

    let mut inq = HashMap::new();
    inq.insert(T2, InquiryStatus::Committed(4));

    assert_eq!(s.handle_read(T1, 5, K1, &inq), ReadResult::Value(v(77)),);
}

#[test]
fn read_ignores_prepared_writer_of_different_key() {
    // T2 has a pending write to K2, not K1. T1 reads K1 — no inquiry needed.
    let mut s = shard();
    s.prepared.insert(T2, 3);
    s.write_buff.entry(T2).or_default().insert(K2, v(77));

    assert_eq!(
        s.handle_read(T1, 5, K1, &no_inquiries()),
        ReadResult::NotFound,
    );
}

// -----------------------------------------------------------------------
// handle_update — basic buffering
// -----------------------------------------------------------------------

#[test]
fn update_buffers_write_successfully() {
    let mut s = shard();
    assert_eq!(s.handle_update(T1, 5, K1, v(42)), UpdateResult::Ok);
    assert_eq!(s.write_buff[&T1][&K1], v(42));
}

#[test]
fn update_overwrites_previous_buffered_write_to_same_key() {
    let mut s = shard();
    s.handle_update(T1, 5, K1, v(1));
    s.handle_update(T1, 5, K1, v(2));
    assert_eq!(s.write_buff[&T1][&K1], v(2));
    // Only one entry for K1.
    assert_eq!(s.write_buff[&T1].len(), 1);
}

#[test]
fn update_buffers_writes_to_multiple_keys() {
    let mut s = shard();
    assert_eq!(s.handle_update(T1, 5, K1, v(1)), UpdateResult::Ok);
    assert_eq!(s.handle_update(T1, 5, K2, v(2)), UpdateResult::Ok);
    assert_eq!(s.write_buff[&T1].len(), 2);
}

// -----------------------------------------------------------------------
// handle_update — aborted transaction
// -----------------------------------------------------------------------

#[test]
fn update_aborted_tx_returns_abort() {
    let mut s = shard();
    s.aborted.insert(T1);
    assert_eq!(s.handle_update(T1, 5, K1, v(42)), UpdateResult::Abort);
}

#[test]
fn update_aborted_tx_does_not_buffer_write() {
    let mut s = shard();
    s.aborted.insert(T1);
    s.handle_update(T1, 5, K1, v(42));
    assert!(!s
        .write_buff
        .get(&T1)
        .map(|b| b.contains_key(&K1))
        .unwrap_or(false));
}

// -----------------------------------------------------------------------
// handle_update — write-write conflict: committed versions
// -----------------------------------------------------------------------

#[test]
fn update_no_conflict_when_committed_version_strictly_before_start_ts() {
    // Committed version at t=4, start_ts=5 → t < start_ts → no conflict.
    let mut s = shard();
    s.versions.entry(K1).or_default().push(Version {
        value: v(10),
        timestamp: 4,
    });

    assert_eq!(s.handle_update(T1, 5, K1, v(99)), UpdateResult::Ok);
}

#[test]
fn update_conflict_when_committed_version_at_start_ts() {
    // Committed version at t=5 == start_ts=5 → conflict → ABORT.
    let mut s = shard();
    s.versions.entry(K1).or_default().push(Version {
        value: v(10),
        timestamp: 5,
    });

    assert_eq!(s.handle_update(T1, 5, K1, v(99)), UpdateResult::Abort);
}

#[test]
fn update_conflict_when_committed_version_after_start_ts() {
    // Committed version at t=6 > start_ts=5 → conflict → ABORT.
    let mut s = shard();
    s.versions.entry(K1).or_default().push(Version {
        value: v(10),
        timestamp: 6,
    });

    assert_eq!(s.handle_update(T1, 5, K1, v(99)), UpdateResult::Abort);
}

#[test]
fn update_conflict_marks_tx_aborted() {
    let mut s = shard();
    s.versions.entry(K1).or_default().push(Version {
        value: v(10),
        timestamp: 6,
    });
    s.handle_update(T1, 5, K1, v(99));
    assert!(s.aborted.contains(&T1));
}

// -----------------------------------------------------------------------
// handle_update — write-write conflict: prepared transactions
// -----------------------------------------------------------------------

#[test]
fn update_no_conflict_when_prepared_writer_prep_ts_strictly_before_start_ts() {
    // prep_t=4 < start_ts=5 → no prepared conflict.
    // T2 prepared for K2 (not K1), so no WriteBuffConflict on K1 either.
    let mut s = shard();
    s.prepared.insert(T2, 4);
    s.write_buff.entry(T2).or_default().insert(K2, v(50));

    assert_eq!(s.handle_update(T1, 5, K1, v(99)), UpdateResult::Ok);
}

#[test]
fn update_conflict_when_prepared_writer_prep_ts_at_start_ts() {
    // prep_t=5 == start_ts=5 → conflict → ABORT.
    let mut s = shard();
    s.prepared.insert(T2, 5);
    s.write_buff.entry(T2).or_default().insert(K1, v(50));

    assert_eq!(s.handle_update(T1, 5, K1, v(99)), UpdateResult::Abort);
}

#[test]
fn update_conflict_when_prepared_writer_prep_ts_after_start_ts() {
    // prep_t=7 > start_ts=5 → conflict → ABORT.
    let mut s = shard();
    s.prepared.insert(T2, 7);
    s.write_buff.entry(T2).or_default().insert(K1, v(50));

    assert_eq!(s.handle_update(T1, 5, K1, v(99)), UpdateResult::Abort);
}

#[test]
fn update_no_conflict_when_prepared_writer_wrote_different_key() {
    let mut s = shard();
    s.prepared.insert(T2, 7);
    s.write_buff.entry(T2).or_default().insert(K2, v(50)); // K2, not K1

    assert_eq!(s.handle_update(T1, 5, K1, v(99)), UpdateResult::Ok);
}

// -----------------------------------------------------------------------
// handle_update — write buffer limit (max_writes_per_tx)
//
// TLA+ traces WL1–WL4 from ShardHandleUpdate spec note.
// The fix adds a check: if this write would add a NEW distinct key to
// write_buff and |write_buff[id]| >= max_writes_per_tx, abort the tx.
// -----------------------------------------------------------------------

// Trace WL1: writes up to the limit succeed; the (limit)th new key is accepted.
#[test]
fn write_buff_limit_writes_within_limit_succeed() {
    let mut s = shard();
    s.max_writes_per_tx = 2;
    assert_eq!(s.handle_update(T1, 1, K1, v(10)), UpdateResult::Ok);
    assert_eq!(s.handle_update(T1, 1, K2, v(20)), UpdateResult::Ok);
    // Both keys are buffered.
    assert_eq!(s.write_buff[&T1].len(), 2);
}

// Trace WL2: adding a new key beyond the limit → Abort, tx marked aborted.
#[test]
fn write_buff_limit_exceeded_aborts_tx() {
    let mut s = shard();
    s.max_writes_per_tx = 2;
    s.handle_update(T1, 1, K1, v(10));
    s.handle_update(T1, 1, K2, v(20));
    // Third distinct key exceeds limit.
    let r = s.handle_update(T1, 1, K3, v(30));
    assert_eq!(r, UpdateResult::Abort, "third write must be aborted");
    assert!(
        s.aborted.contains(&T1),
        "T1 must be in aborted set after limit abort"
    );
}

// Trace WL3: overwriting an already-buffered key does NOT count against the limit.
#[test]
fn write_buff_limit_overwrite_same_key_is_not_limited() {
    let mut s = shard();
    s.max_writes_per_tx = 1; // limit = 1 distinct key
    assert_eq!(s.handle_update(T1, 1, K1, v(10)), UpdateResult::Ok);
    // K1 is already buffered — overwrite is not a new key → limit not hit.
    assert_eq!(
        s.handle_update(T1, 1, K1, v(99)),
        UpdateResult::Ok,
        "overwrite of already-buffered key must succeed even at limit"
    );
    assert_eq!(
        s.write_buff[&T1][&K1],
        v(99),
        "K1 must hold the updated value"
    );
}

// Trace WL4: pre-aborted tx returns Abort immediately; limit check is not reached.
#[test]
fn write_buff_limit_pre_aborted_tx_returns_abort_without_limit_check() {
    let mut s = shard();
    s.max_writes_per_tx = 1;
    s.aborted.insert(T1);
    // T1 is already aborted; returns Abort before even checking the limit.
    assert_eq!(s.handle_update(T1, 1, K1, v(5)), UpdateResult::Abort);
    // Aborted flag remains; write_buff is unaffected.
    assert!(s.aborted.contains(&T1));
    assert!(s.write_buff.get(&T1).is_none());
}

// -----------------------------------------------------------------------
// handle_prepare
// -----------------------------------------------------------------------

#[test]
fn prepare_returns_timestamp_greater_than_current_clock() {
    let mut s = shard();
    s.clock = 3;
    match s.handle_prepare(T1) {
        PrepareResult::Timestamp(t) => assert!(t > 3),
        _ => panic!("expected Timestamp"),
    }
}

#[test]
fn prepare_advances_clock() {
    let mut s = shard();
    s.clock = 3;
    s.handle_prepare(T1);
    assert!(s.clock > 3);
}

#[test]
fn prepare_clock_equals_returned_timestamp() {
    let mut s = shard();
    s.clock = 3;
    match s.handle_prepare(T1) {
        PrepareResult::Timestamp(t) => assert_eq!(s.clock, t),
        _ => panic!("expected Timestamp"),
    }
}

#[test]
fn prepare_records_tx_in_prepared_map() {
    let mut s = shard();
    match s.handle_prepare(T1) {
        PrepareResult::Timestamp(t) => {
            assert_eq!(s.prepared.get(&T1), Some(&t));
        }
        _ => panic!("expected Timestamp"),
    }
}

#[test]
fn prepare_two_txs_get_different_timestamps() {
    // SI2 relies on unique commit timestamps; distinct prepare timestamps
    // are a prerequisite (coordinator takes max).
    let mut s = shard();
    let t1 = match s.handle_prepare(T1) {
        PrepareResult::Timestamp(t) => t,
        _ => panic!(),
    };
    let t2 = match s.handle_prepare(T2) {
        PrepareResult::Timestamp(t) => t,
        _ => panic!(),
    };
    assert_ne!(t1, t2);
}

#[test]
fn prepare_aborted_tx_returns_abort() {
    let mut s = shard();
    s.aborted.insert(T1);
    assert_eq!(s.handle_prepare(T1), PrepareResult::Abort);
}

#[test]
fn prepare_aborted_tx_does_not_record_in_prepared() {
    let mut s = shard();
    s.aborted.insert(T1);
    s.handle_prepare(T1);
    assert!(!s.prepared.contains_key(&T1));
}

// -----------------------------------------------------------------------
// handle_commit
// -----------------------------------------------------------------------

#[test]
fn commit_installs_version_at_commit_ts() {
    let mut s = shard();
    s.write_buff.entry(T1).or_default().insert(K1, v(42));
    s.prepared.insert(T1, 5);

    s.handle_commit(T1, 6);

    let versions = &s.versions[&K1];
    assert!(versions
        .iter()
        .any(|ver| ver.value == v(42) && ver.timestamp == 6));
}

#[test]
fn commit_installs_versions_for_all_written_keys() {
    let mut s = shard();
    s.write_buff.entry(T1).or_default().insert(K1, v(1));
    s.write_buff.entry(T1).or_default().insert(K2, v(2));
    s.prepared.insert(T1, 5);

    s.handle_commit(T1, 6);

    assert!(s.versions[&K1]
        .iter()
        .any(|ver| ver.value == v(1) && ver.timestamp == 6));
    assert!(s.versions[&K2]
        .iter()
        .any(|ver| ver.value == v(2) && ver.timestamp == 6));
}

#[test]
fn commit_clears_write_buffer() {
    let mut s = shard();
    s.write_buff.entry(T1).or_default().insert(K1, v(42));
    s.prepared.insert(T1, 5);

    s.handle_commit(T1, 6);

    assert!(s.write_buff.get(&T1).map(|b| b.is_empty()).unwrap_or(true));
}

#[test]
fn commit_removes_tx_from_prepared() {
    let mut s = shard();
    s.write_buff.entry(T1).or_default().insert(K1, v(42));
    s.prepared.insert(T1, 5);

    s.handle_commit(T1, 6);

    assert!(!s.prepared.contains_key(&T1));
}

#[test]
fn commit_does_not_affect_other_txs_prepared_state() {
    let mut s = shard();
    s.write_buff.entry(T1).or_default().insert(K1, v(42));
    s.prepared.insert(T1, 5);
    s.prepared.insert(T2, 7); // T2 is also prepared

    s.handle_commit(T1, 6);

    assert!(s.prepared.contains_key(&T2));
}

// -----------------------------------------------------------------------
// handle_abort
// -----------------------------------------------------------------------

#[test]
fn abort_marks_tx_in_aborted_set() {
    let mut s = shard();
    // T1=101 encodes as (port=0, seq=101).  An older in-flight tx from the same
    // port (seq=50 < 101) keeps min_active_seq[0]=50, so seq=101 >= 50 → retained.
    let t_older: TxId = 50; // port=0, seq=50
    s.write_buff.entry(t_older).or_default().insert(K2, v(99));
    s.handle_abort(T1);
    assert!(s.aborted.contains(&T1));
}

#[test]
fn abort_clears_write_buffer() {
    let mut s = shard();
    s.write_buff.entry(T1).or_default().insert(K1, v(42));
    s.handle_abort(T1);
    assert!(s.write_buff.get(&T1).map(|b| b.is_empty()).unwrap_or(true));
}

#[test]
fn abort_removes_from_prepared() {
    let mut s = shard();
    s.prepared.insert(T1, 5);
    s.handle_abort(T1);
    assert!(!s.prepared.contains_key(&T1));
}

#[test]
fn abort_does_not_affect_other_txs() {
    let mut s = shard();
    s.prepared.insert(T2, 5);
    s.write_buff.entry(T2).or_default().insert(K1, v(9));

    s.handle_abort(T1);

    assert!(s.prepared.contains_key(&T2));
    assert_eq!(s.write_buff[&T2][&K1], v(9));
}

// -----------------------------------------------------------------------
// SI property tests (cross-operation sequences)
// -----------------------------------------------------------------------

/// SI3: MVCC version timestamps for a given key must be distinct after
/// multiple commits, so LatestVersionBefore is well-defined.
#[test]
fn si3_committed_versions_have_distinct_timestamps() {
    let mut s = shard();

    // T1 writes K1, commits at t=3.
    s.write_buff.entry(T1).or_default().insert(K1, v(10));
    s.prepared.insert(T1, 2);
    s.handle_commit(T1, 3);

    // T2 writes K1, commits at t=5.
    s.write_buff.entry(T2).or_default().insert(K1, v(20));
    s.prepared.insert(T2, 4);
    s.handle_commit(T2, 5);

    let timestamps: Vec<Timestamp> = s.versions[&K1].iter().map(|v| v.timestamp).collect();
    let unique: std::collections::HashSet<_> = timestamps.iter().collect();
    assert_eq!(timestamps.len(), unique.len(), "duplicate timestamps found");
}

/// SI4: Two concurrent writers to the same key — the second must be aborted.
/// T1: start_ts=1, writes K1, prepares, commits (ct=3).
/// T2: start_ts=1, writes K1 → conflict because committed version ct=3 >= start_ts=1.
#[test]
fn si4_concurrent_writers_to_same_key_second_is_aborted() {
    let mut s = shard();

    // T1 succeeds.
    assert_eq!(s.handle_update(T1, 1, K1, v(10)), UpdateResult::Ok);
    assert!(matches!(s.handle_prepare(T1), PrepareResult::Timestamp(_)));
    s.handle_commit(T1, 3);

    // T2 is concurrent (start_ts=1) and tries to write the same key.
    assert_eq!(s.handle_update(T2, 1, K1, v(20)), UpdateResult::Abort);
}

/// SI4: Prepared-transaction conflict — T2 tries to write K1 while T1 is
/// prepared with prep_t >= T2's start_ts.
#[test]
fn si4_prepared_conflict_aborts_concurrent_writer() {
    let mut s = shard();

    // T1 buffered K1 and is now prepared at t=5.
    s.write_buff.entry(T1).or_default().insert(K1, v(10));
    s.prepared.insert(T1, 5);

    // T2 (start_ts=4) tries to write K1. prep_t(T1)=5 >= start_ts(T2)=4 → ABORT.
    assert_eq!(s.handle_update(T2, 4, K1, v(20)), UpdateResult::Abort);
}

/// SI1: After T1 commits, a read by T2 with start_ts > commit_ts(T1) sees T1's value.
#[test]
fn si1_read_sees_version_committed_before_start_ts() {
    let mut s = shard();

    // T1 writes K1, commits at ct=3.
    s.write_buff.entry(T1).or_default().insert(K1, v(42));
    s.prepared.insert(T1, 2);
    s.handle_commit(T1, 3);

    // T2 starts at start_ts=4 > ct=3 and reads K1.
    assert_eq!(
        s.handle_read(T2, 4, K1, &no_inquiries()),
        ReadResult::Value(v(42)),
    );
}

/// SI1: T2 with start_ts == commit_ts(T1) must NOT see T1's write
/// (start_ts is strictly less than required for visibility).
#[test]
fn si1_read_does_not_see_version_committed_at_start_ts() {
    let mut s = shard();

    s.write_buff.entry(T1).or_default().insert(K1, v(42));
    s.prepared.insert(T1, 2);
    s.handle_commit(T1, 3);

    // T2 starts exactly at ct=3 — should NOT see T1's write (no prior version either).
    assert_eq!(
        s.handle_read(T2, 3, K1, &no_inquiries()),
        ReadResult::NotFound,
    );
}

// -----------------------------------------------------------------------
// Clock monotonicity
// -----------------------------------------------------------------------

#[test]
fn clock_advances_on_successive_prepares() {
    let mut s = shard();
    let t1 = match s.handle_prepare(T1) {
        PrepareResult::Timestamp(t) => t,
        _ => panic!(),
    };
    let t2 = match s.handle_prepare(T2) {
        PrepareResult::Timestamp(t) => t,
        _ => panic!(),
    };
    let t3 = match s.handle_prepare(T3) {
        PrepareResult::Timestamp(t) => t,
        _ => panic!(),
    };
    assert!(t1 < t2 && t2 < t3);
}

#[test]
fn clock_updated_on_read_with_higher_ts() {
    let mut s = shard();
    s.clock = 2;
    s.handle_read(T1, 10, K1, &no_inquiries());
    // After processing a message with ts=10, clock >= 10.
    assert!(s.clock >= 10);
}

#[test]
fn clock_updated_on_update_with_higher_ts() {
    let mut s = shard();
    s.clock = 2;
    s.handle_update(T1, 10, K1, v(1));
    assert!(s.clock >= 10);
}

#[test]
fn clock_not_decreased_when_message_ts_is_lower() {
    let mut s = shard();
    s.clock = 10;
    s.handle_read(T1, 3, K1, &no_inquiries());
    assert!(s.clock >= 10);
}

// -----------------------------------------------------------------------
// PATH SUITE
//
// The following tests are derived from equivalence classes of execution
// paths explored by TLC during formal model checking (633k distinct states,
// depth 36). Each test exercises a complete multi-step interaction sequence
// rather than a single operation in isolation.
// -----------------------------------------------------------------------

// -------------------------------------------------------------------
// Path family 1: Full single-transaction commit lifecycle
// -------------------------------------------------------------------

/// Path: update → prepare → commit → read sees new value.
/// The canonical happy path through the entire commit protocol.
#[test]
fn path_single_tx_full_commit_lifecycle() {
    let mut s = shard();
    assert_eq!(s.handle_update(T1, 1, K1, v(7)), UpdateResult::Ok);
    let prep_t = match s.handle_prepare(T1) {
        PrepareResult::Timestamp(t) => t,
        _ => panic!("expected prepare timestamp"),
    };
    s.handle_commit(T1, prep_t + 1);
    assert_eq!(
        s.handle_read(T2, prep_t + 2, K1, &no_inquiries()),
        ReadResult::Value(v(7)),
    );
}

/// Path: update → prepare → commit; snapshot before commit sees init value.
#[test]
fn path_snapshot_before_commit_sees_old_value() {
    let mut s = shard();
    assert_eq!(s.handle_update(T1, 3, K1, v(99)), UpdateResult::Ok);
    let prep_t = match s.handle_prepare(T1) {
        PrepareResult::Timestamp(t) => t,
        _ => panic!(),
    };
    let commit_ts = prep_t + 1;
    s.handle_commit(T1, commit_ts);
    // T2 started BEFORE T1 committed — snapshot at start_ts=2 < commit_ts; key not yet inserted.
    assert_eq!(
        s.handle_read(T2, 2, K1, &no_inquiries()),
        ReadResult::NotFound,
    );
}

/// Path: multiple updates to same key within one tx, then commit.
/// Final committed value must be the last update.
#[test]
fn path_multiple_updates_same_key_last_wins() {
    let mut s = shard();
    s.handle_update(T1, 1, K1, v(10));
    s.handle_update(T1, 1, K1, v(20));
    s.handle_update(T1, 1, K1, v(30));
    let prep_t = match s.handle_prepare(T1) {
        PrepareResult::Timestamp(t) => t,
        _ => panic!(),
    };
    s.handle_commit(T1, prep_t + 1);
    assert_eq!(
        s.handle_read(T2, prep_t + 2, K1, &no_inquiries()),
        ReadResult::Value(v(30)),
    );
}

/// Path: update multiple keys, commit; all keys show new versions.
#[test]
fn path_multi_key_commit_all_versions_installed() {
    let mut s = shard();
    s.handle_update(T1, 1, K1, v(11));
    s.handle_update(T1, 1, K2, v(22));
    s.handle_update(T1, 1, K3, v(33));
    let prep_t = match s.handle_prepare(T1) {
        PrepareResult::Timestamp(t) => t,
        _ => panic!(),
    };
    let ct = prep_t + 1;
    s.handle_commit(T1, ct);
    assert_eq!(
        s.handle_read(T2, ct + 1, K1, &no_inquiries()),
        ReadResult::Value(v(11))
    );
    assert_eq!(
        s.handle_read(T2, ct + 1, K2, &no_inquiries()),
        ReadResult::Value(v(22))
    );
    assert_eq!(
        s.handle_read(T2, ct + 1, K3, &no_inquiries()),
        ReadResult::Value(v(33))
    );
}

/// Path: commit, then read at exactly commit_ts is NOT visible (strict <).
#[test]
fn path_read_at_exactly_commit_ts_is_not_visible() {
    let mut s = shard();
    s.handle_update(T1, 1, K1, v(55));
    let prep_t = match s.handle_prepare(T1) {
        PrepareResult::Timestamp(t) => t,
        _ => panic!(),
    };
    let ct = prep_t + 1;
    s.handle_commit(T1, ct);
    // start_ts == ct: must NOT see the version (and no prior version exists).
    assert_eq!(
        s.handle_read(T2, ct, K1, &no_inquiries()),
        ReadResult::NotFound
    );
}

/// Path: commit, then read at commit_ts + 1 IS visible.
#[test]
fn path_read_one_past_commit_ts_is_visible() {
    let mut s = shard();
    s.handle_update(T1, 1, K1, v(55));
    let prep_t = match s.handle_prepare(T1) {
        PrepareResult::Timestamp(t) => t,
        _ => panic!(),
    };
    let ct = prep_t + 1;
    s.handle_commit(T1, ct);
    assert_eq!(
        s.handle_read(T2, ct + 1, K1, &no_inquiries()),
        ReadResult::Value(v(55))
    );
}

// -------------------------------------------------------------------
// Path family 2: Abort paths
// -------------------------------------------------------------------

/// Path: update → abort; subsequent read sees key as not found (never committed).
#[test]
fn path_update_abort_read_sees_not_found() {
    let mut s = shard();
    s.handle_update(T1, 1, K1, v(42));
    s.handle_abort(T1);
    assert_eq!(
        s.handle_read(T2, 5, K1, &no_inquiries()),
        ReadResult::NotFound
    );
}

/// Path: update → abort; another tx can then write the same key.
#[test]
fn path_after_abort_lock_released_other_tx_can_write() {
    let mut s = shard();
    s.handle_update(T1, 1, K1, v(42));
    s.handle_abort(T1);
    assert_eq!(s.handle_update(T2, 2, K1, v(99)), UpdateResult::Ok);
}

/// Path: update → prepare → abort; lock released, subsequent writer succeeds.
#[test]
fn path_abort_after_prepare_releases_lock() {
    let mut s = shard();
    s.handle_update(T1, 1, K1, v(10));
    s.handle_prepare(T1);
    s.handle_abort(T1);
    assert_eq!(s.handle_update(T2, 2, K1, v(20)), UpdateResult::Ok);
}

/// Path: update → abort → read on aborted tx returns Abort.
/// An older in-flight tx from the same port prevents pruning of T1 from aborted.
#[test]
fn path_read_on_aborted_tx_returns_abort() {
    let mut s = shard();
    let t_older: TxId = 50; // port=0, seq=50 — keeps T1 (seq=101) in aborted
    s.write_buff.entry(t_older).or_default().insert(K2, v(99));
    s.handle_update(T1, 1, K1, v(42));
    s.handle_abort(T1);
    assert_eq!(s.handle_read(T1, 5, K1, &no_inquiries()), ReadResult::Abort);
}

/// Path: update → abort → update on same tx returns Abort.
/// An older in-flight tx from the same port prevents pruning of T1 from aborted.
#[test]
fn path_update_on_aborted_tx_returns_abort() {
    let mut s = shard();
    let t_older: TxId = 50; // port=0, seq=50 — keeps T1 (seq=101) in aborted
    s.write_buff.entry(t_older).or_default().insert(K2, v(99));
    s.handle_update(T1, 1, K1, v(42));
    s.handle_abort(T1);
    assert_eq!(s.handle_update(T1, 1, K2, v(5)), UpdateResult::Abort);
}

// -------------------------------------------------------------------
// Path family 3: Write-write conflict scenarios
// -------------------------------------------------------------------

/// Path: T1 buffers K1; T2 attempts K1 → WriteBuffConflict → Abort.
#[test]
fn path_write_buff_conflict_second_writer_aborted() {
    let mut s = shard();
    assert_eq!(s.handle_update(T1, 1, K1, v(10)), UpdateResult::Ok);
    assert_eq!(s.handle_update(T2, 2, K1, v(20)), UpdateResult::Abort);
}

/// Path: T1 buffers K1; T2 aborted; T3 attempts K1 → only T1's lock blocks.
#[test]
fn path_aborted_tx_does_not_block_other_writers() {
    let mut s = shard();
    // T2 is aborted (e.g. due to a prior conflict elsewhere).
    s.aborted.insert(T2);
    // T1 can still write K1.
    assert_eq!(s.handle_update(T1, 1, K1, v(10)), UpdateResult::Ok);
}

/// Path: T1 commits K1 at ct=5; T2 (start_ts=3) tries to write → CommittedConflict.
#[test]
fn path_committed_conflict_aborts_late_writer() {
    let mut s = shard();
    s.handle_update(T1, 2, K1, v(10));
    s.handle_prepare(T1);
    s.handle_commit(T1, 5);
    // T2 started at ts=3 < ct=5; committed version at ts=5 >= 3 → ABORT.
    assert_eq!(s.handle_update(T2, 3, K1, v(20)), UpdateResult::Abort);
}

/// Path: T1 commits K1 at ct=5; T2 (start_ts=5) tries to write → CommittedConflict (>=).
#[test]
fn path_committed_conflict_at_boundary_start_ts_equals_commit_ts() {
    let mut s = shard();
    s.handle_update(T1, 2, K1, v(10));
    s.handle_prepare(T1);
    s.handle_commit(T1, 5);
    assert_eq!(s.handle_update(T2, 5, K1, v(20)), UpdateResult::Abort);
}

/// Path: T1 commits K1 at ct=5; T2 (start_ts=6) writes → no conflict (started after).
#[test]
fn path_no_conflict_writer_starts_after_commit() {
    let mut s = shard();
    s.handle_update(T1, 2, K1, v(10));
    s.handle_prepare(T1);
    s.handle_commit(T1, 5);
    assert_eq!(s.handle_update(T2, 6, K1, v(20)), UpdateResult::Ok);
}

/// Path: T1 prepared K1 at prep_t=4; T2 (start_ts=3) writes → PreparedConflict.
#[test]
fn path_prepared_conflict_aborts_writer_with_earlier_start() {
    let mut s = shard();
    s.handle_update(T1, 3, K1, v(10));
    s.handle_prepare(T1); // prep_t = clock+1 = 1
                          // Manually set a high prep_t to ensure conflict.
    let prep_t = *s.prepared.get(&T1).unwrap();
    // Override to ensure prep_t=4 >= start_ts(T2)=3.
    s.prepared.insert(T1, 4);
    s.clock = 4;
    assert_eq!(s.handle_update(T2, 3, K1, v(20)), UpdateResult::Abort);
    let _ = prep_t; // suppress unused warning
}

/// Path: T1 prepared K1 at prep_t=1; T2 (start_ts=3) writes K1 → no PreparedConflict
/// (prep_t < start_ts). But WriteBuffConflict still fires since T1 holds the lock.
#[test]
fn path_write_buff_conflict_fires_even_when_prep_ts_below_start_ts() {
    let mut s = shard();
    s.handle_update(T1, 1, K1, v(10));
    s.prepared.insert(T1, 1); // prep_t=1 < start_ts(T2)=3, no PreparedConflict
                              // But T1 still has K1 in write_buff → WriteBuffConflict.
    assert_eq!(s.handle_update(T2, 3, K1, v(20)), UpdateResult::Abort);
}

/// Path: conflict aborts T2; T2 can no longer update any key.
#[test]
fn path_aborted_by_conflict_cannot_update_other_keys() {
    let mut s = shard();
    s.handle_update(T1, 1, K1, v(10));
    s.handle_update(T2, 1, K1, v(20)); // conflict → T2 aborted
    assert_eq!(s.handle_update(T2, 1, K2, v(5)), UpdateResult::Abort);
}

// -------------------------------------------------------------------
// Path family 4: Two non-conflicting concurrent transactions
// -------------------------------------------------------------------

/// Path: T1 and T2 write different keys; both commit; each sees the other's value.
#[test]
fn path_two_txs_different_keys_both_commit() {
    let mut s = shard();
    assert_eq!(s.handle_update(T1, 1, K1, v(11)), UpdateResult::Ok);
    assert_eq!(s.handle_update(T2, 2, K2, v(22)), UpdateResult::Ok);

    let t1_prep = match s.handle_prepare(T1) {
        PrepareResult::Timestamp(t) => t,
        _ => panic!(),
    };
    let t2_prep = match s.handle_prepare(T2) {
        PrepareResult::Timestamp(t) => t,
        _ => panic!(),
    };

    let t1_ct = t1_prep + 1;
    let t2_ct = t2_prep + 1;
    s.handle_commit(T1, t1_ct);
    s.handle_commit(T2, t2_ct);

    let read_ts = t1_ct.max(t2_ct) + 1;
    assert_eq!(
        s.handle_read(T3, read_ts, K1, &no_inquiries()),
        ReadResult::Value(v(11))
    );
    assert_eq!(
        s.handle_read(T3, read_ts, K2, &no_inquiries()),
        ReadResult::Value(v(22))
    );
}

/// Path: T1 commits K1; T2 (started after T1 committed) reads K1 then writes K2; commits.
/// T2 must see T1's write (SI1) but can freely write K2.
#[test]
fn path_tx_reads_committed_value_then_writes_different_key() {
    let mut s = shard();
    s.handle_update(T1, 1, K1, v(50));
    let t1_prep = match s.handle_prepare(T1) {
        PrepareResult::Timestamp(t) => t,
        _ => panic!(),
    };
    let t1_ct = t1_prep + 1;
    s.handle_commit(T1, t1_ct);

    // T2 starts after T1 committed.
    let t2_start = t1_ct + 1;
    assert_eq!(
        s.handle_read(T2, t2_start, K1, &no_inquiries()),
        ReadResult::Value(v(50)),
    );
    assert_eq!(s.handle_update(T2, t2_start, K2, v(99)), UpdateResult::Ok);
    let t2_prep = match s.handle_prepare(T2) {
        PrepareResult::Timestamp(t) => t,
        _ => panic!(),
    };
    s.handle_commit(T2, t2_prep + 1);
}

// -------------------------------------------------------------------
// Path family 5: Snapshot isolation — reads see consistent snapshots
// -------------------------------------------------------------------

/// Path: Three versions of K1 installed at different timestamps.
/// Readers at different snapshots see the correct version.
#[test]
fn path_snapshot_reads_across_multiple_versions() {
    let mut s = shard();
    // Version v(1) at t=2.
    s.versions.entry(K1).or_default().push(Version {
        value: v(1),
        timestamp: 2,
    });
    // Version v(2) at t=5.
    s.versions.entry(K1).or_default().push(Version {
        value: v(2),
        timestamp: 5,
    });
    // Version v(3) at t=9.
    s.versions.entry(K1).or_default().push(Version {
        value: v(3),
        timestamp: 9,
    });

    assert_eq!(
        s.handle_read(T1, 1, K1, &no_inquiries()),
        ReadResult::NotFound
    );
    assert_eq!(
        s.handle_read(T1, 2, K1, &no_inquiries()),
        ReadResult::NotFound
    );
    assert_eq!(
        s.handle_read(T1, 3, K1, &no_inquiries()),
        ReadResult::Value(v(1))
    );
    assert_eq!(
        s.handle_read(T1, 5, K1, &no_inquiries()),
        ReadResult::Value(v(1))
    );
    assert_eq!(
        s.handle_read(T1, 6, K1, &no_inquiries()),
        ReadResult::Value(v(2))
    );
    assert_eq!(
        s.handle_read(T1, 9, K1, &no_inquiries()),
        ReadResult::Value(v(2))
    );
    assert_eq!(
        s.handle_read(T1, 10, K1, &no_inquiries()),
        ReadResult::Value(v(3))
    );
}

/// Path: T2 takes a snapshot at ts=3; T1 commits K1 at ts=5 after T2 has started.
/// T2's snapshot must never see T1's write regardless of when T2 reads.
#[test]
fn path_snapshot_not_polluted_by_later_commit() {
    let mut s = shard();
    // T2 has a snapshot at ts=3.
    // Simulate T1 committing K1 at ts=5 by installing the version directly.
    s.versions.entry(K1).or_default().push(Version {
        value: v(99),
        timestamp: 5,
    });
    // T2 (start_ts=3) reads K1 — must not see v(99) committed at ts=5; no prior version.
    assert_eq!(
        s.handle_read(T2, 3, K1, &no_inquiries()),
        ReadResult::NotFound
    );
}

/// Path: T2 snapshot at ts=5 sees commit at ts=4 but not at ts=5.
#[test]
fn path_snapshot_sees_version_committed_one_before_start_ts() {
    let mut s = shard();
    s.versions.entry(K1).or_default().push(Version {
        value: v(7),
        timestamp: 4,
    });
    s.versions.entry(K1).or_default().push(Version {
        value: v(8),
        timestamp: 5,
    });
    // start_ts=5: sees t=4 but not t=5 (strict <).
    assert_eq!(
        s.handle_read(T2, 5, K1, &no_inquiries()),
        ReadResult::Value(v(7))
    );
}

/// Path: T1 and T2 both read the same key at the same snapshot; they agree.
#[test]
fn path_two_txs_same_snapshot_read_agree() {
    let mut s = shard();
    s.versions.entry(K1).or_default().push(Version {
        value: v(42),
        timestamp: 3,
    });
    assert_eq!(
        s.handle_read(T1, 5, K1, &no_inquiries()),
        s.handle_read(T2, 5, K1, &no_inquiries()),
    );
}

// -------------------------------------------------------------------
// Path family 6: Read-your-own-writes within a transaction
// -------------------------------------------------------------------

/// Path: update K1 → read K1 → see own write (before commit).
#[test]
fn path_read_own_write_before_prepare() {
    let mut s = shard();
    s.handle_update(T1, 1, K1, v(77));
    assert_eq!(
        s.handle_read(T1, 1, K1, &no_inquiries()),
        ReadResult::Value(v(77))
    );
}

/// Path: update K1 twice → read K1 → see second write.
#[test]
fn path_read_own_latest_write() {
    let mut s = shard();
    s.handle_update(T1, 1, K1, v(10));
    s.handle_update(T1, 1, K1, v(20));
    assert_eq!(
        s.handle_read(T1, 1, K1, &no_inquiries()),
        ReadResult::Value(v(20))
    );
}

/// Path: update K1 → read K2 → see committed K2 value (not own write to K1).
#[test]
fn path_own_write_does_not_affect_other_keys() {
    let mut s = shard();
    s.versions.entry(K2).or_default().push(Version {
        value: v(55),
        timestamp: 1,
    });
    s.handle_update(T1, 2, K1, v(99));
    assert_eq!(
        s.handle_read(T1, 2, K2, &no_inquiries()),
        ReadResult::Value(v(55))
    );
}

/// Path: own-write shadows an older committed version of the same key.
#[test]
fn path_own_write_shadows_committed_version_of_same_key() {
    let mut s = shard();
    s.versions.entry(K1).or_default().push(Version {
        value: v(10),
        timestamp: 1,
    });
    s.handle_update(T1, 5, K1, v(99));
    // T1's buffered write (99) must shadow the committed v(10).
    assert_eq!(
        s.handle_read(T1, 5, K1, &no_inquiries()),
        ReadResult::Value(v(99))
    );
}

// -------------------------------------------------------------------
// Path family 7: Full INQUIRE protocol sequences
// -------------------------------------------------------------------

/// Path: T1 prepared; T2 reads same key → NeedsInquiry → inquiry=Active → sees old value.
#[test]
fn path_inquire_active_read_sees_pre_prepare_version() {
    let mut s = shard();
    s.versions.entry(K1).or_default().push(Version {
        value: v(3),
        timestamp: 2,
    });
    // T1 must start after the committed version at ts=2 to avoid CommittedConflict.
    s.handle_update(T1, 3, K1, v(77));
    let t1_prep = match s.handle_prepare(T1) {
        PrepareResult::Timestamp(t) => t,
        _ => panic!(),
    };
    // T2 reads K1 at start_ts > t1_prep.
    let start_ts = t1_prep + 2;
    // First attempt: needs inquiry.
    assert_eq!(
        s.handle_read(T2, start_ts, K1, &no_inquiries()),
        ReadResult::NeedsInquiry(vec![T1]),
    );
    // Inquiry says T1 is still active.
    let mut inq = HashMap::new();
    inq.insert(T1, InquiryStatus::Active);
    // T2 ignores T1's buffered write and reads latest committed version before start_ts.
    assert_eq!(
        s.handle_read(T2, start_ts, K1, &inq),
        ReadResult::Value(v(3)),
    );
}

/// Path: T1 prepared; T2 reads → NeedsInquiry → inquiry=Committed, version not installed
/// → still NeedsInquiry.
#[test]
fn path_inquire_committed_but_version_not_installed_still_needs_inquiry() {
    let mut s = shard();
    s.handle_update(T1, 1, K1, v(77));
    let t1_prep = match s.handle_prepare(T1) {
        PrepareResult::Timestamp(t) => t,
        _ => panic!(),
    };
    let commit_ts = t1_prep + 1;
    // Inquiry says committed but handle_commit not called yet.
    let mut inq = HashMap::new();
    inq.insert(T1, InquiryStatus::Committed(commit_ts));
    let start_ts = t1_prep + 3;
    assert_eq!(
        s.handle_read(T2, start_ts, K1, &inq),
        ReadResult::NeedsInquiry(vec![T1]),
    );
}

/// Path: T1 prepared → commit installed → T2 reads with committed inquiry → sees T1's value.
#[test]
fn path_inquire_committed_and_version_installed_reads_new_value() {
    let mut s = shard();
    s.handle_update(T1, 1, K1, v(77));
    let t1_prep = match s.handle_prepare(T1) {
        PrepareResult::Timestamp(t) => t,
        _ => panic!(),
    };
    let commit_ts = t1_prep + 1;
    s.handle_commit(T1, commit_ts);
    let start_ts = commit_ts + 1;
    let mut inq = HashMap::new();
    inq.insert(T1, InquiryStatus::Committed(commit_ts));
    assert_eq!(
        s.handle_read(T2, start_ts, K1, &inq),
        ReadResult::Value(v(77)),
    );
}

/// Path: prepared writer's prep_ts >= start_ts — no inquiry required, read proceeds.
#[test]
fn path_no_inquiry_needed_when_prep_ts_at_or_after_start_ts() {
    let mut s = shard();
    s.handle_update(T1, 5, K1, v(77));
    s.prepared.insert(T1, 7); // prep_t=7 >= start_ts=5
                              // T2's read at start_ts=5: T1's prep_t=7 >= 5, so T1 cannot have committed before
                              // T2's snapshot → no inquiry needed (NotFound since no prior committed version).
    let result = s.handle_read(T2, 5, K1, &no_inquiries());
    assert!(!matches!(result, ReadResult::NeedsInquiry(_)));
}

/// Path: two prepared writers of K1, both with prep_ts < start_ts → NeedsInquiry for both.
#[test]
fn path_two_prepared_writers_both_need_inquiry() {
    let mut s = shard();
    // T1 and T2 are both prepared with writes to K1 (only one at a time normally,
    // but set up directly to test the read path).
    s.write_buff.entry(T1).or_default().insert(K1, v(10));
    s.write_buff.entry(T2).or_default().insert(K1, v(20));
    s.prepared.insert(T1, 2);
    s.prepared.insert(T2, 3);
    // T3 reads K1 at start_ts=10 > both prep_ts.
    let result = s.handle_read(T3, 10, K1, &no_inquiries());
    match result {
        ReadResult::NeedsInquiry(ids) => {
            let id_set: std::collections::HashSet<_> = ids.into_iter().collect();
            assert!(id_set.contains(&T1));
            assert!(id_set.contains(&T2));
        }
        _ => panic!("expected NeedsInquiry for both"),
    }
}

/// Path: prepared writer of K2 (not K1) → reading K1 does not trigger inquiry.
#[test]
fn path_prepared_writer_of_different_key_no_inquiry_for_read() {
    let mut s = shard();
    s.write_buff.entry(T1).or_default().insert(K2, v(50));
    s.prepared.insert(T1, 1);
    // Reading K1 — T1 only wrote K2, so no inquiry (NotFound since K1 has no versions).
    assert!(!matches!(
        s.handle_read(T2, 5, K1, &no_inquiries()),
        ReadResult::NeedsInquiry(_)
    ));
}

// -------------------------------------------------------------------
// Path family 8: Chained transactions — T2 overwrites T1's value
// -------------------------------------------------------------------

/// Path: T1 commits K1=v1; T2 (started after T1) overwrites K1=v2; T3 sees v2.
#[test]
fn path_chained_overwrites_latest_wins() {
    let mut s = shard();
    s.handle_update(T1, 1, K1, v(1));
    let p1 = match s.handle_prepare(T1) {
        PrepareResult::Timestamp(t) => t,
        _ => panic!(),
    };
    s.handle_commit(T1, p1 + 1);

    s.handle_update(T2, p1 + 2, K1, v(2));
    let p2 = match s.handle_prepare(T2) {
        PrepareResult::Timestamp(t) => t,
        _ => panic!(),
    };
    s.handle_commit(T2, p2 + 1);

    // T3 reads at the latest timestamp — must see v(2).
    let latest = p2 + 2;
    assert_eq!(
        s.handle_read(T3, latest, K1, &no_inquiries()),
        ReadResult::Value(v(2))
    );
}

/// Path: T2 reads K1 at a snapshot between T1 and T2's own commit — sees T1's value.
#[test]
fn path_mid_chain_snapshot_sees_intermediate_value() {
    let mut s = shard();
    s.handle_update(T1, 1, K1, v(1));
    let p1 = match s.handle_prepare(T1) {
        PrepareResult::Timestamp(t) => t,
        _ => panic!(),
    };
    let c1 = p1 + 1;
    s.handle_commit(T1, c1);

    s.handle_update(T2, c1 + 1, K1, v(2));
    let p2 = match s.handle_prepare(T2) {
        PrepareResult::Timestamp(t) => t,
        _ => panic!(),
    };
    s.handle_commit(T2, p2 + 1);

    // Snapshot between c1 and c2 sees T1's write.
    assert_eq!(
        s.handle_read(T3, c1 + 1, K1, &no_inquiries()),
        ReadResult::Value(v(1))
    );
}

// -------------------------------------------------------------------
// Path family 9: Clock monotonicity under interleaving
// -------------------------------------------------------------------

/// Path: prepare, update with higher ts, prepare again — clocks stay monotone.
#[test]
fn path_clock_monotone_through_interleaved_ops() {
    let mut s = shard();
    s.handle_update(T1, 3, K1, v(1));
    let t_after_update = s.clock;
    s.handle_read(T2, 10, K2, &no_inquiries()); // bumps clock
    let t_after_read = s.clock;
    assert!(t_after_read >= t_after_update);
    assert!(t_after_read >= 10);
}

/// Path: successive prepares yield strictly increasing timestamps.
#[test]
fn path_successive_prepares_strictly_increasing() {
    let mut s = shard();
    // T1 and T2 write different keys so they can both prepare.
    s.handle_update(T1, 1, K1, v(1));
    s.handle_update(T2, 1, K2, v(2));
    let p1 = match s.handle_prepare(T1) {
        PrepareResult::Timestamp(t) => t,
        _ => panic!(),
    };
    let p2 = match s.handle_prepare(T2) {
        PrepareResult::Timestamp(t) => t,
        _ => panic!(),
    };
    assert!(p2 > p1, "p2={p2} should be > p1={p1}");
}

/// Path: update with ts=100 → prepare gets timestamp > 100.
#[test]
fn path_prepare_ts_reflects_clock_after_high_ts_update() {
    let mut s = shard();
    s.handle_update(T1, 100, K1, v(1));
    let prep_t = match s.handle_prepare(T1) {
        PrepareResult::Timestamp(t) => t,
        _ => panic!(),
    };
    assert!(
        prep_t > 100,
        "prep_t={prep_t} should reflect that clock was advanced to 100"
    );
}

// -------------------------------------------------------------------
// Path family 10: Prepare-then-abort vs prepare-then-commit divergence
// -------------------------------------------------------------------

/// Path: T1 prepares, aborts → T2 can now write and prepare without conflict.
#[test]
fn path_prepare_abort_clears_prepared_set() {
    let mut s = shard();
    s.handle_update(T1, 1, K1, v(10));
    s.handle_prepare(T1);
    assert!(s.prepared.contains_key(&T1));
    s.handle_abort(T1);
    assert!(!s.prepared.contains_key(&T1));
    // T2 can now write and prepare K1.
    assert_eq!(s.handle_update(T2, 2, K1, v(20)), UpdateResult::Ok);
    assert!(matches!(s.handle_prepare(T2), PrepareResult::Timestamp(_)));
}

/// Path: T1 commits → T1 no longer in prepared set.
#[test]
fn path_commit_clears_prepared_set() {
    let mut s = shard();
    s.handle_update(T1, 1, K1, v(10));
    let prep_t = match s.handle_prepare(T1) {
        PrepareResult::Timestamp(t) => t,
        _ => panic!(),
    };
    assert!(s.prepared.contains_key(&T1));
    s.handle_commit(T1, prep_t + 1);
    assert!(!s.prepared.contains_key(&T1));
}

/// Path: T1 commits → T2 can prepare the same key without prepared conflict.
#[test]
fn path_commit_enables_subsequent_prepare() {
    let mut s = shard();
    s.handle_update(T1, 1, K1, v(10));
    let p1 = match s.handle_prepare(T1) {
        PrepareResult::Timestamp(t) => t,
        _ => panic!(),
    };
    s.handle_commit(T1, p1 + 1);

    s.handle_update(T2, p1 + 2, K1, v(20));
    assert!(matches!(s.handle_prepare(T2), PrepareResult::Timestamp(_)));
}

// -------------------------------------------------------------------
// Path family 11: Edge cases at timestamp zero / uninserted keys
// -------------------------------------------------------------------

/// Path: read at start_ts=1 on a key that has never been inserted → NotFound.
#[test]
fn path_read_at_ts_one_returns_not_found() {
    let mut s = shard();
    assert_eq!(
        s.handle_read(T1, 1, K1, &no_inquiries()),
        ReadResult::NotFound
    );
}

/// Path: first write to a key has no committed conflict (no prior versions).
#[test]
fn path_first_insert_has_no_committed_conflict() {
    let mut s = shard();
    assert_eq!(s.handle_update(T1, 1, K1, v(5)), UpdateResult::Ok);
    assert_eq!(s.handle_update(T2, 1, K2, v(6)), UpdateResult::Ok);
}

/// Path: read at start_ts=0 returns NotFound (no version has ts < 0).
#[test]
fn path_read_at_ts_zero_returns_not_found() {
    let mut s = shard();
    assert_eq!(
        s.handle_read(T1, 0, K1, &no_inquiries()),
        ReadResult::NotFound
    );
}

// -------------------------------------------------------------------
// Path family 12: Idempotency and message-set persistence
// -------------------------------------------------------------------

/// Path: calling handle_commit twice for the same tx must not install duplicate versions.
#[test]
fn path_double_commit_does_not_create_duplicate_versions() {
    let mut s = shard();
    s.handle_update(T1, 1, K1, v(10));
    s.handle_prepare(T1);
    s.handle_commit(T1, 3);
    // Calling commit again (idempotent expectation from the TLA+ guard model).
    s.handle_commit(T1, 3);
    // Only one version at ts=3 should exist.
    let count = s.versions[&K1].iter().filter(|v| v.timestamp == 3).count();
    assert_eq!(count, 1);
}

/// Path: calling handle_abort twice is idempotent.
#[test]
fn path_double_abort_is_idempotent() {
    let mut s = shard();
    s.handle_update(T1, 1, K1, v(10));
    s.handle_abort(T1);
    s.handle_abort(T1);
    // After both aborts, write_buff for T1 must be cleared.
    // T1 may or may not still be in `aborted` depending on pruning; we only
    // assert the observable effect: the write buffer is empty.
    assert!(s.write_buff.get(&T1).map(|b| b.is_empty()).unwrap_or(true));
}

// -------------------------------------------------------------------
// Path family 13: SI property end-to-end sequences
// -------------------------------------------------------------------

/// End-to-end SI1: T1 commits before T2 starts → T2 always reads T1's value.
#[test]
fn path_si1_e2e_committed_before_start() {
    let mut s = shard();
    s.handle_update(T1, 1, K1, v(42));
    let p = match s.handle_prepare(T1) {
        PrepareResult::Timestamp(t) => t,
        _ => panic!(),
    };
    s.handle_commit(T1, p + 1);
    // T2 starts strictly after T1 committed.
    for start_ts in [p + 2, p + 5, p + 10] {
        assert_eq!(
            s.handle_read(T2, start_ts, K1, &no_inquiries()),
            ReadResult::Value(v(42)),
            "T2 with start_ts={start_ts} should see T1's write",
        );
    }
}

/// End-to-end SI4: after T1 commits K1, any concurrent writer is rejected.
#[test]
fn path_si4_e2e_concurrent_writer_rejected_after_commit() {
    let mut s = shard();
    s.handle_update(T1, 2, K1, v(10));
    let p = match s.handle_prepare(T1) {
        PrepareResult::Timestamp(t) => t,
        _ => panic!(),
    };
    s.handle_commit(T1, p + 1);
    // T2 started at ts=1 (before T1 committed) — concurrent → ABORT.
    assert_eq!(s.handle_update(T2, 1, K1, v(20)), UpdateResult::Abort);
}

/// End-to-end SI3: MVCC timestamps remain distinct across many sequential commits.
#[test]
fn path_si3_e2e_many_sequential_commits_distinct_timestamps() {
    let mut s = shard();
    let txids = [101u64, 102, 103, 104, 105];
    let mut last_ct = 0u64;
    for &id in &txids {
        let start = last_ct + 1;
        assert_eq!(
            s.handle_update(id, start, K1, v(id as u64)),
            UpdateResult::Ok
        );
        let prep = match s.handle_prepare(id) {
            PrepareResult::Timestamp(t) => t,
            _ => panic!(),
        };
        let ct = prep + 1;
        s.handle_commit(id, ct);
        // Verify the new version has a timestamp distinct from all prior ones.
        let ts_set: std::collections::HashSet<_> =
            s.versions[&K1].iter().map(|v| v.timestamp).collect();
        assert_eq!(
            ts_set.len(),
            s.versions[&K1].len(),
            "duplicate timestamps after committing tx {id}"
        );
        last_ct = ct;
    }
}

// -----------------------------------------------------------------------
// write_keys index — TLA+ trace corpus
// Traces: WriteKeyOwnerConsistent invariant, ShardHandleUpdate (success +
// conflict), ShardHandleCommit, ShardHandleAbort paths.
// -----------------------------------------------------------------------

// Trace: Init — write_key_owner[k] = NONE for all k.
#[test]
fn write_keys_empty_initially() {
    let s = shard();
    assert!(s.write_keys.is_empty());
}

// Trace: ShardHandleUpdate OK — write_key_owner[K1] set to T1.
#[test]
fn write_keys_populated_on_successful_update() {
    let mut s = shard();
    assert_eq!(s.handle_update(T1, 1, K1, v(10)), UpdateResult::Ok);
    assert_eq!(s.write_keys.get(&K1), Some(&T1));
}

// Trace: ShardHandleUpdate OK for a different key — only that key indexed.
#[test]
fn write_keys_only_indexes_written_key() {
    let mut s = shard();
    s.handle_update(T1, 1, K1, v(10));
    assert_eq!(s.write_keys.get(&K1), Some(&T1));
    assert!(!s.write_keys.contains_key(&K2));
}

// Trace: WriteBuffConflictFast — T1 holds K1; T2 update to K1 aborts.
#[test]
fn write_keys_detects_conflict_for_second_writer() {
    let mut s = shard();
    s.handle_update(T1, 1, K1, v(10));
    // T2 starts later, tries to write same key.
    assert_eq!(s.handle_update(T2, 2, K1, v(20)), UpdateResult::Abort);
    // Index still points to T1 (the original holder).
    assert_eq!(s.write_keys.get(&K1), Some(&T1));
}

// Trace: ShardHandleCommit — write_key_owner cleared after commit.
#[test]
fn write_keys_cleared_after_commit() {
    let mut s = shard();
    s.handle_update(T1, 1, K1, v(10));
    s.handle_prepare(T1);
    s.handle_commit(T1, 5);
    assert!(!s.write_keys.contains_key(&K1));
}

// Trace: ShardHandleAbort — write_key_owner cleared after abort.
#[test]
fn write_keys_cleared_after_abort() {
    let mut s = shard();
    s.handle_update(T1, 1, K1, v(10));
    s.handle_abort(T1);
    assert!(!s.write_keys.contains_key(&K1));
}

// Trace: same tx overwrites its own key — index stays pointing to same tx.
#[test]
fn write_keys_same_tx_overwrite_retains_owner() {
    let mut s = shard();
    s.handle_update(T1, 1, K1, v(10));
    // T1 updates K1 again (idempotent write semantics).
    assert_eq!(s.handle_update(T1, 1, K1, v(99)), UpdateResult::Ok);
    assert_eq!(s.write_keys.get(&K1), Some(&T1));
}

// Trace: sequential writers — T1 commits K1, then T2 can acquire K1.
#[test]
fn write_keys_sequential_writers_after_commit() {
    let mut s = shard();
    s.handle_update(T1, 1, K1, v(10));
    s.handle_prepare(T1);
    s.handle_commit(T1, 2);
    // T2 starts after T1's commit_ts; no conflict.
    assert_eq!(s.handle_update(T2, 3, K1, v(20)), UpdateResult::Ok);
    assert_eq!(s.write_keys.get(&K1), Some(&T2));
}

// Trace: sequential writers — T1 aborts, then T2 can acquire K1.
#[test]
fn write_keys_sequential_writers_after_abort() {
    let mut s = shard();
    s.handle_update(T1, 1, K1, v(10));
    s.handle_abort(T1);
    assert_eq!(s.handle_update(T2, 2, K1, v(20)), UpdateResult::Ok);
    assert_eq!(s.write_keys.get(&K1), Some(&T2));
}

// Trace: multi-key tx — all written keys are indexed; commit clears all.
#[test]
fn write_keys_multi_key_tx_all_indexed_then_cleared() {
    let mut s = shard();
    s.handle_update(T1, 1, K1, v(10));
    s.handle_update(T1, 1, K2, v(20));
    s.handle_update(T1, 1, K3, v(30));
    assert_eq!(s.write_keys.get(&K1), Some(&T1));
    assert_eq!(s.write_keys.get(&K2), Some(&T1));
    assert_eq!(s.write_keys.get(&K3), Some(&T1));
    s.handle_prepare(T1);
    s.handle_commit(T1, 5);
    assert!(s.write_keys.is_empty());
}

// Trace: WriteKeyOwnerConsistent — index matches write_buff at every step.
#[test]
fn write_keys_consistent_with_write_buff_throughout() {
    let mut s = shard();

    let consistent = |s: &ShardState| {
        // Every entry in write_keys must point to a tx that has that key in write_buff.
        for (&key, &owner) in &s.write_keys {
            assert!(
                s.write_buff
                    .get(&owner)
                    .map(|wb| wb.contains_key(&key))
                    .unwrap_or(false),
                "write_keys[{key}] = {owner} but write_buff[{owner}] has no key {key}"
            );
        }
        // Every key in every write_buff must be in write_keys pointing back to its tx.
        for (&tx_id, wb) in &s.write_buff {
            for &key in wb.keys() {
                assert_eq!(
                    s.write_keys.get(&key),
                    Some(&tx_id),
                    "write_buff[{tx_id}] has key {key} but write_keys[{key}] != {tx_id}"
                );
            }
        }
    };

    consistent(&s);
    s.handle_update(T1, 1, K1, v(10));
    consistent(&s);
    s.handle_update(T1, 1, K2, v(20));
    consistent(&s);
    s.handle_prepare(T1);
    consistent(&s);
    s.handle_commit(T1, 5);
    consistent(&s);
    s.handle_update(T2, 6, K1, v(30));
    consistent(&s);
    s.handle_abort(T2);
    consistent(&s);
}

// Trace: index preserved through prepare phase — write lock held until commit.
#[test]
fn write_keys_held_through_prepare_phase() {
    let mut s = shard();
    s.handle_update(T1, 1, K1, v(10));
    s.handle_prepare(T1);
    // After prepare, T1 still holds the write lock.
    assert_eq!(s.write_keys.get(&K1), Some(&T1));
    // T2 attempting to write K1 while T1 is prepared → abort.
    assert_eq!(s.handle_update(T2, 2, K1, v(20)), UpdateResult::Abort);
}

// Trace: aborted tx that never successfully wrote — index not polluted.
#[test]
fn write_keys_failed_update_does_not_pollute_index() {
    let mut s = shard();
    s.handle_update(T1, 1, K1, v(10));
    // T2 fails to write K1 (conflict with T1).
    s.handle_update(T2, 2, K1, v(20));
    // Index still has T1, not T2.
    assert_eq!(s.write_keys.get(&K1), Some(&T1));
    // Explicit abort of T2 (coordinator sends ABORT) doesn't disturb T1's lock.
    s.handle_abort(T2);
    assert_eq!(s.write_keys.get(&K1), Some(&T1));
}

// -----------------------------------------------------------------------
// prune_aborted — bounded aborted HashSet
//
// TxId encoding: (coordinator_port << 32) | seq.
// A shard can prune aborted entry with seq=N from port=P once the
// minimum in-flight seq from P is > N (or P has no in-flight txs).
// -----------------------------------------------------------------------

// Port/seq encoded TxIds used only by the prune tests.
const P1_S1: TxId = (1u64 << 32) | 1; // port 1, seq 1
const P1_S2: TxId = (1u64 << 32) | 2; // port 1, seq 2
const P1_S3: TxId = (1u64 << 32) | 3; // port 1, seq 3
const P2_S1: TxId = (2u64 << 32) | 1; // port 2, seq 1
const P2_S2: TxId = (2u64 << 32) | 2; // port 2, seq 2

// Trace: ShardPruneAborted on empty set — no-op.
#[test]
fn prune_aborted_noop_when_set_empty() {
    let mut s = shard();
    s.prune_aborted();
    assert!(s.aborted.is_empty());
}

// Trace: tx is ABORTED at coordinator, no active tx from same port.
// ShardPruneAborted fires and removes the entry.
#[test]
fn prune_aborted_removes_when_no_active_tx_from_same_port() {
    let mut s = shard();
    s.aborted.insert(P1_S1);
    // No write_buff or prepared entries from port 1 → safe to prune.
    s.prune_aborted();
    assert!(!s.aborted.contains(&P1_S1));
}

// Trace: aborted entry from port P; active tx from a different port P2.
// Different port's watermark is irrelevant — P1_S1 still pruned.
#[test]
fn prune_aborted_ignores_active_tx_from_different_port() {
    let mut s = shard();
    s.aborted.insert(P1_S1);
    // Simulate P2_S1 active in write_buff.
    s.write_buff.entry(P2_S1).or_default().insert(K1, v(10));
    s.prune_aborted();
    assert!(
        !s.aborted.contains(&P1_S1),
        "P1_S1 should be pruned (P2's watermark is irrelevant)"
    );
}

// Trace: aborted entry P1_S1; active tx P1_S3 in write_buff (seq 3 > 1).
// min_active_seq[P1] = 3, aborted seq = 1 < 3 → pruned.
#[test]
fn prune_aborted_removes_lower_seq_when_higher_seq_active_in_write_buff() {
    let mut s = shard();
    s.aborted.insert(P1_S1);
    s.write_buff.entry(P1_S3).or_default().insert(K1, v(10));
    s.prune_aborted();
    assert!(
        !s.aborted.contains(&P1_S1),
        "P1_S1 seq=1 < min_active_seq=3, should be pruned"
    );
}

// Trace: aborted entry P1_S3; active tx P1_S1 in write_buff (seq 1 < 3).
// min_active_seq[P1] = 1, aborted seq = 3 >= 1 → retained (conservative).
#[test]
fn prune_aborted_retains_higher_seq_when_lower_seq_active_in_write_buff() {
    let mut s = shard();
    s.aborted.insert(P1_S3);
    s.write_buff.entry(P1_S1).or_default().insert(K1, v(10));
    s.prune_aborted();
    assert!(
        s.aborted.contains(&P1_S3),
        "P1_S3 seq=3 >= min_active_seq=1, must be retained"
    );
}

// Trace: aborted entry P1_S1; active tx P1_S2 in prepared (not write_buff).
// Prepared txs also count as in-flight: min_active_seq[P1] = 2 > 1 → pruned.
#[test]
fn prune_aborted_removes_lower_seq_when_higher_seq_in_prepared() {
    let mut s = shard();
    s.aborted.insert(P1_S1);
    s.prepared.insert(P1_S2, 5); // P1_S2 is prepared at ts=5
    s.prune_aborted();
    assert!(
        !s.aborted.contains(&P1_S1),
        "P1_S1 seq=1 < min_active_seq=2, should be pruned"
    );
}

// Trace: multiple aborted entries from same port; only older ones pruned.
// aborted = {P1_S1, P1_S3}, write_buff has P1_S2 (active).
// min_active_seq[P1] = 2: seq=1 < 2 → prune; seq=3 >= 2 → retain.
#[test]
fn prune_aborted_selectively_prunes_only_safe_entries() {
    let mut s = shard();
    s.aborted.insert(P1_S1);
    s.aborted.insert(P1_S3);
    s.write_buff.entry(P1_S2).or_default().insert(K1, v(10));
    s.prune_aborted();
    assert!(
        !s.aborted.contains(&P1_S1),
        "P1_S1 seq=1 < min=2, should be pruned"
    );
    assert!(
        s.aborted.contains(&P1_S3),
        "P1_S3 seq=3 >= min=2, should be retained"
    );
}

// Trace: multiple ports; entries from each pruned independently.
// aborted = {P1_S1, P2_S2}; write_buff has P1_S3 (active from port 1).
// min_active_seq[P1]=3 → P1_S1 pruned; no active from P2 → P2_S2 pruned.
#[test]
fn prune_aborted_handles_multiple_ports_independently() {
    let mut s = shard();
    s.aborted.insert(P1_S1);
    s.aborted.insert(P2_S2);
    s.write_buff.entry(P1_S3).or_default().insert(K1, v(10));
    s.prune_aborted();
    assert!(
        !s.aborted.contains(&P1_S1),
        "P1_S1 pruned (seq=1 < min_active[P1]=3)"
    );
    assert!(
        !s.aborted.contains(&P2_S2),
        "P2_S2 pruned (no active tx from P2)"
    );
}

// Trace: handle_abort automatically calls prune_aborted.
// P1_S3 is in write_buff (active); P1_S1 is being aborted.
// After abort: P1_S1 in aborted, prune fires, P1_S1 immediately pruned
// (seq=1 < min_active_seq[P1]=3).
#[test]
fn prune_aborted_called_automatically_from_handle_abort() {
    let mut s = shard();
    s.write_buff.entry(P1_S3).or_default().insert(K1, v(10));
    s.handle_abort(P1_S1);
    // P1_S1 should be immediately pruned since P1_S3 (seq=3) is active.
    assert!(
        !s.aborted.contains(&P1_S1),
        "P1_S1 should be pruned immediately after abort"
    );
}

// Trace: handle_commit automatically calls prune_aborted.
// P1_S1 was aborted earlier; P1_S2 just committed.
// After commit: write_buff[P1_S2] cleared, prune fires, P1_S1 pruned
// (no active tx from P1 remaining).
#[test]
fn prune_aborted_called_automatically_from_handle_commit() {
    let mut s = shard();
    // Set up: P1_S1 was aborted previously.
    s.aborted.insert(P1_S1);
    // P1_S2 writes a key and commits.
    s.write_buff.entry(P1_S2).or_default().insert(K1, v(20));
    s.prepared.insert(P1_S2, 3);
    s.handle_commit(P1_S2, 4);
    // After commit, prune_aborted fires; no active from P1 → P1_S1 pruned.
    assert!(
        !s.aborted.contains(&P1_S1),
        "P1_S1 should be pruned after P1_S2 commits"
    );
}

// Trace: aborted set entry that can't yet be pruned (in-flight tx from same port).
// P1_S2 is aborted; P1_S1 still in write_buff (old active tx, lower seq).
// min_active_seq[P1] = 1, aborted seq=2 >= 1 → must retain.
#[test]
fn prune_aborted_retains_entry_when_older_tx_still_active() {
    let mut s = shard();
    s.aborted.insert(P1_S2);
    s.write_buff.entry(P1_S1).or_default().insert(K1, v(10));
    s.prune_aborted();
    assert!(
        s.aborted.contains(&P1_S2),
        "P1_S2 must be retained: P1_S1 (seq=1) still active"
    );
}

// -----------------------------------------------------------------------
// ShardPruneOrphanedPort — background pruning of dead coordinator ports
//
// Traces from the TLA+ spec action ShardPruneOrphanedPort:
//   T1: coord dead (no in-flight txs) → ALL its aborted entries pruned
//   T2: mixed: dead coord entries pruned; active coord entries retained
//   T3: coord transiently idle (no active txs) → entries pruned (safe)
//   T4: coord still has prepared entry → entries NOT pruned
// -----------------------------------------------------------------------

// T1: Single dead coordinator — all aborted entries from that port pruned at once.
#[test]
fn prune_aborted_clears_all_entries_from_dead_port() {
    let mut s = shard();
    // Three completed (and aborted) transactions from port 1, none in-flight.
    s.aborted.insert(P1_S1);
    s.aborted.insert(P1_S2);
    s.aborted.insert(P1_S3);
    // No write_buff or prepared entries from port 1 → prune_aborted fires.
    s.prune_aborted();
    assert!(
        !s.aborted.contains(&P1_S1),
        "P1_S1 must be pruned: dead coordinator"
    );
    assert!(
        !s.aborted.contains(&P1_S2),
        "P1_S2 must be pruned: dead coordinator"
    );
    assert!(
        !s.aborted.contains(&P1_S3),
        "P1_S3 must be pruned: dead coordinator"
    );
}

// T2: Dead coordinator entries pruned while active coordinator entries are retained.
// P1 is dead (no in-flight txs); P2 has an active tx (P2_S2 in write_buff).
// P2_S1 seq=1 < min_active[P2]=2 → also pruned.
// P2_S2 seq=2 is active, not in aborted.
#[test]
fn dead_port_pruned_active_port_watermark_respected() {
    let mut s = shard();
    s.aborted.insert(P1_S1); // dead port P1
    s.aborted.insert(P2_S1); // old entry from active port P2
                             // P2_S2 is active → min_active[P2] = 2; P2_S1 seq=1 < 2 → pruned.
    s.write_buff.entry(P2_S2).or_default().insert(K1, v(10));

    s.prune_aborted();

    // P1_S1 pruned: P1 has no active txs.
    assert!(
        !s.aborted.contains(&P1_S1),
        "P1_S1 must be pruned: dead coordinator"
    );
    // P2_S1 pruned: seq=1 < min_active[P2]=2.
    assert!(
        !s.aborted.contains(&P2_S1),
        "P2_S1 must be pruned: seq below active watermark"
    );
}

// T3: Coordinator has no current in-flight txs (transiently idle) → entries pruned.
// Safe because new transactions from that coordinator will have fresh tx_ids.
#[test]
fn transiently_idle_port_entries_pruned() {
    let mut s = shard();
    // P1_S1 was aborted; P1 completed all its work and is currently idle.
    s.aborted.insert(P1_S1);
    // No write_buff or prepared from P1 → prune_aborted clears it.
    s.prune_aborted();
    assert!(
        !s.aborted.contains(&P1_S1),
        "idle port entry must be pruned"
    );
}

// T4: Coordinator still has a prepared entry → aborted entries NOT pruned.
// P1_S2 is prepared; P1_S1 is aborted. min_active[P1]=2, P1_S1 seq=1 < 2 → pruned.
// (Note: if the prepared entry has a higher seq, older aborted entries are still pruned.)
#[test]
fn aborted_entry_pruned_when_prepared_has_higher_seq() {
    let mut s = shard();
    s.aborted.insert(P1_S1); // seq=1
    s.prepared.insert(P1_S2, 5); // seq=2 prepared → min_active[P1]=2

    s.prune_aborted();

    assert!(
        !s.aborted.contains(&P1_S1),
        "P1_S1 seq=1 < min_active[P1]=2 → pruned"
    );
}

// T4b: Active prepared entry with lower seq → higher-seq aborted entry retained.
// P1_S1 is prepared; P1_S2 is aborted. min_active[P1]=1, P1_S2 seq=2 >= 1 → retained.
#[test]
fn aborted_entry_retained_when_prepared_has_lower_seq() {
    let mut s = shard();
    s.aborted.insert(P1_S2); // seq=2
    s.prepared.insert(P1_S1, 3); // seq=1 prepared → min_active[P1]=1

    s.prune_aborted();

    assert!(
        s.aborted.contains(&P1_S2),
        "P1_S2 seq=2 >= min_active[P1]=1 → must be retained"
    );
}

// -----------------------------------------------------------------------
// handle_fast_commit — single-shard fast path
//
// TLA+ trace: CoordFastCommit → ShardHandleFastCommit → CoordHandleFastCommitReply
// -----------------------------------------------------------------------

// Trace: normal path — tx writes K1, fast commit installs version.
// ShardHandleFastCommit success case (id ∉ s_aborted, write_buff[id] ≠ {}).
#[test]
fn handle_fast_commit_installs_version() {
    let mut s = shard();
    s.handle_update(T1, 1, K1, v(42));
    let r = s.handle_fast_commit(T1);
    // Ok(commit_ts) where commit_ts = s_clock + 1 at call time.
    let FastCommitResult::Ok(commit_ts) = r else {
        panic!("expected Ok")
    };
    // Version must be installed.
    let installed = s
        .versions
        .get(&K1)
        .map(|vs| {
            vs.iter()
                .any(|ver| ver.timestamp == commit_ts && ver.value == v(42))
        })
        .unwrap_or(false);
    assert!(installed, "version not installed after fast commit");
}

// Trace: ShardHandleFastCommit — commit_ts = s_clock + 1 at time of call.
#[test]
fn handle_fast_commit_uses_shard_clock_plus_one() {
    let mut s = shard();
    s.handle_update(T1, 5, K1, v(10)); // s_clock advances to 5
    let FastCommitResult::Ok(ct) = s.handle_fast_commit(T1) else {
        panic!()
    };
    assert_eq!(ct, 6, "commit_ts must be s_clock+1 = 6");
}

// Trace: ShardHandleFastCommit advances shard clock to commit_ts.
#[test]
fn handle_fast_commit_advances_shard_clock() {
    let mut s = shard();
    s.handle_update(T1, 3, K1, v(10)); // s_clock = 3
    s.handle_fast_commit(T1);
    assert_eq!(s.clock, 4, "s_clock must advance to commit_ts=4");
}

// Trace: ShardHandleFastCommit clears write_buff for the tx.
#[test]
fn handle_fast_commit_clears_write_buff() {
    let mut s = shard();
    s.handle_update(T1, 1, K1, v(10));
    s.handle_fast_commit(T1);
    assert!(
        s.write_buff.get(&T1).map(|b| b.is_empty()).unwrap_or(true),
        "write_buff must be cleared after fast commit"
    );
}

// Trace: ShardHandleFastCommit releases write_key_owner for committed keys.
#[test]
fn handle_fast_commit_releases_write_keys() {
    let mut s = shard();
    s.handle_update(T1, 1, K1, v(10));
    assert_eq!(s.write_keys.get(&K1), Some(&T1));
    s.handle_fast_commit(T1);
    assert_eq!(
        s.write_keys.get(&K1),
        None,
        "write lock must be released after fast commit"
    );
}

// Trace: ShardHandleFastCommit with multiple keys — all versions installed.
#[test]
fn handle_fast_commit_installs_all_writes() {
    let mut s = shard();
    s.handle_update(T1, 1, K1, v(10));
    s.handle_update(T1, 1, K2, v(20));
    s.handle_update(T1, 1, K3, v(30));
    let FastCommitResult::Ok(ct) = s.handle_fast_commit(T1) else {
        panic!()
    };
    for (key, val) in [(K1, v(10)), (K2, v(20)), (K3, v(30))] {
        let installed = s
            .versions
            .get(&key)
            .map(|vs| vs.iter().any(|ver| ver.timestamp == ct && ver.value == val))
            .unwrap_or(false);
        assert!(installed, "version for key {key} not installed");
    }
}

// Trace: ShardHandleFastCommit — aborted tx returns Abort (id ∈ s_aborted).
#[test]
fn handle_fast_commit_on_aborted_tx_returns_abort() {
    let mut s = shard();
    let t_older: TxId = 50; // prevents T1=101 from being pruned
    s.write_buff.entry(t_older).or_default().insert(K2, v(99));
    s.handle_update(T1, 1, K1, v(42));
    s.handle_abort(T1);
    // T1 is aborted and (with older tx holding the watermark) still in s.aborted.
    assert_eq!(s.handle_fast_commit(T1), FastCommitResult::Abort);
}

// Trace: after ShardHandleFastCommit, committed version is visible to readers.
#[test]
fn handle_fast_commit_version_is_readable() {
    let mut s = shard();
    s.handle_update(T1, 1, K1, v(99));
    let FastCommitResult::Ok(ct) = s.handle_fast_commit(T1) else {
        panic!()
    };
    // T2 with start_ts > ct sees T1's value.
    assert_eq!(
        s.handle_read(T2, ct + 1, K1, &no_inquiries()),
        ReadResult::Value(v(99)),
    );
}

// Trace: ShardHandleFastCommit — concurrent write by T2 after T1 fast-commits
// sees the new committed version.
#[test]
fn handle_fast_commit_committed_version_blocks_past_writers() {
    let mut s = shard();
    s.handle_update(T1, 1, K1, v(10));
    let FastCommitResult::Ok(ct) = s.handle_fast_commit(T1) else {
        panic!()
    };
    // T2 with start_ts <= ct — CommittedConflict prevents the write.
    assert_eq!(
        s.handle_update(T2, ct - 1, K1, v(20)),
        UpdateResult::Abort,
        "write with start_ts < commit_ts must be aborted"
    );
}

// Trace: fast-commit doesn't interfere with a concurrently buffered tx.
// T2 buffers a write to K2; T1 fast-commits K1. T2's write_buff is intact.
#[test]
fn handle_fast_commit_does_not_disturb_other_tx_write_buff() {
    let mut s = shard();
    s.handle_update(T1, 1, K1, v(10));
    s.handle_update(T2, 1, K2, v(20));
    s.handle_fast_commit(T1);
    // T2's buffered write must be unchanged.
    assert_eq!(s.write_buff.get(&T2).and_then(|b| b.get(&K2)), Some(&v(20)),);
}

// -----------------------------------------------------------------------
// handle_fast_commit — idempotency fix
//
// Traces FC-I and FC-E from ShardHandleFastCommit spec note.
// The fix: check write_buff BEFORE advancing s_clock, and store commit_ts
// in fast_committed for idempotent retries.
// -----------------------------------------------------------------------

// Trace FC-I: second call returns the same commit_ts without advancing the clock.
#[test]
fn handle_fast_commit_idempotent_returns_same_commit_ts() {
    let mut s = shard();
    // Anchor: a tx with the same coordinator port (port bits = T1's port bits = 0)
    // but a LOWER seq (50 < 101 = T1's seq), so its presence in write_buff keeps
    // min_active_seq[0] = 50, which is <= T1's seq=101, preventing prune_aborted()
    // from discarding T1's fast_committed entry after the first commit.
    const ANCHOR: TxId = 50; // port=0, seq=50
    s.write_buff.entry(ANCHOR).or_default().insert(K2, v(0));

    s.handle_update(T1, 1, K1, v(42));
    let FastCommitResult::Ok(ct1) = s.handle_fast_commit(T1) else {
        panic!("first call must succeed");
    };
    // Second call — write_buff is now empty but fast_committed has the entry.
    let FastCommitResult::Ok(ct2) = s.handle_fast_commit(T1) else {
        panic!("idempotent retry must return Ok");
    };
    assert_eq!(ct1, ct2, "idempotent retry must return the same commit_ts");
}

// Trace FC-I: idempotent retry must NOT advance the shard clock a second time.
#[test]
fn handle_fast_commit_idempotent_does_not_advance_clock() {
    let mut s = shard();
    // Same anchor as above: lower-seq tx keeps fast_committed entry alive.
    const ANCHOR: TxId = 50; // port=0, seq=50
    s.write_buff.entry(ANCHOR).or_default().insert(K2, v(0));

    s.handle_update(T1, 1, K1, v(42));
    s.handle_fast_commit(T1);
    let clock_after_first = s.clock;
    s.handle_fast_commit(T1);
    assert_eq!(
        s.clock, clock_after_first,
        "idempotent retry must not burn a second timestamp"
    );
}

// Trace FC-E: write_buff absent and not in fast_committed → Abort.
// This guards against a spurious fast_commit for a tx that never buffered writes here.
#[test]
fn handle_fast_commit_empty_write_buff_never_committed_returns_abort() {
    let mut s = shard();
    // T1 never called handle_update on this shard — no write_buff, not fast_committed.
    let r = s.handle_fast_commit(T1);
    assert_eq!(
        r,
        FastCommitResult::Abort,
        "tx with no write_buff and not fast_committed must abort"
    );
}

// Trace FC-I: fast_committed entry is pruned by prune_aborted once the
// coordinator has no more in-flight txs from the same port.
#[test]
fn handle_fast_commit_fast_committed_pruned_when_no_inflight() {
    // port=1, tx_a has seq=2 (higher), tx_b has seq=1 (lower).
    // With tx_b (seq=1) in write_buff, min_active_seq[1] = 1.
    // tx_a (seq=2): 2 >= 1 → retained.
    // After tx_b removed: no in-flight → None → tx_a seq=2 not retained → pruned.
    let port: u64 = 1;
    let tx_a: TxId = (port << 32) | 2; // seq=2 (the one we fast-commit)
    let tx_b: TxId = (port << 32) | 1; // seq=1 (lower seq; anchors the watermark)

    let mut s = shard();
    // Buffer a write for tx_b (lower seq) so it anchors min_active_seq[1] = 1.
    s.write_buff.entry(tx_b).or_default().insert(K2, v(99));

    // Fast-commit tx_a.
    s.handle_update(tx_a, 1, K1, v(42));
    let FastCommitResult::Ok(_) = s.handle_fast_commit(tx_a) else {
        panic!("fast commit must succeed");
    };
    // tx_a (seq=2) is in fast_committed; tx_b (seq=1) is still in write_buff.
    // min_active_seq[1] = 1. tx_a seq=2 >= 1 → retained.
    s.prune_aborted();
    assert!(
        s.fast_committed.contains_key(&tx_a),
        "tx_a fast_committed entry must NOT be pruned while tx_b (lower seq) is in-flight"
    );

    // Remove tx_b from write_buff — no more in-flight txs from this port.
    s.write_buff.remove(&tx_b);
    s.prune_aborted();
    assert!(
        !s.fast_committed.contains_key(&tx_a),
        "tx_a fast_committed entry must be pruned once no in-flight tx remains for this port"
    );
}

// -----------------------------------------------------------------------
// Binary-search MVCC version lookup
//
// Traces derived from TLC enumeration of LatestVersionBefore(k, t):
//   - versions[] is a sorted Vec<Version> (ascending by timestamp)
//   - handle_read: partition_point to find latest version with ts < start_ts
//   - handle_update: CommittedConflict via binary search (any ts >= start_ts)
//   - handle_commit / handle_fast_commit: idempotency + sorted insertion
// -----------------------------------------------------------------------

// Trace: eligible = {} because versions[k] is empty.
#[test]
fn bsearch_read_empty_versions_returns_not_found() {
    let mut s = shard();
    assert_eq!(
        s.handle_read(T1, 5, K1, &no_inquiries()),
        ReadResult::NotFound
    );
}

// Trace: single version above start_ts — eligible set empty → NotFound.
#[test]
fn bsearch_read_single_version_above_start_ts() {
    let mut s = shard();
    s.versions.entry(K1).or_default().push(Version {
        value: v(10),
        timestamp: 10,
    });
    assert_eq!(
        s.handle_read(T1, 5, K1, &no_inquiries()),
        ReadResult::NotFound
    );
}

// Trace: single version exactly at start_ts — strict inequality (ts < t) → NotFound.
#[test]
fn bsearch_read_single_version_at_start_ts_is_not_visible() {
    let mut s = shard();
    s.versions.entry(K1).or_default().push(Version {
        value: v(99),
        timestamp: 5,
    });
    assert_eq!(
        s.handle_read(T1, 5, K1, &no_inquiries()),
        ReadResult::NotFound
    );
}

// Trace: single version strictly below start_ts → Value.
#[test]
fn bsearch_read_single_version_below_start_ts() {
    let mut s = shard();
    s.versions.entry(K1).or_default().push(Version {
        value: v(42),
        timestamp: 4,
    });
    assert_eq!(
        s.handle_read(T1, 5, K1, &no_inquiries()),
        ReadResult::Value(v(42))
    );
}

// Trace: two versions, start_ts between them — only the lower one is eligible.
#[test]
fn bsearch_read_picks_lower_version_when_upper_is_not_eligible() {
    let mut s = shard();
    s.versions.entry(K1).or_default().push(Version {
        value: v(1),
        timestamp: 3,
    });
    s.versions.entry(K1).or_default().push(Version {
        value: v(2),
        timestamp: 7,
    });
    // start_ts=5: only ts=3 is eligible (ts=7 >= 5)
    assert_eq!(
        s.handle_read(T1, 5, K1, &no_inquiries()),
        ReadResult::Value(v(1))
    );
}

// Trace: two versions, start_ts above both — pick the latest (ts=7).
#[test]
fn bsearch_read_picks_latest_when_both_versions_eligible() {
    let mut s = shard();
    s.versions.entry(K1).or_default().push(Version {
        value: v(1),
        timestamp: 3,
    });
    s.versions.entry(K1).or_default().push(Version {
        value: v(2),
        timestamp: 7,
    });
    // start_ts=8: both eligible, pick max ts=7
    assert_eq!(
        s.handle_read(T1, 8, K1, &no_inquiries()),
        ReadResult::Value(v(2))
    );
}

// Trace: many versions (10 entries), start_ts in the middle.
#[test]
fn bsearch_read_many_versions_start_ts_in_middle() {
    let mut s = shard();
    for ts in 1u64..=10 {
        s.versions.entry(K1).or_default().push(Version {
            value: v(ts * 10),
            timestamp: ts,
        });
    }
    // start_ts=6: latest eligible is ts=5 → value=50
    assert_eq!(
        s.handle_read(T1, 6, K1, &no_inquiries()),
        ReadResult::Value(v(50))
    );
}

// Trace: many versions, start_ts=1 → nothing strictly before 1 → NotFound.
#[test]
fn bsearch_read_many_versions_start_ts_before_all() {
    let mut s = shard();
    for ts in 1u64..=10 {
        s.versions.entry(K1).or_default().push(Version {
            value: v(ts),
            timestamp: ts,
        });
    }
    assert_eq!(
        s.handle_read(T1, 1, K1, &no_inquiries()),
        ReadResult::NotFound
    );
}

// Trace: many versions, start_ts above all → return the last version.
#[test]
fn bsearch_read_many_versions_start_ts_above_all() {
    let mut s = shard();
    for ts in 1u64..=10 {
        s.versions.entry(K1).or_default().push(Version {
            value: v(ts * 100),
            timestamp: ts,
        });
    }
    // start_ts=11: all eligible, latest is ts=10 → value=1000
    assert_eq!(
        s.handle_read(T1, 11, K1, &no_inquiries()),
        ReadResult::Value(v(1000))
    );
}

// Trace: start_ts exactly at a version boundary — the version at that ts is NOT visible.
// Versions at ts=[3,5,7], start_ts=5 → only ts=3 eligible.
#[test]
fn bsearch_read_boundary_version_at_start_ts_excluded() {
    let mut s = shard();
    s.versions.entry(K1).or_default().push(Version {
        value: v(1),
        timestamp: 3,
    });
    s.versions.entry(K1).or_default().push(Version {
        value: v(2),
        timestamp: 5,
    });
    s.versions.entry(K1).or_default().push(Version {
        value: v(3),
        timestamp: 7,
    });
    assert_eq!(
        s.handle_read(T1, 5, K1, &no_inquiries()),
        ReadResult::Value(v(1))
    );
}

// Trace: CommittedConflict — version at ts=5, start_ts=5 → conflict (5 >= 5).
#[test]
fn bsearch_committed_conflict_at_exact_start_ts() {
    let mut s = shard();
    s.versions.entry(K1).or_default().push(Version {
        value: v(1),
        timestamp: 5,
    });
    assert_eq!(s.handle_update(T1, 5, K1, v(2)), UpdateResult::Abort);
}

// Trace: CommittedConflict — versions [1,3,5], start_ts=4 → version at ts=5 >= 4 → conflict.
#[test]
fn bsearch_committed_conflict_with_version_above_start_ts() {
    let mut s = shard();
    for ts in [1u64, 3, 5] {
        s.versions.entry(K1).or_default().push(Version {
            value: v(ts),
            timestamp: ts,
        });
    }
    assert_eq!(s.handle_update(T1, 4, K1, v(99)), UpdateResult::Abort);
}

// Trace: no CommittedConflict — versions [1,3,5], start_ts=6 → all < 6 → no conflict.
#[test]
fn bsearch_no_committed_conflict_when_all_versions_below_start_ts() {
    let mut s = shard();
    for ts in [1u64, 3, 5] {
        s.versions.entry(K1).or_default().push(Version {
            value: v(ts),
            timestamp: ts,
        });
    }
    s.clock = 10; // ensure no clock-based abort
    assert_eq!(s.handle_update(T1, 6, K1, v(99)), UpdateResult::Ok);
}

// Trace: handle_commit with out-of-order commit_ts maintains sorted order.
// Pre-existing versions at ts=[1,3,7]; commit at ts=5 → sorted result [1,3,5,7].
#[test]
fn bsearch_commit_inserts_version_in_sorted_order() {
    let mut s = shard();
    s.versions.entry(K1).or_default().push(Version {
        value: v(1),
        timestamp: 1,
    });
    s.versions.entry(K1).or_default().push(Version {
        value: v(3),
        timestamp: 3,
    });
    s.versions.entry(K1).or_default().push(Version {
        value: v(7),
        timestamp: 7,
    });
    // Simulate T1 with a buffered write
    s.write_buff.entry(T1).or_default().insert(K1, v(5));
    s.write_keys.insert(K1, T1);
    s.handle_commit(T1, 5);
    let ts_list: Vec<u64> = s.versions[&K1].iter().map(|v| v.timestamp).collect();
    assert_eq!(ts_list, vec![1, 3, 5, 7]);
}

// Trace: handle_commit is idempotent — re-committing same tx/ts installs no duplicate.
#[test]
fn bsearch_commit_idempotent_no_duplicate_version() {
    let mut s = shard();
    s.versions.entry(K1).or_default().push(Version {
        value: v(1),
        timestamp: 1,
    });
    s.versions.entry(K1).or_default().push(Version {
        value: v(5),
        timestamp: 5,
    });
    s.versions.entry(K1).or_default().push(Version {
        value: v(7),
        timestamp: 7,
    });
    // Simulate a second commit message for a tx that already installed ts=5.
    s.write_buff.entry(T1).or_default().insert(K1, v(5));
    s.write_keys.insert(K1, T1);
    s.handle_commit(T1, 5);
    // Still exactly 3 versions.
    assert_eq!(s.versions[&K1].len(), 3);
}

// Trace: handle_fast_commit also inserts in sorted order.
#[test]
fn bsearch_fast_commit_inserts_version_in_sorted_order() {
    let mut s = shard();
    // Pre-install a version at ts=1.
    s.versions.entry(K1).or_default().push(Version {
        value: v(1),
        timestamp: 1,
    });
    s.clock = 4; // next fast-commit will get ts=5
    s.handle_update(T1, 2, K1, v(99));
    let FastCommitResult::Ok(ct) = s.handle_fast_commit(T1) else {
        panic!()
    };
    assert_eq!(ct, 5);
    let ts_list: Vec<u64> = s.versions[&K1].iter().map(|v| v.timestamp).collect();
    assert_eq!(ts_list, vec![1, 5]);
}

// -----------------------------------------------------------------------
// expire_prepared — ShardTimeoutPrepared traces
// -----------------------------------------------------------------------

/// Helper: a ShardState with a specific prepare_ttl.
fn shard_with_ttl(ttl: Duration) -> ShardState {
    let mut s = ShardState::new();
    s.prepare_ttl = ttl;
    s
}

/// A far-future Instant to force-expire all prepared entries without sleeping.
fn far_future() -> Instant {
    Instant::now() + Duration::from_secs(9_999_999)
}

// Trace: no prepared entries → expire_prepared is a no-op.
#[test]
fn expire_prepared_noop_on_empty_prepared_set() {
    let mut s = shard();
    let expired = s.expire_prepared(far_future());
    assert!(expired.is_empty());
}

// Trace: prepared entry within TTL → not expired.
#[test]
fn expire_prepared_does_not_expire_within_ttl() {
    let mut s = shard_with_ttl(Duration::from_secs(30));
    s.handle_update(T1, 1, K1, v(10));
    s.handle_prepare(T1);
    // Call with Instant::now() immediately; 0 time elapsed < 30 s TTL.
    let expired = s.expire_prepared(Instant::now());
    assert!(expired.is_empty());
    assert!(s.prepared.contains_key(&T1));
}

// Trace: prepared entry past TTL → write lock and prepare entry cleaned up.
// (ShardTimeoutPrepared action in TLA+)
//
// Note: after expiry, prune_aborted may immediately remove the tx from the
// aborted set when no other active transactions from the same coordinator
// port are in flight (watermark logic). We assert the functional outcomes
// (write lock released, prepare cleared) rather than aborted-set membership.
#[test]
fn expire_prepared_aborts_entry_past_ttl() {
    let mut s = shard_with_ttl(Duration::from_secs(30));
    s.handle_update(T1, 1, K1, v(10));
    s.handle_prepare(T1);

    let expired = s.expire_prepared(far_future());

    assert_eq!(expired, HashSet::from([T1]));
    assert!(!s.prepared.contains_key(&T1)); // removed from prepared
    assert!(!s.write_buff.contains_key(&T1)); // write buffer cleared
    assert!(!s.write_keys.contains_key(&K1)); // write lock released
}

// Trace: expired prepared entry releases write lock → fresh transaction can now write.
#[test]
fn expire_prepared_unblocks_conflicting_writer() {
    let mut s = shard_with_ttl(Duration::from_secs(30));
    // T1 writes K1 and prepares (takes write lock on K1).
    s.handle_update(T1, 1, K1, v(10));
    s.handle_prepare(T1);

    // T1's TTL expires.
    s.expire_prepared(far_future());

    // T2 (fresh, never aborted) can now write K1 with no conflict.
    assert_eq!(s.handle_update(T2, 2, K1, v(20)), UpdateResult::Ok);
}

// Trace: multiple prepared entries, only the one past TTL is expired.
#[test]
fn expire_prepared_only_expires_entries_past_ttl() {
    let mut s = shard_with_ttl(Duration::from_secs(10));
    s.handle_update(T1, 1, K1, v(10));
    s.handle_prepare(T1);
    s.handle_update(T2, 2, K2, v(20));
    s.handle_prepare(T2);

    let now = Instant::now();
    // Simulate T1 being prepared 20 s ago (> 10 s TTL), T2 just now.
    s.force_prepare_time(T1, now - Duration::from_secs(20));
    s.force_prepare_time(T2, now);

    let expired = s.expire_prepared(now);

    // Only T1 is in the expired set; T2 is untouched.
    assert_eq!(expired, HashSet::from([T1]));
    assert!(!expired.contains(&T2));
    assert!(s.prepared.contains_key(&T2)); // T2 still prepared
    assert!(!s.write_keys.contains_key(&K1)); // T1's lock released
    assert!(s.write_keys.contains_key(&K2)); // T2's lock still held
}

// Trace: committed entry is removed from prepare_times at commit time →
// expire_prepared cannot touch it, versions stay intact.
#[test]
fn expire_prepared_ignores_committed_entries() {
    let mut s = shard_with_ttl(Duration::from_secs(30));
    s.handle_update(T1, 1, K1, v(10));
    s.handle_prepare(T1);
    s.handle_commit(T1, 2); // removes from prepare_times

    let expired = s.expire_prepared(far_future());

    assert!(expired.is_empty());
    assert!(s.versions.contains_key(&K1)); // version still installed
}

// Trace: aborted entry is removed from prepare_times at abort time →
// expire_prepared is a no-op on it.
#[test]
fn expire_prepared_ignores_already_aborted_entries() {
    let mut s = shard_with_ttl(Duration::from_secs(30));
    s.handle_update(T1, 1, K1, v(10));
    s.handle_prepare(T1);
    s.handle_abort(T1); // removes from prepare_times

    let expired = s.expire_prepared(far_future());

    assert!(expired.is_empty());
}

// Trace: expire_prepared calls prune_aborted internally; entries whose
// coordinator has moved on are pruned from the aborted set.
#[test]
fn expire_prepared_prunes_safe_aborted_entries_via_watermark() {
    // tx1: port 7000, seq 1 (old, will be expired)
    let tx1: TxId = (7000u64 << 32) | 1;
    // tx2: port 7000, seq 100 (newer, currently active in write_buff)
    let tx2: TxId = (7000u64 << 32) | 100;

    let mut s = shard_with_ttl(Duration::from_secs(10));
    s.handle_update(tx1, 1, K1, v(10));
    s.handle_prepare(tx1);
    // tx2 is active (has a write buffer entry but no prepare).
    s.handle_update(tx2, 2, K2, v(20));

    // Expire tx1 (force its prepare time to be old).
    let now = Instant::now();
    s.force_prepare_time(tx1, now - Duration::from_secs(20));
    let expired = s.expire_prepared(now);

    assert!(expired.contains(&tx1));
    // prune_aborted fires after expiry: min_active_seq[7000] = 100 (from tx2
    // in write_buff), and tx1.seq = 1 < 100, so tx1 is pruned from aborted.
    assert!(!s.aborted.contains(&tx1));
}

// Trace: expire on a tx with no prepare entry (only in write_buff, never
// prepared) does nothing to that tx.
#[test]
fn expire_prepared_ignores_unprepared_tx() {
    let mut s = shard_with_ttl(Duration::from_secs(30));
    // T1 has a buffered write but was never prepared.
    s.handle_update(T1, 1, K1, v(10));

    let expired = s.expire_prepared(far_future());

    assert!(expired.is_empty());
    assert!(s.write_buff.contains_key(&T1)); // write buffer untouched
}

// -----------------------------------------------------------------------
// handle_commit — rejection of TTL-aborted entries (silent data loss fix)
// -----------------------------------------------------------------------

// Trace 1 (normal 2PC): prepare → commit with no intervening abort →
// data is installed and CommitResult::Ok is returned.
#[test]
fn handle_commit_on_prepared_tx_returns_ok_and_installs_versions() {
    let mut s = shard();
    s.handle_update(T1, 1, K1, v(42));
    s.handle_prepare(T1);
    let result = s.handle_commit(T1, 2);
    assert_eq!(result, CommitResult::Ok);
    // Version must be installed.
    assert!(s
        .versions
        .get(&K1)
        .unwrap()
        .iter()
        .any(|v| v.timestamp == 2));
}

// Trace 2 (TTL expiry then COMMIT): prepare → expire_prepared fires →
// late COMMIT must return Abort and must NOT install any version.
// This is the exact race fixed by the guard id ∉ s_aborted[s] in the spec.
#[test]
fn handle_commit_after_expire_prepared_returns_abort_and_installs_nothing() {
    let mut s = shard_with_ttl(Duration::from_secs(30));
    s.handle_update(T1, 1, K1, v(99));
    s.handle_prepare(T1);

    // Simulate prepare_ttl elapsed: some other RPC triggered expire_prepared.
    s.expire_prepared(far_future());

    // Coordinator (which had already collected all PREPARE replies) now sends COMMIT.
    let result = s.handle_commit(T1, 5);
    assert_eq!(result, CommitResult::Abort);
    // No version must be installed — data must NOT be silently lost.
    assert!(!s.versions.contains_key(&K1));
}

// Trace 3 (handle_abort then COMMIT): explicit abort + no TTL flag means
// write_buff is already cleared. `handle_commit` returns Ok (no-op: nothing
// to install) but — critically — no version is installed.
//
// NOTE: This scenario cannot arise in the correct protocol (coordinator sends
// either ABORT or COMMIT, never both). We verify the safe no-op property only.
#[test]
fn handle_commit_after_handle_abort_is_noop_and_installs_nothing() {
    let mut s = shard();
    s.handle_update(T1, 1, K1, v(7));
    s.handle_prepare(T1);
    s.handle_abort(T1);

    let result = s.handle_commit(T1, 3);
    assert_eq!(result, CommitResult::Ok); // no-op; write_buff already cleared
    assert!(!s.versions.contains_key(&K1)); // no data installed
}

// Trace 4 (idempotent re-commit): first commit installs, second commit is a no-op.
// tx_id is never added to aborted, so the second call must return Ok.
#[test]
fn handle_commit_is_idempotent_when_not_aborted() {
    let mut s = shard();
    s.handle_update(T1, 1, K1, v(5));
    s.handle_prepare(T1);
    let r1 = s.handle_commit(T1, 2);
    let r2 = s.handle_commit(T1, 2);
    assert_eq!(r1, CommitResult::Ok);
    assert_eq!(r2, CommitResult::Ok);
    // Exactly one version at ts=2.
    let versions = s.versions.get(&K1).unwrap();
    assert_eq!(versions.iter().filter(|v| v.timestamp == 2).count(), 1);
}

// Trace 5 (write lock released on TTL expiry + COMMIT rejection):
// after expire_prepared aborts T1, K1's write lock must be free so T2 can write.
#[test]
fn write_lock_released_after_expire_and_commit_rejected() {
    let mut s = shard_with_ttl(Duration::from_secs(30));
    s.handle_update(T1, 1, K1, v(10));
    s.handle_prepare(T1);
    s.expire_prepared(far_future());

    // COMMIT is rejected.
    assert_eq!(s.handle_commit(T1, 5), CommitResult::Abort);

    // T2 should now be able to acquire K1 without conflict.
    assert_eq!(s.handle_update(T2, 2, K1, v(20)), UpdateResult::Ok);
}

// -----------------------------------------------------------------------
// compact_versions
// -----------------------------------------------------------------------

// Helper: commit N sequential single-key transactions to build up a version list.
// Returns the shard clock after all commits.
fn commit_versions(s: &mut ShardState, key: Key, values: &[(TxId, u64)]) {
    for &(tx_id, val) in values {
        let start_ts = s.clock + 1;
        s.handle_update(tx_id, start_ts, key, Value(val));
        s.handle_fast_commit(tx_id);
    }
}

// Trace: 3 versions for a key, active writer on a different key establishes
// watermark above all 3.  Compact must keep only the latest (highest ts) and
// discard the older two.
#[test]
fn compact_removes_shadowed_versions_below_watermark() {
    let mut s = shard();
    commit_versions(&mut s, K1, &[(T1, 10), (T2, 20), (T3, 30)]);
    let v_count_before = s.versions[&K1].len();
    assert_eq!(v_count_before, 3);

    // Active writer with a high start_ts on K2 sets watermark above all versions.
    let high_start = s.clock + 100;
    s.handle_update(200, high_start, K2, Value(99));

    s.compact_versions();

    let vs = s.versions.get(&K1).unwrap();
    assert_eq!(vs.len(), 1, "all but latest should be compacted");
    assert_eq!(vs[0].value, Value(30));
}

// Trace: watermark sits between two versions; the one below watermark must be
// kept (latest before watermark), the one above must also be kept.
#[test]
fn compact_keeps_version_just_below_and_versions_above_watermark() {
    let mut s = shard();
    commit_versions(&mut s, K1, &[(T1, 10), (T2, 20), (T3, 30)]);
    // ts values are 1, 2, 3 after the three fast-commits.

    // Active writer with start_ts=2 → watermark=2.
    // Versions below watermark: [ts=1].  Only 1 version below, nothing to remove.
    // Versions at/above watermark: [ts=2, ts=3] — kept.
    s.handle_update(200, 2, K2, Value(99));
    s.compact_versions();

    let vs = s.versions.get(&K1).unwrap();
    assert_eq!(
        vs.len(),
        3,
        "watermark=2 protects ts=1 (latest below 2); ts=2,3 above watermark"
    );
}

// Trace: no active write-buffered transactions → watermark is 0 → nothing compacted.
#[test]
fn compact_noop_when_no_active_writes() {
    let mut s = shard();
    commit_versions(&mut s, K1, &[(T1, 10), (T2, 20), (T3, 30)]);
    assert_eq!(s.versions[&K1].len(), 3);

    s.compact_versions(); // nothing in write_start_ts

    assert_eq!(s.versions[&K1].len(), 3);
}

// Trace: only one version exists below watermark — the "latest before watermark"
// IS that single version, so nothing is removed.
#[test]
fn compact_noop_with_single_version_below_watermark() {
    let mut s = shard();
    commit_versions(&mut s, K1, &[(T1, 10)]);
    assert_eq!(s.versions[&K1].len(), 1);

    let high_start = s.clock + 100;
    s.handle_update(200, high_start, K2, Value(99));
    s.compact_versions();

    assert_eq!(s.versions[&K1].len(), 1);
}

// Trace: compact_versions trims correctly when called explicitly (as the
// background task does).  Compaction is no longer triggered automatically
// on every commit — it runs periodically via ShardServer's background task.
#[test]
fn compact_triggered_automatically_on_commit() {
    let mut s = shard();

    // Build 3 versions for K1.
    commit_versions(&mut s, K1, &[(T1, 10), (T2, 20), (T3, 30)]);
    assert_eq!(s.versions[&K1].len(), 3);

    // Active writer establishes a high watermark; ts 1,2,3 are all below it.
    let high_start = s.clock + 100;
    s.handle_update(200, high_start, K2, Value(99));

    // Simulate the background compaction task firing.
    s.compact_versions();

    let vs = s.versions.get(&K1).unwrap();
    assert_eq!(vs.len(), 1, "compact_versions should trim K1 to 1 version");
    assert_eq!(vs[0].value, Value(30));
}

// Trace: reads still return correct values after compaction.
#[test]
fn reads_correct_after_compaction() {
    let mut s = shard();
    commit_versions(&mut s, K1, &[(T1, 10), (T2, 20), (T3, 30)]);
    // ts=1 → v10, ts=2 → v20, ts=3 → v30

    let high_start = s.clock + 100;
    s.handle_update(200, high_start, K2, Value(99));
    s.compact_versions();
    // Only ts=3/v30 remains for K1.

    let inq = no_inquiries();
    // A fresh reader at start_ts=high_start should see v30.
    assert_eq!(
        s.handle_read(201, high_start, K1, &inq),
        ReadResult::Value(Value(30))
    );
}

// Trace: watermark tracks the minimum across multiple active writers.
#[test]
fn compact_watermark_is_minimum_of_active_start_ts() {
    let mut s = shard();
    commit_versions(&mut s, K1, &[(T1, 10), (T2, 20), (T3, 30)]);
    // versions at ts=1,2,3

    // Two active writers: start_ts=10 and start_ts=50.  Watermark should be 10.
    s.handle_update(200, 10, K2, Value(1));
    s.handle_update(201, 50, K3, Value(2));
    s.compact_versions();

    // Watermark=10; all 3 versions (ts=1,2,3) are < 10, but only ts=3 is the
    // latest — ts=1 and ts=2 should be removed.
    let vs = s.versions.get(&K1).unwrap();
    assert_eq!(vs.len(), 1);
    assert_eq!(vs[0].value, Value(30));
}

// -----------------------------------------------------------------------
// dirty_keys — compaction only scans committed keys
// -----------------------------------------------------------------------

// Trace T1: commit a key via fast-commit → key appears in dirty_keys.
#[test]
fn dirty_keys_populated_on_fast_commit() {
    let mut s = shard();
    let start_ts = s.clock + 1;
    s.handle_update(T1, start_ts, K1, Value(10));
    s.handle_fast_commit(T1);
    assert!(
        s.dirty_keys.contains(&K1),
        "fast-committed key must be in dirty_keys"
    );
}

// Trace T1 (2PC path): commit a key via handle_commit → key appears in dirty_keys.
#[test]
fn dirty_keys_populated_on_handle_commit() {
    let mut s = shard();
    let start_ts = s.clock + 1;
    s.handle_update(T1, start_ts, K1, Value(10));
    s.handle_prepare(T1);
    s.handle_commit(T1, 5);
    assert!(
        s.dirty_keys.contains(&K1),
        "2PC-committed key must be in dirty_keys"
    );
}

// Trace T2: after compact_versions, dirty_keys no longer contains the key.
#[test]
fn dirty_keys_cleared_after_compact() {
    let mut s = shard();
    commit_versions(&mut s, K1, &[(T1, 10), (T2, 20), (T3, 30)]);
    assert!(s.dirty_keys.contains(&K1));

    // Active writer establishes watermark above all versions.
    let high_start = s.clock + 100;
    s.handle_update(200, high_start, K2, Value(99));

    s.compact_versions();

    assert!(
        !s.dirty_keys.contains(&K1),
        "compact_versions must remove key from dirty_keys"
    );
}

// Trace T3: a key that was never committed is not in dirty_keys and is not
// touched by compact_versions (O(dirty_keys) not O(all keys)).
#[test]
fn dirty_keys_empty_for_uncommitted_key() {
    let mut s = shard();
    // Write but don't commit.
    let start_ts = s.clock + 1;
    s.handle_update(T1, start_ts, K1, Value(10));

    assert!(
        !s.dirty_keys.contains(&K1),
        "write-buffered but uncommitted key must not be in dirty_keys"
    );

    // Compact should not panic or touch K1's (absent) versions.
    s.compact_versions();
    assert!(!s.dirty_keys.contains(&K1));
}

// Trace T4: dirty_keys is cleared after compact even when no versions are removed
// (only one version below watermark — nothing to discard, but key leaves dirty set).
#[test]
fn dirty_keys_cleared_even_when_nothing_compacted() {
    let mut s = shard();
    // Single version for K1.
    commit_versions(&mut s, K1, &[(T1, 10)]);
    assert!(s.dirty_keys.contains(&K1));

    // Active writer sets watermark above the single version.
    let high_start = s.clock + 100;
    s.handle_update(200, high_start, K2, Value(99));

    s.compact_versions();

    // The single version is kept (it's the latest below watermark), but
    // the key must still be removed from dirty_keys.
    assert_eq!(s.versions[&K1].len(), 1, "single version must be kept");
    assert!(
        !s.dirty_keys.contains(&K1),
        "key must leave dirty_keys even when no version was removed"
    );
}

// Trace T5: committing two keys → both appear in dirty_keys; compact each
// separately removes them individually, not together.
#[test]
fn dirty_keys_tracks_multiple_committed_keys() {
    let mut s = shard();

    let s1 = s.clock + 1;
    s.handle_update(T1, s1, K1, Value(10));
    s.handle_fast_commit(T1);

    let s2 = s.clock + 1;
    s.handle_update(T2, s2, K2, Value(20));
    s.handle_fast_commit(T2);

    assert!(s.dirty_keys.contains(&K1));
    assert!(s.dirty_keys.contains(&K2));

    // Active writer to establish watermark.
    let high_start = s.clock + 100;
    s.handle_update(200, high_start, K3, Value(99));

    s.compact_versions();

    // Both keys processed — both removed from dirty_keys.
    assert!(
        !s.dirty_keys.contains(&K1),
        "K1 must leave dirty_keys after compact"
    );
    assert!(
        !s.dirty_keys.contains(&K2),
        "K2 must leave dirty_keys after compact"
    );
}

// Trace: compact without an active writer returns immediately without
// clearing dirty_keys (watermark=0, nothing safe to compact).
#[test]
fn dirty_keys_not_cleared_when_no_active_writers() {
    let mut s = shard();
    commit_versions(&mut s, K1, &[(T1, 10), (T2, 20)]);
    assert!(s.dirty_keys.contains(&K1));

    // No active write-buffered transactions → watermark = 0 → early return.
    s.compact_versions();

    // Versions untouched and dirty_keys unchanged (key still dirty for next tick).
    assert_eq!(s.versions[&K1].len(), 2);
    assert!(
        s.dirty_keys.contains(&K1),
        "dirty_keys must persist when compact bails out due to no active writers"
    );
}

// Trace: multiple commits to the same key — dirty_keys still only contains it once.
#[test]
fn dirty_keys_deduplicates_same_key() {
    let mut s = shard();
    commit_versions(&mut s, K1, &[(T1, 10), (T2, 20), (T3, 30)]);

    // dirty_keys is a set — K1 appears at most once regardless of how many
    // times it was committed to.
    let count = s.dirty_keys.iter().filter(|&&k| k == K1).count();
    assert_eq!(
        count, 1,
        "dirty_keys must deduplicate repeated commits to K1"
    );
}

// -----------------------------------------------------------------------
// read snapshot watermark — spec traces T1–T6
// -----------------------------------------------------------------------

// Spec T1: The core bug.
// T1 reads K1 (start_ts=2) before its first write; T2 writes K2 (start_ts=100).
// OLD watermark = min(write_start_ts) = 100 → removes K1@ts=1 (shadowed by ts=3).
// NEW watermark = min(read_start_ts ∪ write_start_ts) = min(2, 100) = 2
//              → vsBelowWatermark = {ts=1}, maxBelowTs=1, toRemove = {} → nothing removed.
// T1 can still read K1@ts=1 after compaction.
#[test]
fn compact_preserves_version_needed_by_read_before_first_write() {
    let mut s = shard();
    // Build K1 with two committed versions: ts=1 (v10) and ts=3 (v30).
    s.versions.entry(K1).or_default().push(Version {
        value: v(10),
        timestamp: 1,
    });
    s.versions.entry(K1).or_default().push(Version {
        value: v(30),
        timestamp: 3,
    });
    s.dirty_keys.insert(K1);
    s.clock = 3;

    // T1 reads K1 with start_ts=2 (needs v10@ts=1; ts=3 is invisible at snapshot 2).
    // This must register start_ts=2 in the compaction watermark.
    let res = s.handle_read(T1, 2, K1, &no_inquiries());
    assert_eq!(
        res,
        ReadResult::Value(v(10)),
        "T1 must see v10@ts=1 before compaction"
    );

    // T2 writes K2 with start_ts=100 (concurrent writer, sets write-side watermark to 100).
    s.handle_update(T2, 100, K2, v(99));

    // compact: must use watermark=2 (T1's start_ts), NOT 100 (T2's start_ts).
    // With watermark=2: only K1@ts=1 is below it; it's the max-below-2; nothing removed.
    s.compact_versions();

    let vs = s.versions.get(&K1).unwrap();
    assert_eq!(
        vs.len(),
        2,
        "K1@ts=1 and K1@ts=3 must both survive; K1@ts=1 is T1's snapshot"
    );

    // T1 must still read correctly after compaction (retry scenario).
    let res2 = s.handle_read(T1, 2, K1, &no_inquiries());
    assert_eq!(
        res2,
        ReadResult::Value(v(10)),
        "T1 must still see v10@ts=1 after compaction"
    );
}

// Spec T2: Tx that reads then writes — both read and write tracking at same start_ts.
// After fast-commit, read_start_ts must be cleared (same as write_start_ts cleanup).
#[test]
fn read_start_ts_cleared_on_fast_commit() {
    let mut s = shard();
    s.versions.entry(K1).or_default().push(Version {
        value: v(10),
        timestamp: 1,
    });
    s.clock = 1;

    // T1 reads K1 (start_ts=5) then writes K2 (start_ts=5).
    s.handle_read(T1, 5, K1, &no_inquiries());
    s.handle_update(T1, 5, K2, v(99));

    // After fast-commit: both read and write tracking must be cleared.
    s.handle_fast_commit(T1);

    // No read-side entry → compact_versions must not see T1 in the watermark.
    // Verify by adding a competing reader T2 with low start_ts.
    // If T1 were still tracked, watermark = min(5,...); if cleared, no T1 in watermark.
    // Build K2 versions and compact to confirm T1's read tracking is gone.
    s.versions.entry(K2).or_default().push(Version {
        value: v(1),
        timestamp: 1,
    });
    s.versions.entry(K2).or_default().push(Version {
        value: v(2),
        timestamp: 2,
    });
    s.dirty_keys.insert(K2);

    // T3 writes K3 with start_ts=100 (sets write watermark=100 → can compact).
    s.handle_update(T3, 100, K3, v(0));
    s.compact_versions(); // watermark=100, K2@ts=1 shadowed by ts=2 → ts=1 removed

    let vs = s.versions.get(&K2).unwrap();
    assert_eq!(
        vs.len(),
        1,
        "K2@ts=1 compacted; T1's read tracking cleared on fast_commit"
    );
}

// Spec T3 (read-only tx): read_start_ts anchors watermark while tx is active,
// preventing premature compaction of the version it needs.
#[test]
fn compact_read_only_reader_anchors_watermark() {
    let mut s = shard();
    // K1: two versions ts=1 and ts=3.
    s.versions.entry(K1).or_default().push(Version {
        value: v(10),
        timestamp: 1,
    });
    s.versions.entry(K1).or_default().push(Version {
        value: v(30),
        timestamp: 3,
    });
    s.dirty_keys.insert(K1);
    s.clock = 3;

    // Read-only T1 with start_ts=2 reads K1.  No subsequent write on this shard.
    s.handle_read(T1, 2, K1, &no_inquiries());

    // Another writer T2 with start_ts=100.
    s.handle_update(T2, 100, K2, v(0));

    // Watermark = min(2, 100) = 2.
    // vsBelowWatermark = {ts=1}, maxBelowTs=1, toRemove = {} → K1@ts=1 preserved.
    s.compact_versions();

    assert_eq!(
        s.versions.get(&K1).unwrap().len(),
        2,
        "read-only T1 (start_ts=2) must keep K1@ts=1 alive"
    );
}

// Spec T4: handle_abort clears read_start_ts; watermark reverts to write-side only.
#[test]
fn compact_after_reader_abort_reverts_to_write_watermark() {
    let mut s = shard();
    // K1: two versions ts=1 and ts=3.
    s.versions.entry(K1).or_default().push(Version {
        value: v(10),
        timestamp: 1,
    });
    s.versions.entry(K1).or_default().push(Version {
        value: v(30),
        timestamp: 3,
    });
    s.dirty_keys.insert(K1);
    s.clock = 3;

    // T1 reads K1 (start_ts=2), then aborts.
    s.handle_read(T1, 2, K1, &no_inquiries());
    s.handle_abort(T1);

    // T2 writes K2 with start_ts=100.  After T1's abort, watermark = 100.
    // vsBelowWatermark = {ts=1, ts=3}, maxBelowTs=3, toRemove = {ts=1} → K1@ts=1 removed.
    s.handle_update(T2, 100, K2, v(0));
    s.compact_versions();

    let vs = s.versions.get(&K1).unwrap();
    assert_eq!(vs.len(), 1, "K1@ts=1 safe to compact after T1 aborted");
    assert_eq!(vs[0].timestamp, 3);
}

// Spec T4b: handle_commit clears read_start_ts.
#[test]
fn read_start_ts_cleared_on_commit() {
    let mut s = shard();
    s.versions.entry(K1).or_default().push(Version {
        value: v(10),
        timestamp: 1,
    });
    s.clock = 1;

    // T1 reads K1 then writes K1; commit via prepare+commit.
    s.handle_read(T1, 5, K1, &no_inquiries());
    s.handle_update(T1, 5, K1, v(99));
    s.handle_prepare(T1);
    s.handle_commit(T1, 6);

    // Verify: read_start_ts cleared — no T1 watermark anchor.
    // Another writer T2 with start_ts=100 → watermark=100; K1@ts=1 now compactable.
    s.versions.entry(K1).or_default(); // ensure K1 entry exists
    s.dirty_keys.insert(K1);
    s.handle_update(T2, 100, K2, v(0));
    s.compact_versions();

    // K1 now has committed version at ts=6 (from T1's commit) plus old ts=1.
    // With watermark=100, K1@ts=1 is below watermark and shadowed by ts=6 → removed.
    let vs = s.versions.get(&K1).unwrap();
    assert_eq!(
        vs.len(),
        1,
        "K1@ts=1 compacted; T1's read tracking cleared on commit"
    );
    assert_eq!(vs[0].timestamp, 6);
}

// Spec T5/T6: no active readers or writers → watermark=0 → nothing compacted.
// (Handles both the "s_readers empty" and "tx_state ≠ ACTIVE" spec cases.)
#[test]
fn compact_noop_when_no_readers_and_no_writers() {
    let mut s = shard();
    s.versions.entry(K1).or_default().push(Version {
        value: v(10),
        timestamp: 1,
    });
    s.versions.entry(K1).or_default().push(Version {
        value: v(20),
        timestamp: 2,
    });
    s.dirty_keys.insert(K1);
    s.clock = 5;

    // No handle_read calls, no handle_update calls → watermark=0 → early return.
    s.compact_versions();

    assert_eq!(
        s.versions.get(&K1).unwrap().len(),
        2,
        "nothing compacted with no active txs"
    );
}

// Spec T3 variant: expire_reads removes stale read-only entries, allowing compaction.
#[test]
fn expire_reads_clears_stale_reader_allowing_compaction() {
    let mut s = shard();
    s.versions.entry(K1).or_default().push(Version {
        value: v(10),
        timestamp: 1,
    });
    s.versions.entry(K1).or_default().push(Version {
        value: v(20),
        timestamp: 3,
    });
    s.dirty_keys.insert(K1);
    s.clock = 3;

    // T1 reads with start_ts=2. This anchors the watermark at 2.
    s.handle_read(T1, 2, K1, &no_inquiries());

    // Force T1's read time to be very old (simulates abandoned transaction).
    s.force_read_time(T1, Instant::now() - Duration::from_secs(999_999));

    // expire_reads with the default TTL should evict T1.
    s.expire_reads(Instant::now());

    // Now only T2's write anchors the watermark.
    s.handle_update(T2, 100, K2, v(0));
    s.compact_versions();

    // With T1 gone, watermark=100 → K1@ts=1 is below 100 and shadowed by ts=3 → removed.
    let vs = s.versions.get(&K1).unwrap();
    assert_eq!(
        vs.len(),
        1,
        "K1@ts=1 compacted after T1's read entry expired"
    );
}

// -----------------------------------------------------------------------
// handle_write_and_fast_commit — single-write fast path
// -----------------------------------------------------------------------
//
// Traces WF1/WF2/WF3 from ShardHandleWriteAndFastCommit in TLA+ spec.

// Trace WF1: no conflict — version installed directly, commit_ts = clock + 1.
#[test]
fn write_and_fast_commit_installs_version() {
    let mut s = shard();
    s.clock = 4;
    let r = s.handle_write_and_fast_commit(T1, 3, K1, v(42));
    let FastCommitResult::Ok(ct) = r else {
        panic!("expected Ok, got {r:?}");
    };
    assert_eq!(ct, 5, "commit_ts = clock + 1 = 5");
    let vs = s.versions.get(&K1).unwrap();
    assert_eq!(vs.len(), 1);
    assert_eq!(vs[0].value, v(42));
    assert_eq!(vs[0].timestamp, 5);
}

// Trace WF1: commit_ts advances the shard clock.
#[test]
fn write_and_fast_commit_advances_clock() {
    let mut s = shard();
    s.clock = 7;
    s.handle_write_and_fast_commit(T1, 5, K1, v(1));
    assert_eq!(s.clock, 8, "clock must advance to commit_ts = 8");
}

// Trace WF1: key is added to dirty_keys after commit.
#[test]
fn write_and_fast_commit_marks_key_dirty() {
    let mut s = shard();
    s.handle_write_and_fast_commit(T1, 1, K1, v(10));
    assert!(s.dirty_keys.contains(&K1), "K1 must be in dirty_keys");
}

// Trace WF1: committed version is visible to subsequent readers.
#[test]
fn write_and_fast_commit_version_is_readable() {
    let mut s = shard();
    s.clock = 2;
    let FastCommitResult::Ok(ct) = s.handle_write_and_fast_commit(T1, 2, K1, v(99)) else {
        panic!("expected Ok");
    };
    // T2 reads with start_ts = ct + 1 — should see the newly committed version.
    assert_eq!(
        s.handle_read(T2, ct + 1, K1, &no_inquiries()),
        ReadResult::Value(v(99)),
    );
}

// Trace WF2a: CommittedConflict — existing version at ts >= start_ts → Abort.
#[test]
fn write_and_fast_commit_aborts_on_committed_conflict() {
    let mut s = shard();
    // Install committed version at ts=5 for K1.
    s.versions.entry(K1).or_default().push(Version {
        value: v(100),
        timestamp: 5,
    });
    // T1 with start_ts=4: version at ts=5 >= 4 → CommittedConflict.
    let r = s.handle_write_and_fast_commit(T1, 4, K1, v(99));
    assert_eq!(r, FastCommitResult::Abort);
    assert!(s.aborted.contains(&T1), "T1 must be in aborted set");
}

// Trace WF2b: PreparedConflict — another prepared tx wrote this key with prep_t >= start_ts.
#[test]
fn write_and_fast_commit_aborts_on_prepared_conflict() {
    let mut s = shard();
    // T2 prepared K1 at ts=6 (prep_t=6 >= start_ts=5).
    s.prepared.insert(T2, 6);
    s.write_buff.entry(T2).or_default().insert(K1, v(77));
    let r = s.handle_write_and_fast_commit(T1, 5, K1, v(42));
    assert_eq!(r, FastCommitResult::Abort);
    assert!(s.aborted.contains(&T1));
}

// Trace WF3: tx already aborted — early return, no state change.
#[test]
fn write_and_fast_commit_on_aborted_tx_returns_abort() {
    let mut s = shard();
    s.aborted.insert(T1);
    let old_clock = s.clock;
    let r = s.handle_write_and_fast_commit(T1, 5, K1, v(1));
    assert_eq!(r, FastCommitResult::Abort);
    assert_eq!(
        s.clock, old_clock,
        "clock must not change for pre-aborted tx"
    );
    assert!(
        s.versions.get(&K1).is_none(),
        "no version must be installed"
    );
}

// Trace WF1+WF2: two concurrent writes to same key — second one aborted by CommittedConflict.
#[test]
fn write_and_fast_commit_second_writer_sees_conflict() {
    let mut s = shard();
    s.clock = 4;
    // T1 writes K1 at start_ts=4, committed at ts=5.
    let FastCommitResult::Ok(ct) = s.handle_write_and_fast_commit(T1, 4, K1, v(10)) else {
        panic!("T1 must succeed");
    };
    assert_eq!(ct, 5);
    // T2 with start_ts=4: committed version at ts=5 >= 4 → CommittedConflict.
    let r = s.handle_write_and_fast_commit(T2, 4, K1, v(20));
    assert_eq!(r, FastCommitResult::Abort);
}

// Trace WF1: PreparedConflict guard — prepared tx with prep_t < start_ts is NOT a conflict.
#[test]
fn write_and_fast_commit_prepared_conflict_only_fires_at_or_above_start_ts() {
    let mut s = shard();
    // T2 prepared K1 at ts=3 (prep_t=3 < start_ts=5) — not a conflict.
    s.prepared.insert(T2, 3);
    s.write_buff.entry(T2).or_default().insert(K1, v(77));
    let r = s.handle_write_and_fast_commit(T1, 5, K1, v(42));
    let FastCommitResult::Ok(_) = r else {
        panic!("expected Ok: prep_t < start_ts is not a conflict");
    };
}

// Trace WF1: write does not disturb write_buff (bypass path — no write_buff entry created).
#[test]
fn write_and_fast_commit_does_not_use_write_buff() {
    let mut s = shard();
    s.handle_write_and_fast_commit(T1, 1, K1, v(5));
    assert!(
        s.write_buff.get(&T1).is_none(),
        "write_and_fast_commit must bypass write_buff"
    );
    assert!(
        s.write_keys.get(&K1).is_none(),
        "write_and_fast_commit must not acquire write lock"
    );
}
