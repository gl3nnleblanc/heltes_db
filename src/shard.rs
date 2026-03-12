use std::collections::{HashMap, HashSet};

// ---------------------------------------------------------------------------
// Core types
// ---------------------------------------------------------------------------

pub type TxId = u64;
pub type Key = u64;
pub type Timestamp = u64;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Value(pub u64);

/// A single MVCC version of a key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Version {
    pub value: Value,
    pub timestamp: Timestamp,
}

// ---------------------------------------------------------------------------
// Message result types
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq)]
pub enum ReadResult {
    /// Returned the value visible at start_ts.
    Value(Value),
    /// Transaction was already aborted.
    Abort,
    /// One or more prepared writers of this key have prep_t < start_ts and
    /// their status is unknown. The caller must resolve these via INQUIRE and
    /// call handle_read again with the results populated.
    NeedsInquiry(Vec<TxId>),
}

#[derive(Debug, PartialEq, Eq)]
pub enum UpdateResult {
    Ok,
    /// Write-write conflict detected, or transaction already aborted.
    Abort,
}

#[derive(Debug, PartialEq, Eq)]
pub enum PrepareResult {
    /// Prepare timestamp assigned by this shard.
    Timestamp(Timestamp),
    /// Transaction was already aborted.
    Abort,
}

/// The coordinator's answer to an INQUIRE for a prepared transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InquiryStatus {
    /// Transaction committed at this timestamp.
    Committed(Timestamp),
    /// Transaction is still active (not yet committed).
    Active,
}

// ---------------------------------------------------------------------------
// Shard state
// ---------------------------------------------------------------------------

pub struct ShardState {
    /// Logical clock (s_clock in TLA+).
    pub clock: Timestamp,
    /// MVCC version history per key. Each vec is sorted ascending by timestamp.
    /// Invariant: every key that exists has at least the initial version at t=0.
    pub versions: HashMap<Key, Vec<Version>>,
    /// Buffered (uncommitted) writes per transaction.
    pub write_buff: HashMap<TxId, HashMap<Key, Value>>,
    /// Prepared transactions and their prepare timestamps.
    pub prepared: HashMap<TxId, Timestamp>,
    /// Transactions that have been aborted at this shard.
    pub aborted: HashSet<TxId>,
}

impl ShardState {
    /// Create a new shard. All provided keys are initialised with `init_val`
    /// at timestamp 0 (matching TLA+ `versions = [k \in Keys |-> {<<InitVal, 0>>}]`).
    pub fn new(keys: impl IntoIterator<Item = Key>, init_val: Value) -> Self {
        let versions = keys
            .into_iter()
            .map(|k| {
                (
                    k,
                    vec![Version {
                        value: init_val.clone(),
                        timestamp: 0,
                    }],
                )
            })
            .collect();

        ShardState {
            clock: 0,
            versions,
            write_buff: HashMap::new(),
            prepared: HashMap::new(),
            aborted: HashSet::new(),
        }
    }

    /// Handle READ_KEY(id, start_ts, key).
    ///
    /// `inquiry_results` supplies coordinator responses for any prepared
    /// writers that have already been inquired about. If a prepared writer of
    /// `key` with prep_t < start_ts is absent from this map, the shard cannot
    /// yet determine visibility and returns `NeedsInquiry`.
    ///
    /// If a prepared writer is `Committed(ct)` with ct < start_ts, its version
    /// must already be installed in `self.versions` (i.e., handle_commit must
    /// have been called) before the read can proceed; otherwise `NeedsInquiry`
    /// is returned for that writer too.
    pub fn handle_read(
        &mut self,
        tx_id: TxId,
        start_ts: Timestamp,
        key: Key,
        inquiry_results: &HashMap<TxId, InquiryStatus>,
    ) -> ReadResult {
        unimplemented!()
    }

    /// Handle UPDATE_KEY(id, start_ts, key, value).
    ///
    /// Detects write-write conflicts:
    ///   - Abort if any committed version of `key` has timestamp >= start_ts.
    ///   - Abort if any prepared transaction that wrote `key` has prep_t >= start_ts.
    /// Otherwise, buffer the write (overwriting any prior write to `key` by this tx).
    pub fn handle_update(
        &mut self,
        tx_id: TxId,
        start_ts: Timestamp,
        key: Key,
        value: Value,
    ) -> UpdateResult {
        unimplemented!()
    }

    /// Handle PREPARE(id).
    ///
    /// Assigns a prepare timestamp = clock + 1, advances clock, records
    /// (tx_id, prep_t) in `self.prepared`.
    pub fn handle_prepare(&mut self, tx_id: TxId) -> PrepareResult {
        unimplemented!()
    }

    /// Handle COMMIT(id, commit_ts).
    ///
    /// Installs all buffered writes for `tx_id` as new MVCC versions at
    /// `commit_ts`, clears the write buffer, and removes the tx from `prepared`.
    pub fn handle_commit(&mut self, tx_id: TxId, commit_ts: Timestamp) {
        unimplemented!()
    }

    /// Handle ABORT(id).
    ///
    /// Marks `tx_id` as aborted, clears its write buffer, and removes it from
    /// `prepared`.
    pub fn handle_abort(&mut self, tx_id: TxId) {
        unimplemented!()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
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

    /// Shard owning K1, K2, K3 with initial value v(0).
    fn shard() -> ShardState {
        ShardState::new([K1, K2, K3], v(0))
    }

    fn no_inquiries() -> HashMap<TxId, InquiryStatus> {
        HashMap::new()
    }

    // -----------------------------------------------------------------------
    // handle_read — basic MVCC visibility
    // -----------------------------------------------------------------------

    #[test]
    fn read_returns_init_val_when_no_committed_writes() {
        let mut s = shard();
        assert_eq!(
            s.handle_read(T1, 5, K1, &no_inquiries()),
            ReadResult::Value(v(0)),
        );
    }

    #[test]
    fn read_returns_latest_version_strictly_before_start_ts() {
        // Manually install two committed versions: v(10) at t=2, v(20) at t=4.
        // A read with start_ts=5 should see v(20) (latest strictly before 5).
        let mut s = shard();
        s.versions.get_mut(&K1).unwrap().push(Version { value: v(10), timestamp: 2 });
        s.versions.get_mut(&K1).unwrap().push(Version { value: v(20), timestamp: 4 });

        assert_eq!(
            s.handle_read(T1, 5, K1, &no_inquiries()),
            ReadResult::Value(v(20)),
        );
    }

    #[test]
    fn read_does_not_see_version_exactly_at_start_ts() {
        // Version at t=5, read with start_ts=5 — NOT visible (strictly less than).
        let mut s = shard();
        s.versions.get_mut(&K1).unwrap().push(Version { value: v(99), timestamp: 5 });

        assert_eq!(
            s.handle_read(T1, 5, K1, &no_inquiries()),
            ReadResult::Value(v(0)),
        );
    }

    #[test]
    fn read_does_not_see_version_after_start_ts() {
        let mut s = shard();
        s.versions.get_mut(&K1).unwrap().push(Version { value: v(99), timestamp: 10 });

        assert_eq!(
            s.handle_read(T1, 5, K1, &no_inquiries()),
            ReadResult::Value(v(0)),
        );
    }

    #[test]
    fn read_picks_latest_of_multiple_eligible_versions() {
        let mut s = shard();
        s.versions.get_mut(&K1).unwrap().push(Version { value: v(1), timestamp: 1 });
        s.versions.get_mut(&K1).unwrap().push(Version { value: v(2), timestamp: 2 });
        s.versions.get_mut(&K1).unwrap().push(Version { value: v(3), timestamp: 3 });
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
        s.versions.get_mut(&K1).unwrap().push(Version { value: v(10), timestamp: 2 });
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
        assert_eq!(
            s.handle_read(T1, 5, K1, &no_inquiries()),
            ReadResult::Abort,
        );
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

        // Should NOT need inquiry — returns a value directly.
        let result = s.handle_read(T1, 5, K1, &no_inquiries());
        assert!(matches!(result, ReadResult::Value(_)));
    }

    #[test]
    fn read_does_not_need_inquiry_for_prepared_writer_with_prep_ts_after_start_ts() {
        let mut s = shard();
        s.prepared.insert(T2, 10);
        s.write_buff.entry(T2).or_default().insert(K1, v(77));

        let result = s.handle_read(T1, 5, K1, &no_inquiries());
        assert!(matches!(result, ReadResult::Value(_)));
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

        assert_eq!(
            s.handle_read(T1, 5, K1, &inq),
            ReadResult::Value(v(0)),
        );
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
        s.versions.get_mut(&K1).unwrap().push(Version { value: v(77), timestamp: 4 });
        s.prepared.remove(&T2);

        let mut inq = HashMap::new();
        inq.insert(T2, InquiryStatus::Committed(4));

        assert_eq!(
            s.handle_read(T1, 5, K1, &inq),
            ReadResult::Value(v(77)),
        );
    }

    #[test]
    fn read_ignores_prepared_writer_of_different_key() {
        // T2 has a pending write to K2, not K1. T1 reads K1 — no inquiry needed.
        let mut s = shard();
        s.prepared.insert(T2, 3);
        s.write_buff.entry(T2).or_default().insert(K2, v(77));

        assert_eq!(
            s.handle_read(T1, 5, K1, &no_inquiries()),
            ReadResult::Value(v(0)),
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
        assert!(!s.write_buff.get(&T1).map(|b| b.contains_key(&K1)).unwrap_or(false));
    }

    // -----------------------------------------------------------------------
    // handle_update — write-write conflict: committed versions
    // -----------------------------------------------------------------------

    #[test]
    fn update_no_conflict_when_committed_version_strictly_before_start_ts() {
        // Committed version at t=4, start_ts=5 → t < start_ts → no conflict.
        let mut s = shard();
        s.versions.get_mut(&K1).unwrap().push(Version { value: v(10), timestamp: 4 });

        assert_eq!(s.handle_update(T1, 5, K1, v(99)), UpdateResult::Ok);
    }

    #[test]
    fn update_conflict_when_committed_version_at_start_ts() {
        // Committed version at t=5 == start_ts=5 → conflict → ABORT.
        let mut s = shard();
        s.versions.get_mut(&K1).unwrap().push(Version { value: v(10), timestamp: 5 });

        assert_eq!(s.handle_update(T1, 5, K1, v(99)), UpdateResult::Abort);
    }

    #[test]
    fn update_conflict_when_committed_version_after_start_ts() {
        // Committed version at t=6 > start_ts=5 → conflict → ABORT.
        let mut s = shard();
        s.versions.get_mut(&K1).unwrap().push(Version { value: v(10), timestamp: 6 });

        assert_eq!(s.handle_update(T1, 5, K1, v(99)), UpdateResult::Abort);
    }

    #[test]
    fn update_conflict_marks_tx_aborted() {
        let mut s = shard();
        s.versions.get_mut(&K1).unwrap().push(Version { value: v(10), timestamp: 6 });
        s.handle_update(T1, 5, K1, v(99));
        assert!(s.aborted.contains(&T1));
    }

    // -----------------------------------------------------------------------
    // handle_update — write-write conflict: prepared transactions
    // -----------------------------------------------------------------------

    #[test]
    fn update_no_conflict_when_prepared_writer_prep_ts_strictly_before_start_ts() {
        // prep_t=4 < start_ts=5 → no prepared conflict (committed conflict checked separately).
        let mut s = shard();
        s.prepared.insert(T2, 4);
        s.write_buff.entry(T2).or_default().insert(K1, v(50));

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
        assert!(versions.iter().any(|ver| ver.value == v(42) && ver.timestamp == 6));
    }

    #[test]
    fn commit_installs_versions_for_all_written_keys() {
        let mut s = shard();
        s.write_buff.entry(T1).or_default().insert(K1, v(1));
        s.write_buff.entry(T1).or_default().insert(K2, v(2));
        s.prepared.insert(T1, 5);

        s.handle_commit(T1, 6);

        assert!(s.versions[&K1].iter().any(|ver| ver.value == v(1) && ver.timestamp == 6));
        assert!(s.versions[&K2].iter().any(|ver| ver.value == v(2) && ver.timestamp == 6));
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
    /// T1: start_ts=1, writes K1, prepares (prep_t=2), commits (ct=3).
    /// T2: start_ts=1, writes K1 → conflict because committed version ct=3 >= start_ts=1.
    #[test]
    fn si4_concurrent_writers_to_same_key_second_is_aborted() {
        let mut s = shard();

        // T1 succeeds.
        assert_eq!(s.handle_update(T1, 1, K1, v(10)), UpdateResult::Ok);
        assert_eq!(s.handle_prepare(T1), PrepareResult::Timestamp(1));
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

        // T2 starts exactly at ct=3 — should NOT see T1's write.
        assert_eq!(
            s.handle_read(T2, 3, K1, &no_inquiries()),
            ReadResult::Value(v(0)),
        );
    }

    // -----------------------------------------------------------------------
    // Clock monotonicity
    // -----------------------------------------------------------------------

    #[test]
    fn clock_advances_on_successive_prepares() {
        let mut s = shard();
        let t1 = match s.handle_prepare(T1) { PrepareResult::Timestamp(t) => t, _ => panic!() };
        let t2 = match s.handle_prepare(T2) { PrepareResult::Timestamp(t) => t, _ => panic!() };
        let t3 = match s.handle_prepare(T3) { PrepareResult::Timestamp(t) => t, _ => panic!() };
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
}
