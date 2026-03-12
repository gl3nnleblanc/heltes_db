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
        s.versions.entry(K1).or_default().push(Version { value: v(10), timestamp: 2 });
        s.versions.entry(K1).or_default().push(Version { value: v(20), timestamp: 4 });

        assert_eq!(
            s.handle_read(T1, 5, K1, &no_inquiries()),
            ReadResult::Value(v(20)),
        );
    }

    #[test]
    fn read_does_not_see_version_exactly_at_start_ts() {
        // Version at t=5, read with start_ts=5 — NOT visible (strictly less than).
        let mut s = shard();
        s.versions.entry(K1).or_default().push(Version { value: v(99), timestamp: 5 });

        assert_eq!(
            s.handle_read(T1, 5, K1, &no_inquiries()),
            ReadResult::NotFound,
        );
    }

    #[test]
    fn read_does_not_see_version_after_start_ts() {
        let mut s = shard();
        s.versions.entry(K1).or_default().push(Version { value: v(99), timestamp: 10 });

        assert_eq!(
            s.handle_read(T1, 5, K1, &no_inquiries()),
            ReadResult::NotFound,
        );
    }

    #[test]
    fn read_picks_latest_of_multiple_eligible_versions() {
        let mut s = shard();
        s.versions.entry(K1).or_default().push(Version { value: v(1), timestamp: 1 });
        s.versions.entry(K1).or_default().push(Version { value: v(2), timestamp: 2 });
        s.versions.entry(K1).or_default().push(Version { value: v(3), timestamp: 3 });
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
        s.versions.entry(K1).or_default().push(Version { value: v(10), timestamp: 2 });
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

        assert_eq!(
            s.handle_read(T1, 5, K1, &inq),
            ReadResult::NotFound,
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
        s.versions.entry(K1).or_default().push(Version { value: v(77), timestamp: 4 });
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
        assert!(!s.write_buff.get(&T1).map(|b| b.contains_key(&K1)).unwrap_or(false));
    }

    // -----------------------------------------------------------------------
    // handle_update — write-write conflict: committed versions
    // -----------------------------------------------------------------------

    #[test]
    fn update_no_conflict_when_committed_version_strictly_before_start_ts() {
        // Committed version at t=4, start_ts=5 → t < start_ts → no conflict.
        let mut s = shard();
        s.versions.entry(K1).or_default().push(Version { value: v(10), timestamp: 4 });

        assert_eq!(s.handle_update(T1, 5, K1, v(99)), UpdateResult::Ok);
    }

    #[test]
    fn update_conflict_when_committed_version_at_start_ts() {
        // Committed version at t=5 == start_ts=5 → conflict → ABORT.
        let mut s = shard();
        s.versions.entry(K1).or_default().push(Version { value: v(10), timestamp: 5 });

        assert_eq!(s.handle_update(T1, 5, K1, v(99)), UpdateResult::Abort);
    }

    #[test]
    fn update_conflict_when_committed_version_after_start_ts() {
        // Committed version at t=6 > start_ts=5 → conflict → ABORT.
        let mut s = shard();
        s.versions.entry(K1).or_default().push(Version { value: v(10), timestamp: 6 });

        assert_eq!(s.handle_update(T1, 5, K1, v(99)), UpdateResult::Abort);
    }

    #[test]
    fn update_conflict_marks_tx_aborted() {
        let mut s = shard();
        s.versions.entry(K1).or_default().push(Version { value: v(10), timestamp: 6 });
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
        assert_eq!(s.handle_read(T2, ct + 1, K1, &no_inquiries()), ReadResult::Value(v(11)));
        assert_eq!(s.handle_read(T2, ct + 1, K2, &no_inquiries()), ReadResult::Value(v(22)));
        assert_eq!(s.handle_read(T2, ct + 1, K3, &no_inquiries()), ReadResult::Value(v(33)));
    }

    /// Path: commit, then read at exactly commit_ts is NOT visible (strict <).
    #[test]
    fn path_read_at_exactly_commit_ts_is_not_visible() {
        let mut s = shard();
        s.handle_update(T1, 1, K1, v(55));
        let prep_t = match s.handle_prepare(T1) { PrepareResult::Timestamp(t) => t, _ => panic!() };
        let ct = prep_t + 1;
        s.handle_commit(T1, ct);
        // start_ts == ct: must NOT see the version (and no prior version exists).
        assert_eq!(s.handle_read(T2, ct, K1, &no_inquiries()), ReadResult::NotFound);
    }

    /// Path: commit, then read at commit_ts + 1 IS visible.
    #[test]
    fn path_read_one_past_commit_ts_is_visible() {
        let mut s = shard();
        s.handle_update(T1, 1, K1, v(55));
        let prep_t = match s.handle_prepare(T1) { PrepareResult::Timestamp(t) => t, _ => panic!() };
        let ct = prep_t + 1;
        s.handle_commit(T1, ct);
        assert_eq!(s.handle_read(T2, ct + 1, K1, &no_inquiries()), ReadResult::Value(v(55)));
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
        assert_eq!(s.handle_read(T2, 5, K1, &no_inquiries()), ReadResult::NotFound);
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
    #[test]
    fn path_read_on_aborted_tx_returns_abort() {
        let mut s = shard();
        s.handle_update(T1, 1, K1, v(42));
        s.handle_abort(T1);
        assert_eq!(s.handle_read(T1, 5, K1, &no_inquiries()), ReadResult::Abort);
    }

    /// Path: update → abort → update on same tx returns Abort.
    #[test]
    fn path_update_on_aborted_tx_returns_abort() {
        let mut s = shard();
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

        let t1_prep = match s.handle_prepare(T1) { PrepareResult::Timestamp(t) => t, _ => panic!() };
        let t2_prep = match s.handle_prepare(T2) { PrepareResult::Timestamp(t) => t, _ => panic!() };

        let t1_ct = t1_prep + 1;
        let t2_ct = t2_prep + 1;
        s.handle_commit(T1, t1_ct);
        s.handle_commit(T2, t2_ct);

        let read_ts = t1_ct.max(t2_ct) + 1;
        assert_eq!(s.handle_read(T3, read_ts, K1, &no_inquiries()), ReadResult::Value(v(11)));
        assert_eq!(s.handle_read(T3, read_ts, K2, &no_inquiries()), ReadResult::Value(v(22)));
    }

    /// Path: T1 commits K1; T2 (started after T1 committed) reads K1 then writes K2; commits.
    /// T2 must see T1's write (SI1) but can freely write K2.
    #[test]
    fn path_tx_reads_committed_value_then_writes_different_key() {
        let mut s = shard();
        s.handle_update(T1, 1, K1, v(50));
        let t1_prep = match s.handle_prepare(T1) { PrepareResult::Timestamp(t) => t, _ => panic!() };
        let t1_ct = t1_prep + 1;
        s.handle_commit(T1, t1_ct);

        // T2 starts after T1 committed.
        let t2_start = t1_ct + 1;
        assert_eq!(
            s.handle_read(T2, t2_start, K1, &no_inquiries()),
            ReadResult::Value(v(50)),
        );
        assert_eq!(s.handle_update(T2, t2_start, K2, v(99)), UpdateResult::Ok);
        let t2_prep = match s.handle_prepare(T2) { PrepareResult::Timestamp(t) => t, _ => panic!() };
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
        s.versions.entry(K1).or_default().push(Version { value: v(1), timestamp: 2 });
        // Version v(2) at t=5.
        s.versions.entry(K1).or_default().push(Version { value: v(2), timestamp: 5 });
        // Version v(3) at t=9.
        s.versions.entry(K1).or_default().push(Version { value: v(3), timestamp: 9 });

        assert_eq!(s.handle_read(T1, 1,  K1, &no_inquiries()), ReadResult::NotFound);
        assert_eq!(s.handle_read(T1, 2,  K1, &no_inquiries()), ReadResult::NotFound);
        assert_eq!(s.handle_read(T1, 3,  K1, &no_inquiries()), ReadResult::Value(v(1)));
        assert_eq!(s.handle_read(T1, 5,  K1, &no_inquiries()), ReadResult::Value(v(1)));
        assert_eq!(s.handle_read(T1, 6,  K1, &no_inquiries()), ReadResult::Value(v(2)));
        assert_eq!(s.handle_read(T1, 9,  K1, &no_inquiries()), ReadResult::Value(v(2)));
        assert_eq!(s.handle_read(T1, 10, K1, &no_inquiries()), ReadResult::Value(v(3)));
    }

    /// Path: T2 takes a snapshot at ts=3; T1 commits K1 at ts=5 after T2 has started.
    /// T2's snapshot must never see T1's write regardless of when T2 reads.
    #[test]
    fn path_snapshot_not_polluted_by_later_commit() {
        let mut s = shard();
        // T2 has a snapshot at ts=3.
        // Simulate T1 committing K1 at ts=5 by installing the version directly.
        s.versions.entry(K1).or_default().push(Version { value: v(99), timestamp: 5 });
        // T2 (start_ts=3) reads K1 — must not see v(99) committed at ts=5; no prior version.
        assert_eq!(s.handle_read(T2, 3, K1, &no_inquiries()), ReadResult::NotFound);
    }

    /// Path: T2 snapshot at ts=5 sees commit at ts=4 but not at ts=5.
    #[test]
    fn path_snapshot_sees_version_committed_one_before_start_ts() {
        let mut s = shard();
        s.versions.entry(K1).or_default().push(Version { value: v(7), timestamp: 4 });
        s.versions.entry(K1).or_default().push(Version { value: v(8), timestamp: 5 });
        // start_ts=5: sees t=4 but not t=5 (strict <).
        assert_eq!(s.handle_read(T2, 5, K1, &no_inquiries()), ReadResult::Value(v(7)));
    }

    /// Path: T1 and T2 both read the same key at the same snapshot; they agree.
    #[test]
    fn path_two_txs_same_snapshot_read_agree() {
        let mut s = shard();
        s.versions.entry(K1).or_default().push(Version { value: v(42), timestamp: 3 });
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
        assert_eq!(s.handle_read(T1, 1, K1, &no_inquiries()), ReadResult::Value(v(77)));
    }

    /// Path: update K1 twice → read K1 → see second write.
    #[test]
    fn path_read_own_latest_write() {
        let mut s = shard();
        s.handle_update(T1, 1, K1, v(10));
        s.handle_update(T1, 1, K1, v(20));
        assert_eq!(s.handle_read(T1, 1, K1, &no_inquiries()), ReadResult::Value(v(20)));
    }

    /// Path: update K1 → read K2 → see committed K2 value (not own write to K1).
    #[test]
    fn path_own_write_does_not_affect_other_keys() {
        let mut s = shard();
        s.versions.entry(K2).or_default().push(Version { value: v(55), timestamp: 1 });
        s.handle_update(T1, 2, K1, v(99));
        assert_eq!(s.handle_read(T1, 2, K2, &no_inquiries()), ReadResult::Value(v(55)));
    }

    /// Path: own-write shadows an older committed version of the same key.
    #[test]
    fn path_own_write_shadows_committed_version_of_same_key() {
        let mut s = shard();
        s.versions.entry(K1).or_default().push(Version { value: v(10), timestamp: 1 });
        s.handle_update(T1, 5, K1, v(99));
        // T1's buffered write (99) must shadow the committed v(10).
        assert_eq!(s.handle_read(T1, 5, K1, &no_inquiries()), ReadResult::Value(v(99)));
    }

    // -------------------------------------------------------------------
    // Path family 7: Full INQUIRE protocol sequences
    // -------------------------------------------------------------------

    /// Path: T1 prepared; T2 reads same key → NeedsInquiry → inquiry=Active → sees old value.
    #[test]
    fn path_inquire_active_read_sees_pre_prepare_version() {
        let mut s = shard();
        s.versions.entry(K1).or_default().push(Version { value: v(3), timestamp: 2 });
        // T1 must start after the committed version at ts=2 to avoid CommittedConflict.
        s.handle_update(T1, 3, K1, v(77));
        let t1_prep = match s.handle_prepare(T1) { PrepareResult::Timestamp(t) => t, _ => panic!() };
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
        let t1_prep = match s.handle_prepare(T1) { PrepareResult::Timestamp(t) => t, _ => panic!() };
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
        let t1_prep = match s.handle_prepare(T1) { PrepareResult::Timestamp(t) => t, _ => panic!() };
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
        let p1 = match s.handle_prepare(T1) { PrepareResult::Timestamp(t) => t, _ => panic!() };
        s.handle_commit(T1, p1 + 1);

        s.handle_update(T2, p1 + 2, K1, v(2));
        let p2 = match s.handle_prepare(T2) { PrepareResult::Timestamp(t) => t, _ => panic!() };
        s.handle_commit(T2, p2 + 1);

        // T3 reads at the latest timestamp — must see v(2).
        let latest = p2 + 2;
        assert_eq!(s.handle_read(T3, latest, K1, &no_inquiries()), ReadResult::Value(v(2)));
    }

    /// Path: T2 reads K1 at a snapshot between T1 and T2's own commit — sees T1's value.
    #[test]
    fn path_mid_chain_snapshot_sees_intermediate_value() {
        let mut s = shard();
        s.handle_update(T1, 1, K1, v(1));
        let p1 = match s.handle_prepare(T1) { PrepareResult::Timestamp(t) => t, _ => panic!() };
        let c1 = p1 + 1;
        s.handle_commit(T1, c1);

        s.handle_update(T2, c1 + 1, K1, v(2));
        let p2 = match s.handle_prepare(T2) { PrepareResult::Timestamp(t) => t, _ => panic!() };
        s.handle_commit(T2, p2 + 1);

        // Snapshot between c1 and c2 sees T1's write.
        assert_eq!(s.handle_read(T3, c1 + 1, K1, &no_inquiries()), ReadResult::Value(v(1)));
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
        let p1 = match s.handle_prepare(T1) { PrepareResult::Timestamp(t) => t, _ => panic!() };
        let p2 = match s.handle_prepare(T2) { PrepareResult::Timestamp(t) => t, _ => panic!() };
        assert!(p2 > p1, "p2={p2} should be > p1={p1}");
    }

    /// Path: update with ts=100 → prepare gets timestamp > 100.
    #[test]
    fn path_prepare_ts_reflects_clock_after_high_ts_update() {
        let mut s = shard();
        s.handle_update(T1, 100, K1, v(1));
        let prep_t = match s.handle_prepare(T1) { PrepareResult::Timestamp(t) => t, _ => panic!() };
        assert!(prep_t > 100, "prep_t={prep_t} should reflect that clock was advanced to 100");
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
        let prep_t = match s.handle_prepare(T1) { PrepareResult::Timestamp(t) => t, _ => panic!() };
        assert!(s.prepared.contains_key(&T1));
        s.handle_commit(T1, prep_t + 1);
        assert!(!s.prepared.contains_key(&T1));
    }

    /// Path: T1 commits → T2 can prepare the same key without prepared conflict.
    #[test]
    fn path_commit_enables_subsequent_prepare() {
        let mut s = shard();
        s.handle_update(T1, 1, K1, v(10));
        let p1 = match s.handle_prepare(T1) { PrepareResult::Timestamp(t) => t, _ => panic!() };
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
        assert_eq!(s.handle_read(T1, 1, K1, &no_inquiries()), ReadResult::NotFound);
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
        assert_eq!(s.handle_read(T1, 0, K1, &no_inquiries()), ReadResult::NotFound);
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
        assert!(s.aborted.contains(&T1));
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
        let p = match s.handle_prepare(T1) { PrepareResult::Timestamp(t) => t, _ => panic!() };
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
        let p = match s.handle_prepare(T1) { PrepareResult::Timestamp(t) => t, _ => panic!() };
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
            assert_eq!(s.handle_update(id, start, K1, v(id as u64)), UpdateResult::Ok);
            let prep = match s.handle_prepare(id) { PrepareResult::Timestamp(t) => t, _ => panic!() };
            let ct = prep + 1;
            s.handle_commit(id, ct);
            // Verify the new version has a timestamp distinct from all prior ones.
            let ts_set: std::collections::HashSet<_> = s.versions[&K1].iter().map(|v| v.timestamp).collect();
            assert_eq!(ts_set.len(), s.versions[&K1].len(), "duplicate timestamps after committing tx {id}");
            last_ct = ct;
        }
    }
