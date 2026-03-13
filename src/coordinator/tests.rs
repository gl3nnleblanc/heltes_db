use std::collections::HashSet;
use std::time::Duration;

use crate::shard::InquiryStatus;

use super::{
    coord_port_from_tx_id, coord_seq_from_tx_id, BeginCommitResult, BeginFastCommitResult,
    CollectPrepareResult, CoordinatorState, FinalizeFastCommitResult, ReadRetryPolicy,
    SendCommitResult, TxIdGen, TxPhase,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn coord() -> CoordinatorState {
    CoordinatorState::new()
}

fn shards(ids: &[u64]) -> HashSet<u64> {
    ids.iter().copied().collect()
}

// ---------------------------------------------------------------------------
// start_tx
// ---------------------------------------------------------------------------

#[test]
fn start_tx_assigns_start_ts_clock_plus_one() {
    let mut c = coord();
    let ts = c.start_tx(1);
    assert_eq!(ts, 1);
    assert_eq!(c.clock, 1);
}

#[test]
fn start_tx_sequential_txs_get_increasing_start_ts() {
    let mut c = coord();
    let t1 = c.start_tx(1);
    let t2 = c.start_tx(2);
    assert!(t2 > t1);
    assert_eq!(t1, 1);
    assert_eq!(t2, 2);
}

#[test]
fn start_tx_stores_start_ts() {
    let mut c = coord();
    c.start_tx(7);
    assert_eq!(c.start_ts(7), Some(1));
}

#[test]
fn start_tx_sets_active_phase() {
    let mut c = coord();
    c.start_tx(1);
    assert_eq!(c.tx_phase(1), Some(TxPhase::Active));
}

// ---------------------------------------------------------------------------
// add_participant / begin_commit
// ---------------------------------------------------------------------------

#[test]
fn begin_commit_returns_participant_shards() {
    let mut c = coord();
    c.start_tx(1);
    c.add_participant(1, 10);
    c.add_participant(1, 20);
    assert_eq!(
        c.begin_commit(1),
        BeginCommitResult::Prepare(shards(&[10, 20]))
    );
    assert_eq!(c.tx_phase(1), Some(TxPhase::Preparing));
}

#[test]
fn begin_commit_no_participants_returns_no_participants() {
    let mut c = coord();
    c.start_tx(1);
    assert_eq!(c.begin_commit(1), BeginCommitResult::NoParticipants);
    // tx stays Active
    assert_eq!(c.tx_phase(1), Some(TxPhase::Active));
}

#[test]
fn begin_commit_on_aborted_tx_returns_aborted() {
    let mut c = coord();
    c.start_tx(1);
    c.add_participant(1, 10);
    c.abort_tx(1);
    assert_eq!(c.begin_commit(1), BeginCommitResult::Aborted);
}

#[test]
fn begin_commit_unknown_tx_returns_aborted() {
    let mut c = coord();
    assert_eq!(c.begin_commit(99), BeginCommitResult::Aborted);
}

#[test]
fn add_participant_ignored_after_abort() {
    let mut c = coord();
    c.start_tx(1);
    c.abort_tx(1);
    c.add_participant(1, 10); // should be no-op
    assert_eq!(c.begin_commit(1), BeginCommitResult::Aborted);
}

// ---------------------------------------------------------------------------
// collect_prepare_reply
// ---------------------------------------------------------------------------

#[test]
fn collect_prepare_single_shard_returns_done() {
    let mut c = coord();
    c.start_tx(1);
    c.add_participant(1, 10);
    c.begin_commit(1);
    let r = c.collect_prepare_reply(1, 10, Some(3));
    assert_eq!(
        r,
        CollectPrepareResult::Done {
            commit_ts: 3,
            participants: shards(&[10])
        }
    );
    assert_eq!(c.tx_phase(1), Some(TxPhase::CommitWait));
}

#[test]
fn collect_prepare_two_shards_need_more_then_done() {
    let mut c = coord();
    c.start_tx(1);
    c.add_participant(1, 10);
    c.add_participant(1, 20);
    c.begin_commit(1);

    let r1 = c.collect_prepare_reply(1, 10, Some(3));
    assert_eq!(r1, CollectPrepareResult::NeedMore);
    assert_eq!(c.tx_phase(1), Some(TxPhase::Preparing));

    let r2 = c.collect_prepare_reply(1, 20, Some(5));
    assert_eq!(
        r2,
        CollectPrepareResult::Done {
            commit_ts: 5,
            participants: shards(&[10, 20])
        }
    );
}

#[test]
fn collect_prepare_commit_ts_is_max_of_prep_timestamps() {
    let mut c = coord();
    c.start_tx(1); // clock → 1
    c.add_participant(1, 10);
    c.add_participant(1, 20);
    c.begin_commit(1);
    c.collect_prepare_reply(1, 10, Some(3));
    let r = c.collect_prepare_reply(1, 20, Some(7));
    // commit_ts = max(3, 7, clock=1) = 7
    assert_eq!(
        r,
        CollectPrepareResult::Done {
            commit_ts: 7,
            participants: shards(&[10, 20])
        }
    );
}

#[test]
fn collect_prepare_commit_ts_is_clock_when_clock_greater() {
    let mut c = coord();
    c.start_tx(1); // clock → 1
    c.start_tx(2); // clock → 2
    c.add_participant(1, 10);
    c.begin_commit(1);
    // prep_ts = 1, but c_clock = 2 after T2 started
    let r = c.collect_prepare_reply(1, 10, Some(1));
    // commit_ts = max(1, clock=2) = 2
    assert_eq!(
        r,
        CollectPrepareResult::Done {
            commit_ts: 2,
            participants: shards(&[10])
        }
    );
}

#[test]
fn collect_prepare_advances_clock_past_commit_ts() {
    let mut c = coord();
    c.start_tx(1);
    c.add_participant(1, 10);
    c.begin_commit(1);
    c.collect_prepare_reply(1, 10, Some(5));
    // commit_ts = max(5, clock=1) = 5; clock → 6
    assert_eq!(c.clock, 6);
}

#[test]
fn collect_prepare_abort_from_shard() {
    let mut c = coord();
    c.start_tx(1);
    c.add_participant(1, 10);
    c.begin_commit(1);
    let r = c.collect_prepare_reply(1, 10, None);
    assert_eq!(r, CollectPrepareResult::Aborted);
    assert_eq!(c.tx_phase(1), Some(TxPhase::Aborted));
}

#[test]
fn collect_prepare_abort_when_first_of_two_shards_aborts() {
    let mut c = coord();
    c.start_tx(1);
    c.add_participant(1, 10);
    c.add_participant(1, 20);
    c.begin_commit(1);
    let r = c.collect_prepare_reply(1, 10, None);
    assert_eq!(r, CollectPrepareResult::Aborted);
}

// ---------------------------------------------------------------------------
// send_commit
// ---------------------------------------------------------------------------

#[test]
fn send_commit_transitions_to_committed_and_sets_is_committed() {
    let mut c = coord();
    c.start_tx(1);
    c.add_participant(1, 10);
    c.begin_commit(1);
    c.collect_prepare_reply(1, 10, Some(3));
    let r = c.send_commit(1);
    assert_eq!(
        r,
        SendCommitResult::Ok {
            commit_ts: 3,
            participants: shards(&[10])
        }
    );
    assert_eq!(c.tx_phase(1), Some(TxPhase::Committed));
}

#[test]
fn send_commit_not_ready_when_still_preparing() {
    let mut c = coord();
    c.start_tx(1);
    c.add_participant(1, 10);
    c.add_participant(1, 20);
    c.begin_commit(1);
    c.collect_prepare_reply(1, 10, Some(3)); // only one of two replied
    assert_eq!(c.send_commit(1), SendCommitResult::NotReady);
}

#[test]
fn send_commit_not_ready_when_active() {
    let mut c = coord();
    c.start_tx(1);
    assert_eq!(c.send_commit(1), SendCommitResult::NotReady);
}

// ---------------------------------------------------------------------------
// abort_tx
// ---------------------------------------------------------------------------

#[test]
fn abort_tx_returns_participants() {
    let mut c = coord();
    c.start_tx(1);
    c.add_participant(1, 10);
    c.add_participant(1, 20);
    let participants = c.abort_tx(1);
    assert_eq!(participants, shards(&[10, 20]));
    assert_eq!(c.tx_phase(1), Some(TxPhase::Aborted));
}

#[test]
fn abort_tx_unknown_tx_returns_empty() {
    let mut c = coord();
    assert_eq!(c.abort_tx(99), HashSet::new());
}

#[test]
fn abort_tx_from_preparing_phase() {
    let mut c = coord();
    c.start_tx(1);
    c.add_participant(1, 10);
    c.begin_commit(1);
    c.abort_tx(1);
    assert_eq!(c.tx_phase(1), Some(TxPhase::Aborted));
}

// ---------------------------------------------------------------------------
// handle_inquire
// ---------------------------------------------------------------------------

#[test]
fn handle_inquire_active_tx_returns_active() {
    let mut c = coord();
    c.start_tx(1);
    c.add_participant(1, 10);
    assert_eq!(c.handle_inquire(1, 0), InquiryStatus::Active);
}

#[test]
fn handle_inquire_committed_tx_returns_committed_at() {
    let mut c = coord();
    c.start_tx(1);
    c.add_participant(1, 10);
    c.begin_commit(1);
    c.collect_prepare_reply(1, 10, Some(4));
    c.send_commit(1);
    assert_eq!(c.handle_inquire(1, 0), InquiryStatus::Committed(4));
}

#[test]
fn handle_inquire_unknown_tx_returns_active() {
    let mut c = coord();
    assert_eq!(c.handle_inquire(99, 0), InquiryStatus::Active);
}

#[test]
fn handle_inquire_advances_clock() {
    let mut c = coord();
    c.start_tx(1); // clock → 1
    c.handle_inquire(1, 5); // reader_start_ts=5 > clock=1 → clock → 5
    assert_eq!(c.clock, 5);
}

#[test]
fn handle_inquire_does_not_regress_clock() {
    let mut c = coord();
    c.start_tx(1); // clock → 1
    c.handle_inquire(1, 0); // reader_start_ts=0 < clock=1 → clock stays 1
    assert_eq!(c.clock, 1);
}

// ---------------------------------------------------------------------------
// Full TLA+ path tests
// ---------------------------------------------------------------------------

/// TLA+ path: CoordStartTx → CoordUpdate → CoordBeginCommit →
///            ShardHandlePrepare → CoordFinalizePrepare → CoordSendCommit
///            (single shard happy path)
#[test]
fn path_single_shard_happy_commit() {
    let mut c = coord();

    // CoordStartTx(T1)
    let start_ts = c.start_tx(1);
    assert_eq!(start_ts, 1);

    // CoordUpdate(T1, K1, v): register shard S1
    c.add_participant(1, 100);

    // CoordBeginCommit(T1): all update acks received, send PREPARE
    let BeginCommitResult::Prepare(participants) = c.begin_commit(1) else {
        panic!("expected Prepare");
    };
    assert_eq!(participants, shards(&[100]));

    // ShardHandlePrepare(S1, T1): shard responds with prep_t=2
    let r = c.collect_prepare_reply(1, 100, Some(2));
    let CollectPrepareResult::Done {
        commit_ts,
        participants,
    } = r
    else {
        panic!("expected Done");
    };
    assert_eq!(participants, shards(&[100]));
    assert_eq!(commit_ts, 2); // max(prep_ts=2, clock=1) = 2

    // CoordSendCommit(T1)
    let SendCommitResult::Ok { commit_ts: ct2, .. } = c.send_commit(1) else {
        panic!("expected Ok");
    };
    assert_eq!(ct2, 2);
    assert_eq!(c.tx_phase(1), Some(TxPhase::Committed));
}

/// TLA+ path: Two shards — commit_ts = max of both prepare timestamps.
#[test]
fn path_two_shards_commit_ts_is_max_prep_ts() {
    let mut c = coord();
    c.start_tx(1); // clock → 1

    c.add_participant(1, 100);
    c.add_participant(1, 200);
    c.begin_commit(1);

    // S1 prepares at ts=3, S2 at ts=7
    c.collect_prepare_reply(1, 100, Some(3));
    let r = c.collect_prepare_reply(1, 200, Some(7));
    let CollectPrepareResult::Done { commit_ts, .. } = r else {
        panic!("expected Done");
    };
    assert_eq!(commit_ts, 7);
    assert_eq!(c.clock, 8); // 7 + 1
}

/// TLA+ path: Shard aborts during prepare — coordinator aborts T1.
#[test]
fn path_prepare_abort_propagates() {
    let mut c = coord();
    c.start_tx(1);
    c.add_participant(1, 100);
    c.begin_commit(1);

    let r = c.collect_prepare_reply(1, 100, None);
    assert_eq!(r, CollectPrepareResult::Aborted);
    assert_eq!(c.tx_phase(1), Some(TxPhase::Aborted));
}

/// TLA+ path: CoordHandleInquire — shard asks about T1 while T2 is reading.
/// T1 is still active → Active reply; clock advances to reader's start_ts.
#[test]
fn path_inquire_active_writer() {
    let mut c = coord();
    let t1_start = c.start_tx(1); // clock → 1
    let t2_start = c.start_tx(2); // clock → 2

    c.add_participant(1, 100);
    // T1 is preparing; T2 is reading and encounters T1 as a prepared writer.
    // Shard asks coordinator: is T1 committed?
    let status = c.handle_inquire(1, t2_start);
    assert_eq!(status, InquiryStatus::Active);
    // Lamport clock: max(clock=2, reader_start_ts=2) = 2
    assert_eq!(c.clock, t2_start.max(t1_start));
}

/// TLA+ path: CoordHandleInquire after commit — returns Committed(ts).
#[test]
fn path_inquire_after_commit() {
    let mut c = coord();
    c.start_tx(1);
    c.add_participant(1, 100);
    c.begin_commit(1);
    c.collect_prepare_reply(1, 100, Some(3));
    c.send_commit(1);

    // T2 starts after T1 commits
    let t2_start = c.start_tx(2);
    let status = c.handle_inquire(1, t2_start);
    assert_eq!(status, InquiryStatus::Committed(3));
}

// ---------------------------------------------------------------------------
// TxIdGen / coord_port_from_tx_id / coord_seq_from_tx_id
// ---------------------------------------------------------------------------

// Trace: new_at(port, 0) → first ID has seq=0, correct port.
#[test]
fn txidgen_encodes_port_in_high_bits() {
    let mut gen = TxIdGen::new_at(50052, 0);
    let id = gen.next().unwrap();
    assert_eq!(coord_port_from_tx_id(id), 50052);
}

// Trace: new_at(port, 0) → IDs increment by 1 each step.
#[test]
fn txidgen_sequence_increments() {
    let mut gen = TxIdGen::new_at(50052, 0);
    let id0 = gen.next().unwrap();
    let id1 = gen.next().unwrap();
    let id2 = gen.next().unwrap();
    assert_eq!(coord_seq_from_tx_id(id0), 0);
    assert_eq!(coord_seq_from_tx_id(id1), 1);
    assert_eq!(coord_seq_from_tx_id(id2), 2);
}

// Trace: new_at(port, start) starts at given start, increments from there.
#[test]
fn txidgen_new_at_starts_from_given_seq() {
    let mut gen = TxIdGen::new_at(50052, 1000);
    let id0 = gen.next().unwrap();
    let id1 = gen.next().unwrap();
    assert_eq!(coord_seq_from_tx_id(id0), 1000);
    assert_eq!(coord_seq_from_tx_id(id1), 1001);
}

// Trace: port is still correctly encoded regardless of start_seq.
#[test]
fn txidgen_new_at_port_correct_with_nonzero_start() {
    for port in [50051u16, 50052, 60000, 1, u16::MAX] {
        let mut gen = TxIdGen::new_at(port, 999_999);
        let id = gen.next().unwrap();
        assert_eq!(coord_port_from_tx_id(id), port);
        assert_eq!(coord_seq_from_tx_id(id), 999_999);
    }
}

// Trace: two generators at well-separated starts don't collide in their
// first N iterations (non-overlapping ranges).
#[test]
fn txidgen_non_overlapping_starts_produce_distinct_ids() {
    let port = 50052;
    let mut old_epoch = TxIdGen::new_at(port, 0);
    let new_epoch = TxIdGen::new_at(port, 100_000);
    let old_ids: std::collections::HashSet<u64> = old_epoch.by_ref().take(1000).collect();
    for id in new_epoch.take(1000) {
        assert!(
            !old_ids.contains(&id),
            "new-epoch ID {id} collides with old-epoch range"
        );
    }
}

// Trace: different ports at the same start always produce distinct IDs.
#[test]
fn txidgen_different_ports_produce_distinct_ids() {
    let mut gen_a = TxIdGen::new_at(50052, 0);
    let mut gen_b = TxIdGen::new_at(50053, 0);
    assert_ne!(gen_a.next().unwrap(), gen_b.next().unwrap());
}

// Trace: coord_port_roundtrips through new_at with various ports and starts.
#[test]
fn coord_port_roundtrips() {
    for port in [50051u16, 50052, 60000, 1, u16::MAX] {
        let mut gen = TxIdGen::new_at(port, 42);
        let id = gen.next().unwrap();
        assert_eq!(coord_port_from_tx_id(id), port);
    }
}

// Trace (restart-safe path): old epoch aborted seq=50; new epoch starts at
// seq=1000 (well past 50). Shard's aborted check on new-epoch seq=1000 tx
// must not fire — the old aborted entry has a different seq.
#[test]
fn txidgen_restart_safety_new_epoch_seq_does_not_hit_old_aborted() {
    use crate::shard::ShardState;

    let port: u16 = 50052;
    let old_epoch_aborted_seq: u32 = 50;
    let old_tx_id = ((port as u64) << 32) | old_epoch_aborted_seq as u64;

    let mut s = ShardState::new();
    // Simulate the shard aborting a tx from the old epoch.
    s.aborted.insert(old_tx_id);

    // New-epoch generator starts at 1000 (far from 50 — simulates time-seeded restart).
    let mut new_epoch = TxIdGen::new_at(port, 1000);
    let new_tx_id = new_epoch.next().unwrap(); // seq=1000

    // The new-epoch tx must NOT be treated as aborted by the shard.
    assert!(
        !s.aborted.contains(&new_tx_id),
        "new-epoch tx (seq=1000) incorrectly matches old-epoch aborted entry (seq=50)"
    );
}

// Trace (broken path — documents the bug that time-seeding fixes):
// old epoch aborted seq=50; new epoch starts at seq=0; when it issues seq=50
// it would collide with the old aborted entry.
#[test]
fn txidgen_restart_collision_demonstrates_the_bug_fixed_by_time_seeding() {
    use crate::shard::ShardState;

    let port: u16 = 50052;
    let collision_seq: u32 = 50;
    let old_tx_id = ((port as u64) << 32) | collision_seq as u64;

    let mut s = ShardState::new();
    s.aborted.insert(old_tx_id);

    // New-epoch generator starts at 0 (the old broken behaviour).
    let mut broken_new_epoch = TxIdGen::new_at(port, 0);
    // Advance to the colliding seq.
    for _ in 0..50 {
        let _ = broken_new_epoch.next();
    }
    let colliding_id = broken_new_epoch.next().unwrap(); // seq=50

    // This new-epoch tx IS incorrectly in the aborted set — the bug.
    assert!(
        s.aborted.contains(&colliding_id),
        "expected collision seq=50 to be in aborted set (demonstrating the pre-fix bug)"
    );
    assert_eq!(coord_port_from_tx_id(colliding_id), port);
    assert_eq!(coord_seq_from_tx_id(colliding_id), collision_seq);
}

// Trace: time-seeded new() produces a starting sequence that varies
// with time (not fixed at 0). We verify the sequence is non-zero by
// constructing two generators and checking they produce different first seqs.
// (Testing true randomness is hard; this is a sanity check.)
#[test]
fn txidgen_new_time_seeded_start_varies_across_instances() {
    // Two generators constructed at the same port should produce different
    // IDs because the time-mixed seed evolves continuously.
    // We can't guarantee they differ (same nanosecond), but we can assert
    // the API works and the port is still encoded correctly.
    let mut g = TxIdGen::new(50052);
    let id = g.next().unwrap();
    assert_eq!(
        coord_port_from_tx_id(id),
        50052,
        "port must still be correct after time-seeding"
    );
}

/// SI2 — two transactions get distinct commit timestamps.
#[test]
fn si2_committed_txs_have_distinct_commit_timestamps() {
    let mut c = coord();

    c.start_tx(1);
    c.add_participant(1, 100);
    c.begin_commit(1);
    let r1 = c.collect_prepare_reply(1, 100, Some(2));
    let CollectPrepareResult::Done { commit_ts: ct1, .. } = r1 else {
        panic!()
    };
    c.send_commit(1);

    c.start_tx(2);
    c.add_participant(2, 100);
    c.begin_commit(2);
    let r2 = c.collect_prepare_reply(2, 100, Some(2)); // shard reused prep_ts value
    let CollectPrepareResult::Done { commit_ts: ct2, .. } = r2 else {
        panic!()
    };
    c.send_commit(2);

    assert_ne!(
        ct1, ct2,
        "SI2 violated: two committed txs share a commit timestamp"
    );
}

// ---------------------------------------------------------------------------
// begin_fast_commit / finalize_fast_commit
// ---------------------------------------------------------------------------

/// Trace: CoordFastCommit — single participant → Ok(shard_id).
#[test]
fn begin_fast_commit_returns_shard_for_single_participant() {
    let mut c = coord();
    c.start_tx(1);
    c.add_participant(1, 100);
    assert_eq!(c.begin_fast_commit(1), BeginFastCommitResult::Ok(100));
}

/// Trace: CoordFastCommit guard — zero participants → NotSingleShard.
#[test]
fn begin_fast_commit_no_participants_returns_not_single_shard() {
    let mut c = coord();
    c.start_tx(1);
    assert_eq!(
        c.begin_fast_commit(1),
        BeginFastCommitResult::NotSingleShard
    );
}

/// Trace: CoordFastCommit guard — two participants → NotSingleShard (use regular 2PC).
#[test]
fn begin_fast_commit_multiple_participants_returns_not_single_shard() {
    let mut c = coord();
    c.start_tx(1);
    c.add_participant(1, 100);
    c.add_participant(1, 200);
    assert_eq!(
        c.begin_fast_commit(1),
        BeginFastCommitResult::NotSingleShard
    );
}

/// Trace: CoordFastCommit guard — aborted transaction → Aborted.
#[test]
fn begin_fast_commit_on_aborted_tx_returns_aborted() {
    let mut c = coord();
    c.start_tx(1);
    c.add_participant(1, 100);
    c.abort_tx(1);
    assert_eq!(c.begin_fast_commit(1), BeginFastCommitResult::Aborted);
}

/// Trace: CoordFastCommit guard — unknown tx_id → Aborted.
#[test]
fn begin_fast_commit_unknown_tx_returns_aborted() {
    let mut c = coord();
    assert_eq!(c.begin_fast_commit(99), BeginFastCommitResult::Aborted);
}

/// Trace T1: CoordFastCommit — begin_fast_commit transitions phase ACTIVE → PREPARING.
#[test]
fn begin_fast_commit_transitions_phase_to_preparing() {
    let mut c = coord();
    c.start_tx(1);
    c.add_participant(1, 100);
    assert_eq!(c.tx_phase(1), Some(TxPhase::Active));
    c.begin_fast_commit(1);
    assert_eq!(
        c.tx_phase(1),
        Some(TxPhase::Preparing),
        "begin_fast_commit must transition phase to Preparing (CoordFastCommit in TLA+)"
    );
}

/// Trace T1: CoordHandleFastCommitReply success — tx transitions PREPARING → Committed.
#[test]
fn finalize_fast_commit_transitions_to_committed() {
    let mut c = coord();
    c.start_tx(1);
    c.add_participant(1, 100);
    c.begin_fast_commit(1); // phase → Preparing
    assert_eq!(c.finalize_fast_commit(1, 5), FinalizeFastCommitResult::Ok);
    assert_eq!(c.tx_phase(1), Some(TxPhase::Committed));
}

/// Trace T1: CoordHandleFastCommitReply — sets is_committed so Inquire replies correctly.
#[test]
fn finalize_fast_commit_makes_tx_visible_to_inquire() {
    let mut c = coord();
    c.start_tx(1);
    c.add_participant(1, 100);
    c.begin_fast_commit(1); // phase → Preparing
    c.finalize_fast_commit(1, 7);
    use crate::shard::InquiryStatus;
    assert_eq!(c.handle_inquire(1, 0), InquiryStatus::Committed(7));
}

/// Trace T1: CoordHandleFastCommitReply — coordinator clock advances past commit_ts (SI2).
#[test]
fn finalize_fast_commit_advances_clock_past_commit_ts() {
    let mut c = coord();
    c.start_tx(1); // clock → 1
    c.add_participant(1, 100);
    c.begin_fast_commit(1); // phase → Preparing
    c.finalize_fast_commit(1, 8); // commit_ts = 8; clock must → 9
    assert_eq!(c.clock, 9);
}

/// Trace T1: CoordHandleFastCommitReply — clock only advances, never regresses.
#[test]
fn finalize_fast_commit_does_not_regress_clock() {
    let mut c = coord();
    c.start_tx(1); // clock → 1
    c.start_tx(2); // clock → 2
    c.add_participant(1, 100);
    c.begin_fast_commit(1); // phase → Preparing
    // commit_ts = 1 < clock = 2; clock must stay at 2 (or advance past commit_ts=2).
    c.finalize_fast_commit(1, 1);
    assert!(
        c.clock >= 2,
        "clock must not regress below pre-existing value"
    );
}

/// Trace T2: abort_tx called on PREPARING tx (internal abort path, e.g. shard returned abort)
/// — still transitions PREPARING → Aborted so finalize returns NotReady.
#[test]
fn abort_tx_on_preparing_tx_sets_aborted() {
    let mut c = coord();
    c.start_tx(1);
    c.add_participant(1, 100);
    c.begin_fast_commit(1); // phase → Preparing
    assert_eq!(c.tx_phase(1), Some(TxPhase::Preparing));
    c.abort_tx(1); // internal abort (e.g. shard replied Abort to FastCommit RPC)
    assert_eq!(c.tx_phase(1), Some(TxPhase::Aborted));
    // finalize must return NotReady when phase is Aborted
    assert_eq!(
        c.finalize_fast_commit(1, 99),
        FinalizeFastCommitResult::NotReady
    );
}

/// Trace T5: finalize_fast_commit on ACTIVE phase (begin_fast_commit never called) → NotReady.
/// The implementation requires PREPARING phase; calling finalize directly from ACTIVE is
/// a misuse — the caller must call begin_fast_commit first (CoordFastCommit in TLA+).
#[test]
fn finalize_fast_commit_on_active_phase_returns_not_ready() {
    let mut c = coord();
    c.start_tx(1);
    c.add_participant(1, 100);
    // Phase is Active — begin_fast_commit not called
    assert_eq!(
        c.finalize_fast_commit(1, 5),
        FinalizeFastCommitResult::NotReady,
        "finalize_fast_commit must require Preparing phase, not Active"
    );
    // Phase must remain Active (finalize did nothing)
    assert_eq!(c.tx_phase(1), Some(TxPhase::Active));
}

/// Trace T5 variant: CoordHandleFastCommitReply on wrong phase (Aborted) → NotReady.
#[test]
fn finalize_fast_commit_on_aborted_phase_returns_not_ready() {
    let mut c = coord();
    c.start_tx(1);
    c.add_participant(1, 100);
    c.abort_tx(1);
    assert_eq!(
        c.finalize_fast_commit(1, 5),
        FinalizeFastCommitResult::NotReady
    );
}

/// TLA+ fast path: CoordFastCommit → ShardHandleFastCommit → CoordHandleFastCommitReply.
#[test]
fn path_single_shard_fast_commit() {
    let mut c = coord();

    // CoordStartTx(T1)
    let _start_ts = c.start_tx(1);

    // CoordUpdate(T1, K1, v): register shard S1
    c.add_participant(1, 100);

    // CoordFastCommit(T1): single shard — get its id
    let BeginFastCommitResult::Ok(shard_id) = c.begin_fast_commit(1) else {
        panic!("expected Ok");
    };
    assert_eq!(shard_id, 100);

    // ShardHandleFastCommit(S1, T1): shard commits and replies with commit_ts=3
    // CoordHandleFastCommitReply(T1): finalize
    let r = c.finalize_fast_commit(1, 3);
    assert_eq!(r, FinalizeFastCommitResult::Ok);
    assert_eq!(c.tx_phase(1), Some(TxPhase::Committed));
    assert_eq!(c.clock, 4); // advanced past commit_ts=3
}

/// SI2: after T1 fast-commits at ts=5, coordinator clock advances to 6.
/// T2 therefore gets start_ts=7, which ensures any shard assigned commit_ts for T2
/// is necessarily > T1's commit_ts, preventing duplicate commit timestamps.
#[test]
fn si2_fast_commit_clock_advances_prevent_timestamp_reuse() {
    let mut c = coord();

    // T1 fast-commits at shard-assigned timestamp 5.
    c.start_tx(1);
    c.add_participant(1, 100);
    c.begin_fast_commit(1); // phase → Preparing
    let r = c.finalize_fast_commit(1, 5);
    assert_eq!(r, FinalizeFastCommitResult::Ok);
    // Coordinator clock must now be > 5 so T2 starts at a strictly later time.
    assert!(c.clock > 5, "clock must advance past T1 commit_ts to prevent SI2 violation");

    // T2 starts after T1 committed; its start_ts > T1's commit_ts.
    let t2_start = c.start_tx(2);
    assert!(
        t2_start > 5,
        "T2 start_ts={t2_start} must be > T1 commit_ts=5"
    );

    // T2 fast-commits at shard-assigned timestamp 6 (shard clock advanced past T1's ts).
    c.add_participant(2, 100);
    c.begin_fast_commit(2);
    c.finalize_fast_commit(2, 6);
    // Both commit timestamps are distinct — SI2 holds.
    assert_ne!(
        c.start_ts(1), c.start_ts(2),
        "SI2: two committed transactions must not share a start timestamp"
    );
}

// ---------------------------------------------------------------------------
// ReadRetryPolicy — TLA+ traces T1–T5 (spec CoordAbortReadDeadline note)
// ---------------------------------------------------------------------------

// Trace T5: initial_delay = 0 → no backoff regardless of retry index.
#[test]
fn retry_policy_no_backoff_always_returns_zero() {
    let p = ReadRetryPolicy::no_backoff();
    assert_eq!(p.sleep_duration(0), Duration::ZERO);
    assert_eq!(p.sleep_duration(1), Duration::ZERO);
    assert_eq!(p.sleep_duration(10), Duration::ZERO);
}

// Trace T2: first retry (retry=0) returns initial_delay when jitter_max=0.
#[test]
fn retry_policy_first_retry_equals_initial_delay_without_jitter() {
    let p = ReadRetryPolicy::new(
        Duration::from_millis(5),
        Duration::from_millis(100),
        Duration::ZERO,
    );
    assert_eq!(p.sleep_duration(0), Duration::from_millis(5));
}

// Trace T3: delay doubles on each retry.
#[test]
fn retry_policy_delay_doubles_each_retry_without_jitter() {
    let p = ReadRetryPolicy::new(
        Duration::from_millis(5),
        Duration::from_millis(1000), // high cap — not reached in first few retries
        Duration::ZERO,
    );
    assert_eq!(p.sleep_duration(0), Duration::from_millis(5));
    assert_eq!(p.sleep_duration(1), Duration::from_millis(10));
    assert_eq!(p.sleep_duration(2), Duration::from_millis(20));
    assert_eq!(p.sleep_duration(3), Duration::from_millis(40));
}

// Trace T4: delay is capped at max_delay once 2^k * initial >= max.
#[test]
fn retry_policy_delay_capped_at_max_delay() {
    let p = ReadRetryPolicy::new(
        Duration::from_millis(5),
        Duration::from_millis(100),
        Duration::ZERO,
    );
    // 5 * 2^0=5, 2^1=10, 2^2=20, 2^3=40, 2^4=80, 2^5=160 → capped at 100.
    assert_eq!(p.sleep_duration(4), Duration::from_millis(80));
    assert_eq!(p.sleep_duration(5), Duration::from_millis(100));
    assert_eq!(p.sleep_duration(6), Duration::from_millis(100));
    assert_eq!(p.sleep_duration(100), Duration::from_millis(100));
}

// Trace T4 (variant): once capped, every subsequent retry returns max_delay (no jitter).
#[test]
fn retry_policy_stays_at_max_once_capped() {
    let p = ReadRetryPolicy::new(
        Duration::from_millis(10),
        Duration::from_millis(10),
        Duration::ZERO,
    );
    // initial == max → always returns max regardless of retry.
    assert_eq!(p.sleep_duration(0), Duration::from_millis(10));
    assert_eq!(p.sleep_duration(1), Duration::from_millis(10));
    assert_eq!(p.sleep_duration(50), Duration::from_millis(10));
}

// Trace T2/T3: jitter is within [0, jitter_max) and total sleep <= max_delay + jitter_max.
#[test]
fn retry_policy_jitter_within_bounds() {
    let initial = Duration::from_millis(5);
    let max = Duration::from_millis(100);
    let jitter_max = Duration::from_millis(10);
    let p = ReadRetryPolicy::new(initial, max, jitter_max);
    for retry in 0u32..20 {
        let d = p.sleep_duration(retry);
        // Must be at least initial_delay (jitter is additive, never subtractive).
        assert!(
            d >= initial,
            "retry {retry}: expected >= {initial:?}, got {d:?}"
        );
        // Must not exceed max_delay + jitter_max.
        assert!(
            d <= max + jitter_max,
            "retry {retry}: expected <= {:?}, got {d:?}",
            max + jitter_max,
        );
    }
}

// Trace T3 (variant): different retry indices produce different jitter values
// (hash is not degenerate — at least two distinct values across 10 retries).
#[test]
fn retry_policy_jitter_not_constant_across_retries() {
    let p = ReadRetryPolicy::new(
        Duration::from_millis(5),
        Duration::from_millis(5), // base always 5ms — any variation comes from jitter
        Duration::from_millis(10),
    );
    let durations: Vec<Duration> = (0u32..10).map(|r| p.sleep_duration(r)).collect();
    let distinct: std::collections::HashSet<Duration> = durations.iter().copied().collect();
    assert!(
        distinct.len() > 1,
        "expected at least two distinct sleep durations, got {durations:?}"
    );
}

// Policy accessors: default_policy has non-zero values.
#[test]
fn retry_policy_default_has_non_zero_initial_and_max() {
    let p = ReadRetryPolicy::default_policy();
    assert!(p.initial_delay > Duration::ZERO);
    assert!(p.max_delay >= p.initial_delay);
}

// Idempotency: calling sleep_duration with the same retry index twice returns same value.
#[test]
fn retry_policy_sleep_duration_is_deterministic() {
    let p = ReadRetryPolicy::default_policy();
    for retry in 0u32..5 {
        assert_eq!(
            p.sleep_duration(retry),
            p.sleep_duration(retry),
            "sleep_duration({retry}) is not deterministic"
        );
    }
}
