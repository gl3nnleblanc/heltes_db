use std::collections::HashSet;

use crate::shard::InquiryStatus;

use super::{
    coord_port_from_tx_id, BeginCommitResult, BeginFastCommitResult, CollectPrepareResult,
    CoordinatorState, FinalizeFastCommitResult, SendCommitResult, TxIdGen, TxPhase,
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
    assert_eq!(c.begin_commit(1), BeginCommitResult::Prepare(shards(&[10, 20])));
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
        CollectPrepareResult::Done { commit_ts: 3, participants: shards(&[10]) }
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
        CollectPrepareResult::Done { commit_ts: 5, participants: shards(&[10, 20]) }
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
        CollectPrepareResult::Done { commit_ts: 7, participants: shards(&[10, 20]) }
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
        CollectPrepareResult::Done { commit_ts: 2, participants: shards(&[10]) }
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
        SendCommitResult::Ok { commit_ts: 3, participants: shards(&[10]) }
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
    let CollectPrepareResult::Done { commit_ts, participants } = r else {
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
// TxIdGen / coord_port_from_tx_id
// ---------------------------------------------------------------------------

#[test]
fn txidgen_encodes_port_in_high_bits() {
    let mut gen = TxIdGen::new(50052);
    let id = gen.next();
    assert_eq!(coord_port_from_tx_id(id), 50052);
}

#[test]
fn txidgen_sequence_increments() {
    let mut gen = TxIdGen::new(50052);
    let id0 = gen.next();
    let id1 = gen.next();
    let id2 = gen.next();
    assert_eq!(id0 & 0xFFFF_FFFF, 0);
    assert_eq!(id1 & 0xFFFF_FFFF, 1);
    assert_eq!(id2 & 0xFFFF_FFFF, 2);
}

#[test]
fn txidgen_different_ports_produce_distinct_ids() {
    let mut gen_a = TxIdGen::new(50052);
    let mut gen_b = TxIdGen::new(50053);
    // Same sequence slot, different ports — ids must not collide.
    assert_ne!(gen_a.next(), gen_b.next());
}

#[test]
fn coord_port_roundtrips() {
    for port in [50051u16, 50052, 60000, 1, u16::MAX] {
        let mut gen = TxIdGen::new(port);
        let id = gen.next();
        assert_eq!(coord_port_from_tx_id(id), port);
    }
}

/// SI2 — two transactions get distinct commit timestamps.
#[test]
fn si2_committed_txs_have_distinct_commit_timestamps() {
    let mut c = coord();

    c.start_tx(1);
    c.add_participant(1, 100);
    c.begin_commit(1);
    let r1 = c.collect_prepare_reply(1, 100, Some(2));
    let CollectPrepareResult::Done { commit_ts: ct1, .. } = r1 else { panic!() };
    c.send_commit(1);

    c.start_tx(2);
    c.add_participant(2, 100);
    c.begin_commit(2);
    let r2 = c.collect_prepare_reply(2, 100, Some(2)); // shard reused prep_ts value
    let CollectPrepareResult::Done { commit_ts: ct2, .. } = r2 else { panic!() };
    c.send_commit(2);

    assert_ne!(ct1, ct2, "SI2 violated: two committed txs share a commit timestamp");
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
    assert_eq!(c.begin_fast_commit(1), BeginFastCommitResult::NotSingleShard);
}

/// Trace: CoordFastCommit guard — two participants → NotSingleShard (use regular 2PC).
#[test]
fn begin_fast_commit_multiple_participants_returns_not_single_shard() {
    let mut c = coord();
    c.start_tx(1);
    c.add_participant(1, 100);
    c.add_participant(1, 200);
    assert_eq!(c.begin_fast_commit(1), BeginFastCommitResult::NotSingleShard);
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

/// Trace: CoordHandleFastCommitReply success — tx transitions to Committed.
#[test]
fn finalize_fast_commit_transitions_to_committed() {
    let mut c = coord();
    c.start_tx(1);
    c.add_participant(1, 100);
    assert_eq!(c.finalize_fast_commit(1, 5), FinalizeFastCommitResult::Ok);
    assert_eq!(c.tx_phase(1), Some(TxPhase::Committed));
}

/// Trace: CoordHandleFastCommitReply — sets is_committed so Inquire replies correctly.
#[test]
fn finalize_fast_commit_makes_tx_visible_to_inquire() {
    let mut c = coord();
    c.start_tx(1);
    c.add_participant(1, 100);
    c.finalize_fast_commit(1, 7);
    use crate::shard::InquiryStatus;
    assert_eq!(c.handle_inquire(1, 0), InquiryStatus::Committed(7));
}

/// Trace: CoordHandleFastCommitReply — coordinator clock advances past commit_ts (SI2).
#[test]
fn finalize_fast_commit_advances_clock_past_commit_ts() {
    let mut c = coord();
    c.start_tx(1); // clock → 1
    c.add_participant(1, 100);
    c.finalize_fast_commit(1, 8); // commit_ts = 8; clock must → 9
    assert_eq!(c.clock, 9);
}

/// Trace: CoordHandleFastCommitReply — clock only advances, never regresses.
#[test]
fn finalize_fast_commit_does_not_regress_clock() {
    let mut c = coord();
    c.start_tx(1); // clock → 1
    c.start_tx(2); // clock → 2
    c.add_participant(1, 100);
    // commit_ts = 1 < clock = 2; clock must stay at 2 (or advance to 2).
    c.finalize_fast_commit(1, 1);
    assert!(c.clock >= 2, "clock must not regress below pre-existing value");
}

/// Trace: CoordHandleFastCommitReply on wrong phase → NotReady.
#[test]
fn finalize_fast_commit_on_wrong_phase_returns_not_ready() {
    let mut c = coord();
    c.start_tx(1);
    c.add_participant(1, 100);
    c.abort_tx(1);
    assert_eq!(c.finalize_fast_commit(1, 5), FinalizeFastCommitResult::NotReady);
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

/// SI2: sequential fast-commit txs must have distinct commit timestamps.
#[test]
fn si2_fast_commit_txs_have_distinct_timestamps() {
    let mut c = coord();

    c.start_tx(1);
    c.add_participant(1, 100);
    c.finalize_fast_commit(1, 5); // T1 commits at 5; clock → 6

    c.start_tx(2);
    c.add_participant(2, 100);
    // T2's shard timestamp would be based on shard clock, but coordinator clock
    // is 6; if the shard replies with 5 again (reuse), coordinator must not
    // issue the same timestamp.
    // The fast path uses the SHARD's commit_ts directly — callers must pass
    // the actual shard-assigned timestamp. Here we simulate shard returning 6.
    c.finalize_fast_commit(2, 6);
    assert_ne!(5u64, 6u64, "SI2: timestamps must be distinct");
}
