use std::collections::{HashMap, HashSet};

use crate::shard::{InquiryStatus, Timestamp, TxId};

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

pub type ShardId = u64;

/// Maps directly to tx_state in TLA+: ACTIVE / PREPARING / COMMIT_WAIT / COMMITTED / ABORTED.
/// IDLE is not represented here; a TxEntry is only created on start_tx.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TxPhase {
    Active,
    Preparing,
    CommitWait,
    Committed,
    Aborted,
}

// ---------------------------------------------------------------------------
// Result types (one per coordinator action)
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq)]
pub enum BeginCommitResult {
    /// Send PREPARE to these shards (tx_state → PREPARING).
    Prepare(HashSet<ShardId>),
    /// Transaction was already aborted.
    Aborted,
    /// No participant shards were registered — nothing to commit.
    NoParticipants,
}

#[derive(Debug, PartialEq, Eq)]
pub enum CollectPrepareResult {
    /// Waiting for more PREPARE_REPLYs.
    NeedMore,
    /// All shards replied OK; send COMMIT to participants.
    Done {
        commit_ts: Timestamp,
        participants: HashSet<ShardId>,
    },
    /// At least one shard replied ABORT.
    Aborted,
}

#[derive(Debug, PartialEq, Eq)]
pub enum SendCommitResult {
    /// tx_state → COMMITTED; send CommitMsg(tx_id, commit_ts) to these shards.
    Ok {
        commit_ts: Timestamp,
        participants: HashSet<ShardId>,
    },
    /// Transaction is not in CommitWait phase.
    NotReady,
}

/// Returned by `begin_fast_commit`.
#[derive(Debug, PartialEq, Eq)]
pub enum BeginFastCommitResult {
    /// Send FastCommit to this single shard.
    Ok(ShardId),
    /// Transaction was already aborted (or unknown).
    Aborted,
    /// Transaction has zero or more than one participant shard — use regular 2PC.
    NotSingleShard,
}

/// Returned by `finalize_fast_commit`.
#[derive(Debug, PartialEq, Eq)]
pub enum FinalizeFastCommitResult {
    /// tx_state → COMMITTED; `is_committed` set.
    Ok,
    /// Transaction is not in the expected Active phase.
    NotReady,
}

// ---------------------------------------------------------------------------
// Coordinator state
// ---------------------------------------------------------------------------

struct TxEntry {
    start_ts: Timestamp,
    commit_ts: Option<Timestamp>,
    is_committed: bool,
    participants: HashSet<ShardId>,
    phase: TxPhase,
    /// PREPARE_REPLYs collected so far: shard → prep_t.
    prepare_replies: HashMap<ShardId, Timestamp>,
}

pub struct CoordinatorState {
    /// Logical clock (c_clock in TLA+).
    pub clock: Timestamp,
    transactions: HashMap<TxId, TxEntry>,
}

impl Default for CoordinatorState {
    fn default() -> Self {
        Self::new()
    }
}

impl CoordinatorState {
    pub fn new() -> Self {
        CoordinatorState {
            clock: 0,
            transactions: HashMap::new(),
        }
    }

    // -- Accessors -----------------------------------------------------------

    pub fn tx_phase(&self, tx_id: TxId) -> Option<TxPhase> {
        self.transactions.get(&tx_id).map(|t| t.phase.clone())
    }

    pub fn start_ts(&self, tx_id: TxId) -> Option<Timestamp> {
        self.transactions.get(&tx_id).map(|t| t.start_ts)
    }

    // -- Actions -------------------------------------------------------------

    /// CoordStartTx: assign start_ts = clock + 1, advance clock.
    /// Returns start_ts.
    pub fn start_tx(&mut self, tx_id: TxId) -> Timestamp {
        let start_ts = self.clock + 1;
        self.clock = start_ts;
        self.transactions.insert(
            tx_id,
            TxEntry {
                start_ts,
                commit_ts: None,
                is_committed: false,
                participants: HashSet::new(),
                phase: TxPhase::Active,
                prepare_replies: HashMap::new(),
            },
        );
        start_ts
    }

    /// CoordUpdate (participant tracking half): record shard_id as a participant
    /// of tx_id. Called when the coordinator routes an update to a shard.
    pub fn add_participant(&mut self, tx_id: TxId, shard_id: ShardId) {
        if let Some(tx) = self.transactions.get_mut(&tx_id) {
            if tx.phase == TxPhase::Active {
                tx.participants.insert(shard_id);
            }
        }
    }

    /// CoordBeginCommit: transition ACTIVE → PREPARING.
    /// Returns the participant set so the caller can dispatch PREPARE RPCs.
    pub fn begin_commit(&mut self, tx_id: TxId) -> BeginCommitResult {
        let tx = match self.transactions.get_mut(&tx_id) {
            Some(t) => t,
            None => return BeginCommitResult::Aborted,
        };
        match tx.phase {
            TxPhase::Active => {}
            TxPhase::Aborted => return BeginCommitResult::Aborted,
            _ => return BeginCommitResult::Aborted,
        }
        if tx.participants.is_empty() {
            return BeginCommitResult::NoParticipants;
        }
        tx.phase = TxPhase::Preparing;
        BeginCommitResult::Prepare(tx.participants.clone())
    }

    /// CoordFinalizePrepare (incremental): record one shard's PREPARE_REPLY.
    ///
    /// `ts` is `Some(prep_t)` for a successful prepare, `None` for abort.
    ///
    /// Returns `Done` once all participant shards have replied with timestamps,
    /// at which point commit_ts = max(all prep_ts, clock) and the coordinator
    /// clock advances to commit_ts + 1 (ensuring SI2).
    pub fn collect_prepare_reply(
        &mut self,
        tx_id: TxId,
        shard_id: ShardId,
        ts: Option<Timestamp>,
    ) -> CollectPrepareResult {
        let tx = match self.transactions.get_mut(&tx_id) {
            Some(t) => t,
            None => return CollectPrepareResult::Aborted,
        };
        if tx.phase == TxPhase::Aborted {
            return CollectPrepareResult::Aborted;
        }
        if ts.is_none() {
            tx.phase = TxPhase::Aborted;
            return CollectPrepareResult::Aborted;
        }
        tx.prepare_replies.insert(shard_id, ts.unwrap());
        if tx.prepare_replies.len() < tx.participants.len() {
            return CollectPrepareResult::NeedMore;
        }

        // All replied — compute commit_ts = max(all prep_ts, c_clock).
        let max_prep = tx.prepare_replies.values().copied().max().unwrap_or(0);
        let commit_ts = max_prep.max(self.clock);
        tx.commit_ts = Some(commit_ts);
        tx.phase = TxPhase::CommitWait;
        // Advance clock past commit_ts so the next tx's commit_ts is strictly greater (SI2).
        self.clock = commit_ts + 1;

        CollectPrepareResult::Done {
            commit_ts,
            participants: tx.participants.clone(),
        }
    }

    /// CoordSendCommit: transition COMMIT_WAIT → COMMITTED.
    /// Returns commit_ts and participants so the caller can dispatch COMMIT RPCs.
    pub fn send_commit(&mut self, tx_id: TxId) -> SendCommitResult {
        let tx = match self.transactions.get_mut(&tx_id) {
            Some(t) => t,
            None => return SendCommitResult::NotReady,
        };
        if tx.phase != TxPhase::CommitWait {
            return SendCommitResult::NotReady;
        }
        let commit_ts = tx.commit_ts.unwrap();
        tx.phase = TxPhase::Committed;
        tx.is_committed = true;
        SendCommitResult::Ok {
            commit_ts,
            participants: tx.participants.clone(),
        }
    }

    /// CoordFastCommit (phase 1): validate that this is a single-shard transaction
    /// and return the sole participant so the caller can send a FastCommit RPC.
    ///
    /// Does NOT change the transaction phase (the coordinator goes ACTIVE →
    /// COMMITTED atomically in `finalize_fast_commit` once the shard replies).
    pub fn begin_fast_commit(&mut self, tx_id: TxId) -> BeginFastCommitResult {
        let tx = match self.transactions.get(&tx_id) {
            Some(t) => t,
            None => return BeginFastCommitResult::Aborted,
        };
        match tx.phase {
            TxPhase::Active => {}
            TxPhase::Aborted => return BeginFastCommitResult::Aborted,
            _ => return BeginFastCommitResult::Aborted,
        }
        if tx.participants.len() != 1 {
            return BeginFastCommitResult::NotSingleShard;
        }
        let shard_id = *tx.participants.iter().next().unwrap();
        BeginFastCommitResult::Ok(shard_id)
    }

    /// CoordHandleFastCommitReply: record the shard's assigned commit_ts and
    /// transition ACTIVE → COMMITTED. Advances the coordinator clock to
    /// `commit_ts + 1` to maintain SI2.
    pub fn finalize_fast_commit(
        &mut self,
        tx_id: TxId,
        commit_ts: Timestamp,
    ) -> FinalizeFastCommitResult {
        let tx = match self.transactions.get_mut(&tx_id) {
            Some(t) => t,
            None => return FinalizeFastCommitResult::NotReady,
        };
        if tx.phase != TxPhase::Active {
            return FinalizeFastCommitResult::NotReady;
        }
        tx.phase = TxPhase::Committed;
        tx.is_committed = true;
        tx.commit_ts = Some(commit_ts);
        self.clock = self.clock.max(commit_ts + 1);
        FinalizeFastCommitResult::Ok
    }

    /// CoordAbortOnReply / manual abort: mark ABORTED.
    /// Returns participants so the caller can dispatch ABORT RPCs.
    pub fn abort_tx(&mut self, tx_id: TxId) -> HashSet<ShardId> {
        match self.transactions.get_mut(&tx_id) {
            Some(tx) => {
                tx.phase = TxPhase::Aborted;
                tx.participants.clone()
            }
            None => HashSet::new(),
        }
    }

    /// CoordHandleInquire: answer a shard's question about tx_id's commit status.
    ///
    /// Advances the coordinator clock by the reader's start_ts (Lamport clock
    /// synchronisation — matches `c_clock' = max(c_clock, m.ts)` in TLA+).
    pub fn handle_inquire(&mut self, tx_id: TxId, reader_start_ts: Timestamp) -> InquiryStatus {
        self.clock = self.clock.max(reader_start_ts);
        match self.transactions.get(&tx_id) {
            Some(tx) if tx.is_committed => InquiryStatus::Committed(tx.commit_ts.unwrap()),
            _ => InquiryStatus::Active,
        }
    }
}

// ---------------------------------------------------------------------------
// Tx-ID encoding
// ---------------------------------------------------------------------------

/// Generates transaction IDs that encode the issuing coordinator's port.
///
/// Format (64 bits):
///   [63..32]  coordinator port (u16 in low 16 bits of the high word)
///   [31.. 0]  sequence number
///
/// Any process holding a tx_id can recover the coordinator port with
/// `coord_port_from_tx_id` and reconstruct its address without a lookup
/// service — satisfying the TLA+ `CoordOf` relation purely from the id.
pub struct TxIdGen {
    coordinator_port: u16,
    next_seq: u32,
}

impl TxIdGen {
    /// Create a generator whose first sequence number is chosen by a
    /// time-mixed pseudo-random seed.  Different restarts of the same
    /// coordinator port will (with overwhelming probability) start from
    /// different positions, preventing new-epoch tx_ids from colliding with
    /// old-epoch entries still held in shards' aborted sets.
    pub fn new(coordinator_port: u16) -> Self {
        Self::new_at(coordinator_port, Self::time_mixed_seed(coordinator_port))
    }

    /// Create a generator whose first sequence number is exactly `start_seq`.
    /// Use this in tests where deterministic ID values are required.
    pub fn new_at(coordinator_port: u16, start_seq: u32) -> Self {
        TxIdGen {
            coordinator_port,
            next_seq: start_seq,
        }
    }

    /// Derive a pseudo-random starting sequence from the current wall-clock
    /// time mixed with the coordinator port.
    ///
    /// The LCG step (Knuth multiplicative hash) spreads the nanosecond
    /// timestamp across all 32 bits.  XORing in the port ensures two
    /// coordinators starting at the same nanosecond get different seeds.
    fn time_mixed_seed(coordinator_port: u16) -> u32 {
        use std::time::{SystemTime, UNIX_EPOCH};
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(1);
        let mixed = nanos
            .wrapping_mul(6364136223846793005u64)
            .wrapping_add(1442695040888963407u64)
            ^ (coordinator_port as u64).wrapping_mul(2246822519u64);
        (mixed >> 32) as u32
    }
}

impl Iterator for TxIdGen {
    type Item = TxId;

    fn next(&mut self) -> Option<TxId> {
        let seq = self.next_seq;
        self.next_seq = self.next_seq.wrapping_add(1);
        Some(((self.coordinator_port as u64) << 32) | seq as u64)
    }
}

/// Decode the coordinator's listening port from a tx_id.
pub fn coord_port_from_tx_id(tx_id: TxId) -> u16 {
    (tx_id >> 32) as u16
}

/// Decode the sequence number from a tx_id.
pub fn coord_seq_from_tx_id(tx_id: TxId) -> u32 {
    tx_id as u32
}

#[cfg(test)]
mod tests;

pub mod routing;
pub mod server;
