use std::collections::{HashMap, HashSet};
use std::time::Duration;

use crate::shard::{InquiryStatus, Timestamp, TxId};

// ---------------------------------------------------------------------------
// ReadRetryPolicy — backoff between NeedsInquiry retries
// ---------------------------------------------------------------------------

/// Controls exponential-backoff behaviour of the NeedsInquiry retry loop.
///
/// After each `NeedsInquiry` reply the read loop sleeps for
///   `base = min(initial_delay * 2^retry, max_delay) + jitter`
/// before sending the next `READ_KEY` to the shard.  Jitter is derived
/// deterministically from the retry index (no external RNG needed).
///
/// Set `initial_delay = Duration::ZERO` to disable all backoff.
///
/// Matches the implementation note on `CoordAbortReadDeadline` in the TLA+ spec.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadRetryPolicy {
    /// Delay before the first retry.
    pub initial_delay: Duration,
    /// Upper bound on the base delay (before jitter is added).
    pub max_delay: Duration,
    /// Maximum jitter added to each sleep.
    pub jitter_max: Duration,
}

impl ReadRetryPolicy {
    pub fn new(initial_delay: Duration, max_delay: Duration, jitter_max: Duration) -> Self {
        ReadRetryPolicy {
            initial_delay,
            max_delay,
            jitter_max,
        }
    }

    /// Default production policy: 5 ms initial, 100 ms cap, 10 ms jitter.
    pub fn default_policy() -> Self {
        Self::new(
            Duration::from_millis(5),
            Duration::from_millis(100),
            Duration::from_millis(10),
        )
    }

    /// No backoff at all — retries are immediate.  Equivalent to the
    /// previous behaviour before this feature was added; useful in tests.
    pub fn no_backoff() -> Self {
        Self::new(Duration::ZERO, Duration::ZERO, Duration::ZERO)
    }

    /// Compute the sleep duration before the `retry`-th retry (0-indexed).
    ///
    /// `base = min(initial_delay * 2^retry, max_delay)`
    /// `jitter` is a deterministic pseudo-random value in `[0, jitter_max)`
    /// derived from the retry index via Knuth's multiplicative hash.
    pub fn sleep_duration(&self, retry: u32) -> Duration {
        if self.initial_delay.is_zero() {
            return Duration::ZERO;
        }
        // Saturating multiply: 2^retry * initial_delay, capped at max_delay.
        let shift = retry.min(30); // guard against overflow in 1u32 << shift
        let base = self
            .initial_delay
            .saturating_mul(1u32 << shift)
            .min(self.max_delay);

        let jitter = if self.jitter_max.is_zero() {
            Duration::ZERO
        } else {
            // Knuth multiplicative hash for spread-out deterministic jitter.
            let h = (retry as u64)
                .wrapping_mul(2_654_435_761)
                .wrapping_add(1_013_904_223);
            // jitter_max is expected to be small (ms range), fits in u64 nanos.
            let max_ns = self.jitter_max.as_nanos() as u64;
            Duration::from_nanos(h % max_ns)
        };

        base + jitter
    }
}

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

    /// CoordFastCommit (phase 1): validate that this is a single-shard transaction,
    /// transition ACTIVE → PREPARING, and return the sole participant so the caller
    /// can send a FastCommit RPC.
    ///
    /// Setting PREPARING before returning matches the `CoordFastCommit` action in the
    /// TLA+ spec (`tx_state' = "PREPARING"`).  This prevents a concurrent client
    /// `abort()` RPC from preempting the in-flight FastCommit: `CoordAbortOnReply`
    /// requires `tx_state = "ACTIVE"`, so the abort handler is a no-op during PREPARING.
    /// Without this transition, a concurrent abort could set coordinator state to
    /// ABORTED while the shard's `handle_fast_commit` has already committed the write.
    pub fn begin_fast_commit(&mut self, tx_id: TxId) -> BeginFastCommitResult {
        let tx = match self.transactions.get_mut(&tx_id) {
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
        // Transition ACTIVE → PREPARING before returning the shard id.
        // Matches CoordFastCommit in the TLA+ spec: tx_state' = "PREPARING".
        tx.phase = TxPhase::Preparing;
        BeginFastCommitResult::Ok(shard_id)
    }

    /// CoordHandleFastCommitReply: record the shard's assigned commit_ts and
    /// transition PREPARING → COMMITTED. Advances the coordinator clock to
    /// `commit_ts + 1` to maintain SI2.
    ///
    /// Requires `Preparing` phase (not `Active`): the caller must have called
    /// `begin_fast_commit` first, which transitions ACTIVE → PREPARING per the
    /// TLA+ spec (`CoordFastCommit` sets `tx_state → PREPARING`).
    pub fn finalize_fast_commit(
        &mut self,
        tx_id: TxId,
        commit_ts: Timestamp,
    ) -> FinalizeFastCommitResult {
        let tx = match self.transactions.get_mut(&tx_id) {
            Some(t) => t,
            None => return FinalizeFastCommitResult::NotReady,
        };
        if tx.phase != TxPhase::Preparing {
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
