use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use tonic::{Request, Response, Status};

use crate::proto::shard_service_server::ShardService;
use crate::proto::{
    Abort, AbortReply, AbortRequest, Active, CommitReply, CommitRequest, FastCommitReply,
    FastCommitRequest, GetClockReply, GetClockRequest, InquiryResult, NeedsInquirySet,
    PrepareReply, PrepareRequest, ReadReply, ReadRequest, UpdateReply, UpdateRequest,
    WriteAndFastCommitRequest,
};
use crate::shard::{CommitResult, InquiryStatus, ShardState};

pub use crate::proto::shard_service_server::ShardServiceServer;

pub struct ShardServer {
    state: Arc<Mutex<ShardState>>,
}

impl ShardServer {
    /// Default interval between background compaction passes.
    pub const DEFAULT_COMPACT_INTERVAL: Duration = Duration::from_millis(100);

    /// Construct a shard server and spawn a background task that calls
    /// `compact_versions()` every `compact_interval`.
    ///
    /// Compaction holds the shard mutex only for the duration of a single
    /// scan-and-drain pass (typically microseconds), then releases it.
    /// RPCs may be briefly delayed when the compactor runs, but it is no
    /// longer on the per-commit hot path.
    pub fn new(state: ShardState) -> Self {
        Self::with_compact_interval(state, Self::DEFAULT_COMPACT_INTERVAL)
    }

    pub fn with_compact_interval(state: ShardState, compact_interval: Duration) -> Self {
        let state = Arc::new(Mutex::new(state));
        let bg_state = Arc::clone(&state);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(compact_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                let mut state = bg_state.lock();
                state.compact_versions();
                // Evict read tracking entries for abandoned read-only transactions.
                // Transactions that only read (never write) don't send COMMIT/ABORT
                // to this shard, so their read_start_ts entries must be expired here
                // to prevent them from pinning the compaction watermark indefinitely.
                state.expire_reads(Instant::now());
                // Prune aborted entries from dead coordinator ports.
                // prune_aborted() already runs on every commit/abort RPC, but if a
                // coordinator crashes and never restarts, no new RPCs arrive from that
                // port and its aborted entries accumulate indefinitely.
                // Running it here ensures eviction within one compact_interval even
                // when a coordinator port goes permanently quiescent.
                // (ShardPruneOrphanedPort in TLA+)
                state.prune_aborted();
            }
        });
        Self { state }
    }
}

// ── Type conversion helpers ───────────────────────────────────────────────────

fn proto_inquiry_results(results: Vec<InquiryResult>) -> HashMap<u64, InquiryStatus> {
    results
        .into_iter()
        .filter_map(|r| {
            let status = match r.status? {
                crate::proto::inquiry_result::Status::CommittedAt(ts) => {
                    InquiryStatus::Committed(ts)
                }
                crate::proto::inquiry_result::Status::Active(_) => InquiryStatus::Active,
            };
            Some((r.tx_id, status))
        })
        .collect()
}

// ── ShardService impl ─────────────────────────────────────────────────────────

#[tonic::async_trait]
impl ShardService for ShardServer {
    async fn read(&self, request: Request<ReadRequest>) -> Result<Response<ReadReply>, Status> {
        let req = request.into_inner();
        let inquiry_results = proto_inquiry_results(req.inquiry_results);

        let result = {
            let mut state = self.state.lock();
            state.expire_prepared(Instant::now());
            state.handle_read(req.tx_id, req.start_ts, req.key, &inquiry_results)
        };

        use crate::proto::read_reply::Result as R;
        use crate::shard::ReadResult;

        let proto_result = match result {
            ReadResult::Value(v) => R::Value(v.0),
            ReadResult::NotFound => R::NotFound(true),
            ReadResult::Abort => R::Abort(Abort {}),
            ReadResult::NeedsInquiry(ids) => R::NeedsInquiry(NeedsInquirySet { tx_ids: ids }),
        };

        Ok(Response::new(ReadReply {
            result: Some(proto_result),
        }))
    }

    async fn update(
        &self,
        request: Request<UpdateRequest>,
    ) -> Result<Response<UpdateReply>, Status> {
        let req = request.into_inner();
        let result = {
            let mut state = self.state.lock();
            state.expire_prepared(Instant::now());
            state.handle_update(
                req.tx_id,
                req.start_ts,
                req.key,
                crate::shard::Value(req.value),
            )
        };

        use crate::proto::update_reply::Result as R;
        use crate::shard::UpdateResult;

        let proto_result = match result {
            UpdateResult::Ok => R::Ok(true),
            UpdateResult::Abort => R::Abort(Abort {}),
        };

        Ok(Response::new(UpdateReply {
            result: Some(proto_result),
        }))
    }

    async fn prepare(
        &self,
        request: Request<PrepareRequest>,
    ) -> Result<Response<PrepareReply>, Status> {
        let req = request.into_inner();
        let result = {
            let mut state = self.state.lock();
            state.expire_prepared(Instant::now());
            state.handle_prepare(req.tx_id)
        };

        use crate::proto::prepare_reply::Result as R;
        use crate::shard::PrepareResult;

        let proto_result = match result {
            PrepareResult::Timestamp(ts) => R::Timestamp(ts),
            PrepareResult::Abort => R::Abort(Abort {}),
        };

        Ok(Response::new(PrepareReply {
            result: Some(proto_result),
        }))
    }

    async fn commit(
        &self,
        request: Request<CommitRequest>,
    ) -> Result<Response<CommitReply>, Status> {
        let req = request.into_inner();
        let result = self
            .state
            .lock()
            .handle_commit(req.tx_id, req.commit_ts);

        use crate::proto::commit_reply::Result as R;
        let proto_result = match result {
            CommitResult::Ok => R::Ok(true),
            CommitResult::Abort => R::Abort(Abort {}),
        };
        Ok(Response::new(CommitReply {
            result: Some(proto_result),
        }))
    }

    async fn abort(&self, request: Request<AbortRequest>) -> Result<Response<AbortReply>, Status> {
        let req = request.into_inner();
        self.state.lock().handle_abort(req.tx_id);
        Ok(Response::new(AbortReply {}))
    }

    /// CoordSyncClock (shard side): return the current shard logical clock so
    /// a newly-started coordinator can advance its c_clock before serving requests.
    async fn get_clock(
        &self,
        _request: Request<GetClockRequest>,
    ) -> Result<Response<GetClockReply>, Status> {
        let clock = self.state.lock().clock;
        Ok(Response::new(GetClockReply { clock }))
    }

    async fn fast_commit(
        &self,
        request: Request<FastCommitRequest>,
    ) -> Result<Response<FastCommitReply>, Status> {
        let req = request.into_inner();
        let result = self.state.lock().handle_fast_commit(req.tx_id);

        use crate::proto::fast_commit_reply::Result as R;
        use crate::shard::FastCommitResult;

        let proto_result = match result {
            FastCommitResult::Ok(commit_ts) => R::CommitTs(commit_ts),
            FastCommitResult::Abort => R::Abort(Abort {}),
        };

        Ok(Response::new(FastCommitReply {
            result: Some(proto_result),
        }))
    }

    /// ShardHandleWriteAndFastCommit: atomically check conflicts, install the write, commit.
    ///
    /// Single-write fast path: bypasses write_buff and installs the version directly.
    /// Returns commit_ts on success or Abort on conflict / pre-existing abort.
    async fn write_and_fast_commit(
        &self,
        request: Request<WriteAndFastCommitRequest>,
    ) -> Result<Response<FastCommitReply>, Status> {
        let req = request.into_inner();
        let result = self.state.lock().handle_write_and_fast_commit(
            req.tx_id,
            req.start_ts,
            req.key,
            crate::shard::Value(req.value),
        );

        use crate::proto::fast_commit_reply::Result as R;
        use crate::shard::FastCommitResult;

        let proto_result = match result {
            FastCommitResult::Ok(commit_ts) => R::CommitTs(commit_ts),
            FastCommitResult::Abort => R::Abort(Abort {}),
        };

        Ok(Response::new(FastCommitReply {
            result: Some(proto_result),
        }))
    }
}

// Suppress unused import warning for Active (used in proto_inquiry_results via generated code)
const _: () = {
    let _ = Active {};
};

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shard::{Key, Value};

    // Port-encoded TxIds matching the convention in shard/tests.rs.
    const P1_S1: u64 = (1u64 << 32) | 1;
    const P1_S2: u64 = (1u64 << 32) | 2;
    const P2_S1: u64 = (2u64 << 32) | 1;

    /// Trace T1: ShardPruneOrphanedPort — background compact task calls
    /// prune_aborted() so dead coordinator entries are evicted even when
    /// no new RPC from that coordinator ever arrives.
    ///
    /// Without the fix, compact_versions_only fires and aborted entries
    /// from dead ports accumulate indefinitely. With the fix, the background
    /// tick also calls prune_aborted(), clearing quiescent-port entries within
    /// one compact_interval.
    #[tokio::test]
    async fn background_task_prunes_orphaned_aborted_entries() {
        let mut state = ShardState::new();

        // Simulate two transactions from port P1 that were aborted.
        // No write_buff or prepared entries remain from P1 (coordinator is dead).
        state.aborted.insert(P1_S1);
        state.aborted.insert(P1_S2);

        // Use a very short compact interval so the background task fires quickly.
        let server = ShardServer::with_compact_interval(state, Duration::from_millis(5));

        // Wait for at least two ticks to be confident the background task ran.
        tokio::time::sleep(Duration::from_millis(30)).await;

        let state = server.state.lock();
        assert!(
            !state.aborted.contains(&P1_S1),
            "P1_S1 must be pruned by background task: dead coordinator"
        );
        assert!(
            !state.aborted.contains(&P1_S2),
            "P1_S2 must be pruned by background task: dead coordinator"
        );
    }

    /// Trace T2: background task does not prune entries from ports that still
    /// have in-flight transactions (active write_buff entry protects them).
    #[tokio::test]
    async fn background_task_preserves_entries_protected_by_active_tx() {
        let mut state = ShardState::new();

        // P1_S1 aborted; P1_S2 still active in write_buff → min_active[P1]=2.
        // P1_S1 seq=1 < 2 → will be pruned (normal watermark behavior).
        // P2_S1 aborted; P2 has no active txs → will be pruned (dead port).
        state.aborted.insert(P1_S1);
        state.aborted.insert(P2_S1);
        state
            .write_buff
            .entry(P1_S2)
            .or_default()
            .insert(1u64 as Key, Value(99));

        let server = ShardServer::with_compact_interval(state, Duration::from_millis(5));
        tokio::time::sleep(Duration::from_millis(30)).await;

        let state = server.state.lock();
        // P1_S1: seq=1 < min_active[P1]=2 → pruned by watermark.
        assert!(
            !state.aborted.contains(&P1_S1),
            "P1_S1 seq<watermark must be pruned"
        );
        // P2_S1: no active P2 txs → pruned by dead-port eviction.
        assert!(
            !state.aborted.contains(&P2_S1),
            "P2_S1 from dead port must be pruned"
        );
        // P1_S2 is still active (in write_buff), not in aborted — not affected.
        assert!(!state.aborted.contains(&P1_S2), "P1_S2 was never aborted");
    }
}
