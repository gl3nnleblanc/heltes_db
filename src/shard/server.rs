use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tonic::{Request, Response, Status};

use crate::proto::shard_service_server::ShardService;
use crate::proto::{
    Abort, AbortReply, AbortRequest, Active, CommitReply, CommitRequest, FastCommitReply,
    FastCommitRequest, GetClockReply, GetClockRequest, InquiryResult, NeedsInquirySet,
    PrepareReply, PrepareRequest, ReadReply, ReadRequest, UpdateReply, UpdateRequest,
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
                bg_state.lock().unwrap().compact_versions();
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
            let mut state = self.state.lock().unwrap();
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
            let mut state = self.state.lock().unwrap();
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
            let mut state = self.state.lock().unwrap();
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
            .unwrap()
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
        self.state.lock().unwrap().handle_abort(req.tx_id);
        Ok(Response::new(AbortReply {}))
    }

    /// CoordSyncClock (shard side): return the current shard logical clock so
    /// a newly-started coordinator can advance its c_clock before serving requests.
    async fn get_clock(
        &self,
        _request: Request<GetClockRequest>,
    ) -> Result<Response<GetClockReply>, Status> {
        let clock = self.state.lock().unwrap().clock;
        Ok(Response::new(GetClockReply { clock }))
    }

    async fn fast_commit(
        &self,
        request: Request<FastCommitRequest>,
    ) -> Result<Response<FastCommitReply>, Status> {
        let req = request.into_inner();
        let result = self.state.lock().unwrap().handle_fast_commit(req.tx_id);

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
