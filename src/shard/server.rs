use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Instant;

use tonic::{Request, Response, Status};

use crate::proto::shard_service_server::ShardService;
use crate::proto::{
    Abort, AbortReply, AbortRequest, Active, CommitReply, CommitRequest, FastCommitReply,
    FastCommitRequest, InquiryResult, NeedsInquirySet, PrepareReply, PrepareRequest, ReadReply,
    ReadRequest, UpdateReply, UpdateRequest,
};
use crate::shard::{InquiryStatus, ShardState};

pub use crate::proto::shard_service_server::ShardServiceServer;

pub struct ShardServer {
    state: Mutex<ShardState>,
}

impl ShardServer {
    pub fn new(state: ShardState) -> Self {
        Self {
            state: Mutex::new(state),
        }
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
        self.state
            .lock()
            .unwrap()
            .handle_commit(req.tx_id, req.commit_ts);
        Ok(Response::new(CommitReply {}))
    }

    async fn abort(&self, request: Request<AbortRequest>) -> Result<Response<AbortReply>, Status> {
        let req = request.into_inner();
        self.state.lock().unwrap().handle_abort(req.tx_id);
        Ok(Response::new(AbortReply {}))
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
