use std::sync::Mutex;

use tonic::{Request, Response, Status};

use crate::coordinator::CoordinatorState;
use crate::proto::coordinator_service_server::CoordinatorService;
use crate::proto::{
    Active, InquireReply, InquireRequest,
    TxAbortReply, TxAbortRequest,
    TxBeginReply, TxBeginRequest,
    TxCommitReply, TxCommitRequest,
    TxReadReply, TxReadRequest,
    TxUpdateReply, TxUpdateRequest,
};
use crate::shard::InquiryStatus;

pub use crate::proto::coordinator_service_server::CoordinatorServiceServer;

pub struct CoordinatorServer {
    state: Mutex<CoordinatorState>,
}

impl CoordinatorServer {
    pub fn new(state: CoordinatorState) -> Self {
        Self { state: Mutex::new(state) }
    }
}

#[tonic::async_trait]
impl CoordinatorService for CoordinatorServer {
    async fn begin(
        &self,
        _request: Request<TxBeginRequest>,
    ) -> Result<Response<TxBeginReply>, Status> {
        Err(Status::unimplemented("begin: shard routing not yet wired"))
    }

    async fn read(
        &self,
        _request: Request<TxReadRequest>,
    ) -> Result<Response<TxReadReply>, Status> {
        Err(Status::unimplemented("read: shard routing not yet wired"))
    }

    async fn update(
        &self,
        _request: Request<TxUpdateRequest>,
    ) -> Result<Response<TxUpdateReply>, Status> {
        Err(Status::unimplemented("update: shard routing not yet wired"))
    }

    async fn commit(
        &self,
        _request: Request<TxCommitRequest>,
    ) -> Result<Response<TxCommitReply>, Status> {
        Err(Status::unimplemented("commit: shard routing not yet wired"))
    }

    async fn abort(
        &self,
        _request: Request<TxAbortRequest>,
    ) -> Result<Response<TxAbortReply>, Status> {
        Err(Status::unimplemented("abort: shard routing not yet wired"))
    }

    async fn inquire(
        &self,
        request: Request<InquireRequest>,
    ) -> Result<Response<InquireReply>, Status> {
        let req = request.into_inner();
        let status = self
            .state
            .lock()
            .unwrap()
            .handle_inquire(req.tx_id, req.prep_ts);

        use crate::proto::inquire_reply::Status as S;
        let proto_status = match status {
            InquiryStatus::Committed(ts) => S::CommittedAt(ts),
            InquiryStatus::Active => S::Active(Active {}),
        };

        Ok(Response::new(InquireReply { status: Some(proto_status) }))
    }
}
