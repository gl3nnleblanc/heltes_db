use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Mutex;

use futures::future::join_all;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Response, Status};

use crate::coordinator::{
    coord_port_from_tx_id, routing::ConsistentHashRouter, BeginCommitResult,
    BeginFastCommitResult, CollectPrepareResult, CoordinatorState, FinalizeFastCommitResult,
    SendCommitResult, TxIdGen,
};
use crate::proto::coordinator_service_client::CoordinatorServiceClient;
use crate::proto::coordinator_service_server::CoordinatorService;
use crate::proto::shard_service_client::ShardServiceClient;
use crate::proto::{
    Abort, AbortRequest as ShardAbortRequest, Active, CommitRequest as ShardCommitRequest,
    FastCommitRequest as ShardFastCommitRequest,
    InquireReply, InquireRequest, InquiryResult, NeedsInquirySet, PrepareRequest, ReadRequest,
    TxAbortReply, TxAbortRequest, TxBeginReply, TxBeginRequest, TxCommitReply, TxCommitRequest,
    TxReadReply, TxReadRequest, TxUpdateReply, TxUpdateRequest, UpdateRequest,
};
use crate::shard::InquiryStatus;

pub use crate::proto::coordinator_service_server::CoordinatorServiceServer;

// ── Server struct ─────────────────────────────────────────────────────────────

pub struct CoordinatorServer {
    state: Mutex<CoordinatorState>,
    tx_id_gen: Mutex<TxIdGen>,
    router: ConsistentHashRouter,
    /// port → connected shard client (lazy connection)
    shard_clients: HashMap<u64, ShardServiceClient<Channel>>,
    my_port: u16,
}

impl CoordinatorServer {
    /// Construct a coordinator that will route to `shard_addrs` via consistent
    /// hashing. Connections to shards are lazy (established on first use).
    pub fn new(
        state: CoordinatorState,
        my_port: u16,
        shard_addrs: Vec<SocketAddr>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let router = ConsistentHashRouter::new(shard_addrs.iter().copied());
        let mut shard_clients = HashMap::new();
        for addr in shard_addrs {
            let channel = Endpoint::from_shared(format!("http://{addr}"))?.connect_lazy();
            shard_clients.insert(addr.port() as u64, ShardServiceClient::new(channel));
        }
        Ok(CoordinatorServer {
            state: Mutex::new(state),
            tx_id_gen: Mutex::new(TxIdGen::new(my_port)),
            router,
            shard_clients,
            my_port,
        })
    }

    fn shard_client(&self, port: u64) -> Result<ShardServiceClient<Channel>, Status> {
        self.shard_clients
            .get(&port)
            .cloned()
            .ok_or_else(|| Status::internal(format!("no client for shard port {port}")))
    }

    /// Resolve an Inquire for `tx_id`. If that tx belongs to us, answer
    /// in-process; otherwise connect to the remote coordinator.
    async fn resolve_inquiry(
        &self,
        tx_id: u64,
        reader_start_ts: u64,
    ) -> Result<InquiryStatus, Status> {
        let port = coord_port_from_tx_id(tx_id);
        if port == self.my_port {
            return Ok(self.state.lock().unwrap().handle_inquire(tx_id, reader_start_ts));
        }
        let uri = format!("http://[::1]:{port}");
        let mut client = CoordinatorServiceClient::connect(uri)
            .await
            .map_err(|e| Status::unavailable(format!("coordinator :{port} unreachable: {e}")))?;
        let reply = client
            .inquire(InquireRequest { tx_id, prep_ts: reader_start_ts })
            .await?
            .into_inner();
        use crate::proto::inquire_reply::Status as S;
        match reply.status {
            Some(S::CommittedAt(ts)) => Ok(InquiryStatus::Committed(ts)),
            Some(S::Active(_)) => Ok(InquiryStatus::Active),
            None => Err(Status::internal("empty inquire reply")),
        }
    }

    /// Mark `tx_id` aborted in state and fire-and-forget ABORT to every
    /// participant shard. Matches CoordAbortOnReply in the TLA+ spec.
    async fn abort_and_notify(&self, tx_id: u64) {
        let participants = self.state.lock().unwrap().abort_tx(tx_id);
        let futs = participants.into_iter().filter_map(|port| {
            self.shard_clients.get(&port).map(|c| {
                let mut client = c.clone();
                async move { let _ = client.abort(ShardAbortRequest { tx_id }).await; }
            })
        });
        join_all(futs).await;
    }
}

// ── CoordinatorService impl ───────────────────────────────────────────────────

#[tonic::async_trait]
impl CoordinatorService for CoordinatorServer {
    /// CoordStartTx: generate a tx_id that encodes our port, assign start_ts.
    async fn begin(
        &self,
        _request: Request<TxBeginRequest>,
    ) -> Result<Response<TxBeginReply>, Status> {
        let tx_id = self.tx_id_gen.lock().unwrap().next();
        let start_ts = self.state.lock().unwrap().start_tx(tx_id);
        Ok(Response::new(TxBeginReply { tx_id, start_ts }))
    }

    /// CoordRead: route to the owning shard, resolve any NeedsInquiry loops.
    async fn read(
        &self,
        request: Request<TxReadRequest>,
    ) -> Result<Response<TxReadReply>, Status> {
        let req = request.into_inner();
        let tx_id = req.tx_id;
        let key = req.key;

        let start_ts = self
            .state
            .lock()
            .unwrap()
            .start_ts(tx_id)
            .ok_or_else(|| Status::not_found("unknown tx_id"))?;

        let shard_port = self
            .router
            .shard_for_key(key)
            .ok_or_else(|| Status::unavailable("no shards registered"))?
            .port() as u64;
        let mut client = self.shard_client(shard_port)?;

        use crate::proto::read_reply::Result as R;
        use crate::proto::tx_read_reply::Result as TR;

        let mut inquiry_results: Vec<InquiryResult> = vec![];
        loop {
            let reply = client
                .read(ReadRequest { tx_id, start_ts, key, inquiry_results: inquiry_results.clone() })
                .await?
                .into_inner();

            match reply.result {
                Some(R::Value(v)) => {
                    return Ok(Response::new(TxReadReply { result: Some(TR::Value(v)) }));
                }
                Some(R::NotFound(_)) => {
                    return Ok(Response::new(TxReadReply { result: Some(TR::NotFound(true)) }));
                }
                Some(R::Abort(_)) => {
                    self.abort_and_notify(tx_id).await;
                    return Ok(Response::new(TxReadReply { result: Some(TR::Abort(Abort {})) }));
                }
                Some(R::NeedsInquiry(NeedsInquirySet { tx_ids })) => {
                    for id in tx_ids {
                        let status = self.resolve_inquiry(id, start_ts).await?;
                        let proto_status = match status {
                            InquiryStatus::Committed(ts) => {
                                crate::proto::inquiry_result::Status::CommittedAt(ts)
                            }
                            InquiryStatus::Active => {
                                crate::proto::inquiry_result::Status::Active(Active {})
                            }
                        };
                        match inquiry_results.iter_mut().find(|r| r.tx_id == id) {
                            Some(r) => r.status = Some(proto_status),
                            None => inquiry_results
                                .push(InquiryResult { tx_id: id, status: Some(proto_status) }),
                        }
                    }
                }
                None => return Err(Status::internal("shard returned empty read result")),
            }
        }
    }

    /// CoordUpdate: route to owning shard, track participant, propagate abort.
    async fn update(
        &self,
        request: Request<TxUpdateRequest>,
    ) -> Result<Response<TxUpdateReply>, Status> {
        let req = request.into_inner();
        let tx_id = req.tx_id;
        let key = req.key;
        let value = req.value;

        let start_ts = self
            .state
            .lock()
            .unwrap()
            .start_ts(tx_id)
            .ok_or_else(|| Status::not_found("unknown tx_id"))?;

        let shard_port = self
            .router
            .shard_for_key(key)
            .ok_or_else(|| Status::unavailable("no shards registered"))?
            .port() as u64;

        self.state.lock().unwrap().add_participant(tx_id, shard_port);

        let mut client = self.shard_client(shard_port)?;
        let reply = client
            .update(UpdateRequest { tx_id, start_ts, key, value })
            .await?
            .into_inner();

        use crate::proto::tx_update_reply::Result as TR;
        use crate::proto::update_reply::Result as R;

        let result = match reply.result {
            Some(R::Ok(_)) => TR::Ok(true),
            Some(R::Abort(_)) => {
                self.abort_and_notify(tx_id).await;
                TR::Abort(Abort {})
            }
            None => return Err(Status::internal("shard returned empty update result")),
        };
        Ok(Response::new(TxUpdateReply { result: Some(result) }))
    }

    /// Commit a transaction.
    ///
    /// If the transaction touches exactly one shard, the single-shard fast path
    /// is used: a single `FastCommit` RPC atomically prepares and commits,
    /// saving one network round trip compared to full 2PC.
    ///
    /// For multi-shard transactions the standard 2PC path is used: PREPARE to
    /// all shards concurrently, then COMMIT concurrently once commit_ts is known.
    async fn commit(
        &self,
        request: Request<TxCommitRequest>,
    ) -> Result<Response<TxCommitReply>, Status> {
        let tx_id = request.into_inner().tx_id;

        use crate::proto::tx_commit_reply::Result as TR;

        // ── Fast path: single-shard transaction ──────────────────────────────

        let fast_shard = self.state.lock().unwrap().begin_fast_commit(tx_id);
        match fast_shard {
            BeginFastCommitResult::Ok(shard_port) => {
                let mut client = self.shard_client(shard_port)?;
                let reply = client
                    .fast_commit(ShardFastCommitRequest { tx_id })
                    .await?
                    .into_inner();

                use crate::proto::fast_commit_reply::Result as R;
                return match reply.result {
                    Some(R::CommitTs(commit_ts)) => {
                        match self.state.lock().unwrap().finalize_fast_commit(tx_id, commit_ts) {
                            FinalizeFastCommitResult::Ok => {}
                            FinalizeFastCommitResult::NotReady => {
                                return Err(Status::internal("unexpected state after fast commit"));
                            }
                        }
                        Ok(Response::new(TxCommitReply { result: Some(TR::CommitTs(commit_ts)) }))
                    }
                    Some(R::Abort(_)) => {
                        self.abort_and_notify(tx_id).await;
                        Ok(Response::new(TxCommitReply { result: Some(TR::Abort(Abort {})) }))
                    }
                    None => Err(Status::internal("shard returned empty fast commit reply")),
                };
            }
            BeginFastCommitResult::Aborted => {
                return Ok(Response::new(TxCommitReply { result: Some(TR::Abort(Abort {})) }));
            }
            BeginFastCommitResult::NotSingleShard => {
                // Fall through to standard 2PC below.
            }
        }

        // ── Standard 2PC path: multi-shard transaction ───────────────────────
        // ── Phase 1: begin commit ────────────────────────────────────────────

        let participants: Vec<u64> = {
            let mut state = self.state.lock().unwrap();
            match state.begin_commit(tx_id) {
                BeginCommitResult::Prepare(p) => p.into_iter().collect(),
                BeginCommitResult::Aborted => {
                    return Ok(Response::new(TxCommitReply { result: Some(TR::Abort(Abort {})) }));
                }
                BeginCommitResult::NoParticipants => {
                    return Err(Status::failed_precondition("commit with no participant shards"));
                }
            }
        };

        // ── Phase 2: PREPARE concurrently ────────────────────────────────────

        let prepare_futs = participants.iter().map(|&port| {
            let mut client = match self.shard_clients.get(&port) {
                Some(c) => c.clone(),
                None => {
                    return futures::future::Either::Left(async move {
                        Err::<(u64, Option<u64>), Status>(Status::internal(format!(
                            "no client for shard port {port}"
                        )))
                    });
                }
            };
            futures::future::Either::Right(async move {
                let reply = client.prepare(PrepareRequest { tx_id }).await?.into_inner();
                use crate::proto::prepare_reply::Result as R;
                let ts = match reply.result {
                    Some(R::Timestamp(t)) => Some(t),
                    Some(R::Abort(_)) => None,
                    None => return Err(Status::internal("empty prepare reply")),
                };
                Ok((port, ts))
            })
        });
        let prepare_results = join_all(prepare_futs).await;

        // ── Phase 3: collect replies into state machine ───────────────────────

        let mut commit_ts: Option<u64> = None;
        let mut prepare_aborted = false;
        for result in prepare_results {
            let (port, ts) = result?;
            match self.state.lock().unwrap().collect_prepare_reply(tx_id, port, ts) {
                CollectPrepareResult::Done { commit_ts: ct, .. } => commit_ts = Some(ct),
                CollectPrepareResult::Aborted => {
                    prepare_aborted = true;
                    break;
                }
                CollectPrepareResult::NeedMore => {}
            }
        }

        if prepare_aborted || commit_ts.is_none() {
            let futs = participants.iter().filter_map(|&port| {
                self.shard_clients.get(&port).map(|c| {
                    let mut client = c.clone();
                    async move { let _ = client.abort(ShardAbortRequest { tx_id }).await; }
                })
            });
            join_all(futs).await;
            return Ok(Response::new(TxCommitReply { result: Some(TR::Abort(Abort {})) }));
        }

        let commit_ts = commit_ts.unwrap();

        // ── Phase 4: transition to COMMITTED ─────────────────────────────────

        match self.state.lock().unwrap().send_commit(tx_id) {
            SendCommitResult::Ok { .. } => {}
            SendCommitResult::NotReady => {
                return Err(Status::internal("unexpected state after prepare"));
            }
        }

        // ── Phase 5: COMMIT concurrently ─────────────────────────────────────

        let commit_futs = participants.iter().filter_map(|&port| {
            self.shard_clients.get(&port).map(|c| {
                let mut client = c.clone();
                async move {
                    let _ = client.commit(ShardCommitRequest { tx_id, commit_ts }).await;
                }
            })
        });
        join_all(commit_futs).await;

        Ok(Response::new(TxCommitReply { result: Some(TR::CommitTs(commit_ts)) }))
    }

    /// CoordAbortOnReply (manual): abort state and notify all participant shards.
    async fn abort(
        &self,
        request: Request<TxAbortRequest>,
    ) -> Result<Response<TxAbortReply>, Status> {
        self.abort_and_notify(request.into_inner().tx_id).await;
        Ok(Response::new(TxAbortReply {}))
    }

    /// CoordHandleInquire: answer a shard's question about tx_id commit status.
    async fn inquire(
        &self,
        request: Request<InquireRequest>,
    ) -> Result<Response<InquireReply>, Status> {
        let req = request.into_inner();
        let status = self.state.lock().unwrap().handle_inquire(req.tx_id, req.prep_ts);
        use crate::proto::inquire_reply::Status as S;
        let proto_status = match status {
            InquiryStatus::Committed(ts) => S::CommittedAt(ts),
            InquiryStatus::Active => S::Active(Active {}),
        };
        Ok(Response::new(InquireReply { status: Some(proto_status) }))
    }
}
