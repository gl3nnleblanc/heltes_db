use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use parking_lot::Mutex;

use futures::future::join_all;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Response, Status};

use crate::coordinator::{
    coord_port_from_tx_id, routing::ConsistentHashRouter, BeginCommitResult, BeginFastCommitResult,
    CollectPrepareResult, CoordinatorState, FinalizeFastCommitResult, ReadRetryPolicy,
    SendCommitResult, TxIdGen, TxPhase,
};
use crate::proto::coordinator_service_client::CoordinatorServiceClient;
use crate::proto::coordinator_service_server::CoordinatorService;
use crate::proto::shard_service_client::ShardServiceClient;
use crate::proto::{
    Abort, AbortRequest as ShardAbortRequest, Active, CommitRequest as ShardCommitRequest,
    FastCommitRequest as ShardFastCommitRequest, GetClockRequest, InquireReply, InquireRequest,
    InquiryResult, NeedsInquirySet, PrepareRequest, ReadRequest, TxAbortReply, TxAbortRequest,
    TxBeginReply, TxBeginRequest, TxCommitReply, TxCommitRequest, TxReadReply, TxReadRequest,
    TxUpdateReply, TxUpdateRequest, TxWriteAndCommitReply, TxWriteAndCommitRequest, UpdateRequest,
    WriteAndFastCommitRequest as ShardWriteAndFastCommitRequest,
};
use crate::shard::InquiryStatus;

pub use crate::proto::coordinator_service_server::CoordinatorServiceServer;

/// Maximum number of cross-coordinator inquiry forwarding hops before the request
/// is rejected with `deadline_exceeded`.  Matches `MaxHops` in the TLA+ spec.
/// Breaking A↔B circular-inquiry cycles: when hop_count reaches this limit the
/// handler refuses to answer, and the caller's read_loop_timeout aborts the tx.
pub const MAX_INQUIRY_HOPS: u32 = 2;

// ── Server struct ─────────────────────────────────────────────────────────────

pub struct CoordinatorServer {
    state: Mutex<CoordinatorState>,
    tx_id_gen: Mutex<TxIdGen>,
    router: ConsistentHashRouter,
    /// port → connected shard client (lazy connection)
    shard_clients: HashMap<u64, ShardServiceClient<Channel>>,
    my_port: u16,
    /// port → gRPC URI for peer coordinators (for cross-coordinator Inquire forwarding).
    /// Built from `peer_coordinator_addrs` supplied at construction time.
    #[allow(dead_code)] // used in tests only
    coordinator_uris: HashMap<u16, String>,
    /// port → cached gRPC client for peer coordinators.
    /// Pre-built with lazy connections so cross-coordinator Inquire RPCs reuse the
    /// existing HTTP/2 session rather than opening a new TCP connection per call.
    coordinator_clients: HashMap<u16, CoordinatorServiceClient<Channel>>,
    /// Maximum time to wait for any individual shard RPC (or the full prepare phase).
    /// A timeout is treated as an ABORT from the shard: the coordinator aborts the
    /// transaction and returns Abort to the client. Matches CoordAbortOnReply in TLA+.
    shard_rpc_timeout: Duration,
    /// Maximum wall-clock time for the entire NeedsInquiry retry loop in read().
    /// Bounds the total latency of a read even when a prepared writer's COMMIT is
    /// delayed indefinitely (e.g. coordinator crashed mid-2PC after the write
    /// committed). Matches CoordAbortReadDeadline in TLA+.
    read_loop_timeout: Duration,
    /// Exponential-backoff policy applied between NeedsInquiry retries.
    /// Prevents a thundering herd of Inquire RPCs when many readers are blocked
    /// on the same prepared writer. Matches the implementation note on
    /// CoordAbortReadDeadline in the TLA+ spec.
    read_retry_policy: ReadRetryPolicy,
}

impl CoordinatorServer {
    /// Construct a coordinator that will route to `shard_addrs` via consistent
    /// hashing and forward cross-coordinator Inquire RPCs to `peer_coordinator_addrs`.
    /// Connections to shards are lazy (established on first use).
    ///
    /// `shard_rpc_timeout` is applied to every shard RPC (individually for reads and
    /// updates; to the full `join_all` for the PREPARE phase). A timeout is treated as
    /// an ABORT, matching the `CoordAbortOnReply` path in the TLA+ spec.
    pub fn new(
        state: CoordinatorState,
        my_port: u16,
        shard_addrs: Vec<SocketAddr>,
        peer_coordinator_addrs: Vec<SocketAddr>,
        shard_rpc_timeout: Duration,
        read_loop_timeout: Duration,
        read_retry_policy: ReadRetryPolicy,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let router = ConsistentHashRouter::new(shard_addrs.iter().copied());
        let mut shard_clients = HashMap::new();
        for addr in shard_addrs {
            let channel = Endpoint::from_shared(format!("http://{addr}"))?.connect_lazy();
            shard_clients.insert(addr.port() as u64, ShardServiceClient::new(channel));
        }
        let mut coordinator_uris = HashMap::new();
        let mut coordinator_clients = HashMap::new();
        for addr in &peer_coordinator_addrs {
            let uri = format!("http://{addr}");
            let channel = Endpoint::from_shared(uri.clone())?.connect_lazy();
            coordinator_clients.insert(addr.port(), CoordinatorServiceClient::new(channel));
            coordinator_uris.insert(addr.port(), uri);
        }
        Ok(CoordinatorServer {
            state: Mutex::new(state),
            tx_id_gen: Mutex::new(TxIdGen::new(my_port)),
            router,
            shard_clients,
            my_port,
            coordinator_uris,
            coordinator_clients,
            shard_rpc_timeout,
            read_loop_timeout,
            read_retry_policy,
        })
    }

    /// Return the gRPC URI for a peer coordinator by its port, or `None` if
    /// no address was configured for that port.
    #[allow(dead_code)] // used in tests only
    pub(crate) fn coordinator_uri_for_port(&self, port: u16) -> Option<&str> {
        self.coordinator_uris.get(&port).map(String::as_str)
    }

    #[allow(clippy::result_large_err)] // Status is the tonic-mandated error type; boxing it would complicate callers
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
            return Ok(self.state.lock().handle_inquire(tx_id, reader_start_ts));
        }
        let mut client = self
            .coordinator_clients
            .get(&port)
            .ok_or_else(|| {
                Status::unavailable(format!("no address configured for coordinator port {port}"))
            })?
            .clone();
        let reply = client
            .inquire(InquireRequest {
                tx_id,
                prep_ts: reader_start_ts,
                hop_count: 0,
            })
            .await?
            .into_inner();
        use crate::proto::inquire_reply::Status as S;
        match reply.status {
            Some(S::CommittedAt(ts)) => Ok(InquiryStatus::Committed(ts)),
            Some(S::Active(_)) => Ok(InquiryStatus::Active),
            None => Err(Status::internal("empty inquire reply")),
        }
    }

    /// CoordSyncClock: query every registered shard for its current clock and advance
    /// the coordinator's own clock to `max(all s_clock values)`.
    ///
    /// Call this once at coordinator startup — before serving any client requests —
    /// to prevent CommittedConflict aborts when joining a cluster that has already
    /// processed transactions.  Without this, a fresh coordinator starts with
    /// `c_clock = 0`, assigns `start_ts ≈ 1`, and shards whose committed versions
    /// are at timestamps >> 1 reject every write with CommittedConflict.
    ///
    /// Matches the `CoordSyncClock` action added to the TLA+ spec.
    pub async fn sync_clock_from_shards(&self) {
        let futs: Vec<_> = self
            .shard_clients
            .values()
            .map(|client| {
                let mut c = client.clone();
                async move {
                    c.get_clock(GetClockRequest {})
                        .await
                        .ok()
                        .map(|r| r.into_inner().clock)
                }
            })
            .collect();
        let results = tokio::time::timeout(self.shard_rpc_timeout, join_all(futs))
            .await
            .unwrap_or_default();
        let mut state = self.state.lock();
        for clock in results.into_iter().flatten() {
            if clock > state.clock {
                state.clock = clock;
            }
        }
    }

    /// Mark `tx_id` aborted in state and best-effort ABORT to every participant shard.
    /// The abort RPCs are bounded by `shard_rpc_timeout`; hung shards are abandoned.
    /// Matches CoordAbortOnReply in the TLA+ spec.
    async fn abort_and_notify(&self, tx_id: u64) {
        let participants = self.state.lock().abort_tx(tx_id);
        let futs = participants.into_iter().filter_map(|port| {
            self.shard_clients.get(&port).map(|c| {
                let mut client = c.clone();
                async move {
                    let _ = client.abort(ShardAbortRequest { tx_id }).await;
                }
            })
        });
        // Best-effort: don't block indefinitely if shards are unresponsive.
        let _ = tokio::time::timeout(self.shard_rpc_timeout, join_all(futs)).await;
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
        let tx_id = self
            .tx_id_gen
            .lock()
            .next()
            .expect("TxIdGen never exhausts");
        let start_ts = self.state.lock().start_tx(tx_id);
        Ok(Response::new(TxBeginReply { tx_id, start_ts }))
    }

    /// CoordRead: route to the owning shard, resolve any NeedsInquiry loops.
    ///
    /// The NeedsInquiry retry loop is bounded by `read_loop_timeout`: if the loop
    /// has not produced a Value/NotFound result within that wall-clock window the
    /// coordinator aborts the transaction and returns Abort to the client.
    /// Matches CoordAbortReadDeadline in the TLA+ spec.
    async fn read(&self, request: Request<TxReadRequest>) -> Result<Response<TxReadReply>, Status> {
        let req = request.into_inner();
        let tx_id = req.tx_id;
        let key = req.key;

        let start_ts = self
            .state
            .lock()
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

        let read_retry_policy = self.read_retry_policy.clone();
        let loop_result = tokio::time::timeout(self.read_loop_timeout, async move {
            let mut inquiry_results: Vec<InquiryResult> = vec![];
            let mut retry: u32 = 0;
            loop {
                let reply = tokio::time::timeout(
                    self.shard_rpc_timeout,
                    client.read(ReadRequest {
                        tx_id,
                        start_ts,
                        key,
                        inquiry_results: inquiry_results.clone(),
                    }),
                )
                .await
                .map_err(|_| Status::deadline_exceeded("shard read RPC timed out"))??
                .into_inner();

                match reply.result {
                    Some(R::Value(v)) => {
                        return Ok(Response::new(TxReadReply {
                            result: Some(TR::Value(v)),
                        }));
                    }
                    Some(R::NotFound(_)) => {
                        return Ok(Response::new(TxReadReply {
                            result: Some(TR::NotFound(true)),
                        }));
                    }
                    Some(R::Abort(_)) => {
                        self.abort_and_notify(tx_id).await;
                        return Ok(Response::new(TxReadReply {
                            result: Some(TR::Abort(Abort {})),
                        }));
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
                                None => inquiry_results.push(InquiryResult {
                                    tx_id: id,
                                    status: Some(proto_status),
                                }),
                            }
                        }
                        // Back off before retrying to avoid thundering-herd of
                        // Inquire RPCs under high prepared-writer contention.
                        // Matches the ReadRetryPolicy implementation note on
                        // CoordAbortReadDeadline in the TLA+ spec.
                        let sleep_dur = read_retry_policy.sleep_duration(retry);
                        if !sleep_dur.is_zero() {
                            tokio::time::sleep(sleep_dur).await;
                        }
                        retry = retry.saturating_add(1);
                    }
                    None => return Err(Status::internal("shard returned empty read result")),
                }
            }
        })
        .await;

        match loop_result {
            Ok(result) => result,
            Err(_elapsed) => {
                // read_loop_timeout fired — CoordAbortReadDeadline in TLA+.
                self.abort_and_notify(tx_id).await;
                Ok(Response::new(TxReadReply {
                    result: Some(TR::Abort(Abort {})),
                }))
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
            .start_ts(tx_id)
            .ok_or_else(|| Status::not_found("unknown tx_id"))?;

        let shard_port = self
            .router
            .shard_for_key(key)
            .ok_or_else(|| Status::unavailable("no shards registered"))?
            .port() as u64;

        self.state.lock().add_participant(tx_id, shard_port);

        let mut client = self.shard_client(shard_port)?;
        let reply = tokio::time::timeout(
            self.shard_rpc_timeout,
            client.update(UpdateRequest {
                tx_id,
                start_ts,
                key,
                value,
            }),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("shard update RPC timed out"))??
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
        Ok(Response::new(TxUpdateReply {
            result: Some(result),
        }))
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

        let fast_shard = self.state.lock().begin_fast_commit(tx_id);
        match fast_shard {
            BeginFastCommitResult::Ok(shard_port) => {
                let mut client = self.shard_client(shard_port)?;
                let fast_commit_result = tokio::time::timeout(
                    self.shard_rpc_timeout,
                    client.fast_commit(ShardFastCommitRequest { tx_id }),
                )
                .await;
                let reply = match fast_commit_result {
                    Ok(Ok(r)) => r.into_inner(),
                    Ok(Err(e)) => return Err(e),
                    Err(_elapsed) => {
                        // Shard did not respond in time — treat as ABORT.
                        self.abort_and_notify(tx_id).await;
                        return Ok(Response::new(TxCommitReply {
                            result: Some(TR::Abort(Abort {})),
                        }));
                    }
                };

                use crate::proto::fast_commit_reply::Result as R;
                return match reply.result {
                    Some(R::CommitTs(commit_ts)) => {
                        match self.state.lock().finalize_fast_commit(tx_id, commit_ts) {
                            FinalizeFastCommitResult::Ok => {}
                            FinalizeFastCommitResult::NotReady => {
                                return Err(Status::internal("unexpected state after fast commit"));
                            }
                        }
                        Ok(Response::new(TxCommitReply {
                            result: Some(TR::CommitTs(commit_ts)),
                        }))
                    }
                    Some(R::Abort(_)) => {
                        self.abort_and_notify(tx_id).await;
                        Ok(Response::new(TxCommitReply {
                            result: Some(TR::Abort(Abort {})),
                        }))
                    }
                    None => Err(Status::internal("shard returned empty fast commit reply")),
                };
            }
            BeginFastCommitResult::Aborted => {
                return Ok(Response::new(TxCommitReply {
                    result: Some(TR::Abort(Abort {})),
                }));
            }
            BeginFastCommitResult::NotSingleShard => {
                // Fall through to standard 2PC below.
            }
        }

        // ── Standard 2PC path: multi-shard transaction ───────────────────────
        // ── Phase 1: begin commit ────────────────────────────────────────────

        let participants: Vec<u64> = {
            let mut state = self.state.lock();
            match state.begin_commit(tx_id) {
                BeginCommitResult::Prepare(p) => p.into_iter().collect(),
                BeginCommitResult::Aborted => {
                    return Ok(Response::new(TxCommitReply {
                        result: Some(TR::Abort(Abort {})),
                    }));
                }
                BeginCommitResult::NoParticipants => {
                    return Err(Status::failed_precondition(
                        "commit with no participant shards",
                    ));
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
        let prepare_results =
            match tokio::time::timeout(self.shard_rpc_timeout, join_all(prepare_futs)).await {
                Ok(results) => results,
                Err(_elapsed) => {
                    // At least one shard did not respond in time — treat as ABORT from all
                    // outstanding shards. Matches CoordAbortOnReply in the TLA+ spec.
                    self.abort_and_notify(tx_id).await;
                    return Ok(Response::new(TxCommitReply {
                        result: Some(TR::Abort(Abort {})),
                    }));
                }
            };

        // ── Phase 3: collect replies into state machine ───────────────────────

        let mut commit_ts: Option<u64> = None;
        let mut prepare_aborted = false;
        for result in prepare_results {
            let (port, ts) = result?;
            match self.state.lock().collect_prepare_reply(tx_id, port, ts) {
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
                    async move {
                        let _ = client.abort(ShardAbortRequest { tx_id }).await;
                    }
                })
            });
            // Bounded by shard_rpc_timeout: mirrors abort_and_notify.
            let _ = tokio::time::timeout(self.shard_rpc_timeout, join_all(futs)).await;
            return Ok(Response::new(TxCommitReply {
                result: Some(TR::Abort(Abort {})),
            }));
        }

        let commit_ts = commit_ts.unwrap();

        // ── Phase 4: transition to COMMITTED ─────────────────────────────────

        match self.state.lock().send_commit(tx_id) {
            SendCommitResult::Ok { .. } => {}
            SendCommitResult::NotReady => {
                return Err(Status::internal("unexpected state after prepare"));
            }
        }

        // ── Phase 5: COMMIT concurrently ─────────────────────────────────────
        // Each future resolves to `true` if the shard rejected the commit
        // (CommitResult::Abort — TTL expiry race), `false` for success or RPC error.

        let commit_futs = participants.iter().filter_map(|&port| {
            self.shard_clients.get(&port).map(|c| {
                let mut client = c.clone();
                async move {
                    let reply = client.commit(ShardCommitRequest { tx_id, commit_ts }).await;
                    use crate::proto::commit_reply::Result as R;
                    matches!(
                        reply.ok().and_then(|r| r.into_inner().result),
                        Some(R::Abort(_))
                    )
                }
            })
        });
        // Bounded by shard_rpc_timeout: the tx is already COMMITTED at the coordinator
        // (is_committed=TRUE, CoordSendCommit fired). A timeout here means some shards
        // haven't acked yet, but the CommitMsg is in flight. Return deadline_exceeded so
        // the client knows the tx is committed and can retry to observe it.
        let commit_results = tokio::time::timeout(self.shard_rpc_timeout, join_all(commit_futs))
            .await
            .map_err(|_| {
                Status::deadline_exceeded(
                    "COMMIT phase timed out; transaction is committed — client may retry",
                )
            })?;
        let any_abort = commit_results.into_iter().any(|a| a);
        if any_abort {
            return Ok(Response::new(TxCommitReply {
                result: Some(TR::Abort(Abort {})),
            }));
        }

        Ok(Response::new(TxCommitReply {
            result: Some(TR::CommitTs(commit_ts)),
        }))
    }

    /// CoordWriteAndFastCommit: single-RPC fast path for single-write single-shard transactions.
    ///
    /// Combines the client-facing Update + Commit into a single coordinator RPC, which
    /// routes to the owning shard's WriteAndFastCommit RPC (itself a single shard-level
    /// operation). Eliminates two network round trips vs. the Update + Commit path.
    ///
    /// Matches `CoordWriteAndFastCommit + ShardHandleWriteAndFastCommit` in the TLA+ spec.
    async fn write_and_commit(
        &self,
        request: Request<TxWriteAndCommitRequest>,
    ) -> Result<Response<TxWriteAndCommitReply>, Status> {
        let req = request.into_inner();
        let tx_id = req.tx_id;
        let key = req.key;
        let value = req.value;

        use crate::proto::tx_write_and_commit_reply::Result as TR;

        let start_ts = self
            .state
            .lock()
            .start_ts(tx_id)
            .ok_or_else(|| Status::not_found("unknown tx_id"))?;

        let shard_port = self
            .router
            .shard_for_key(key)
            .ok_or_else(|| Status::unavailable("no shards registered"))?
            .port() as u64;

        // Record participant and immediately transition ACTIVE → PREPARING.
        self.state.lock().add_participant(tx_id, shard_port);

        let shard_id = match self.state.lock().begin_fast_commit(tx_id) {
            BeginFastCommitResult::Ok(id) => id,
            BeginFastCommitResult::Aborted => {
                return Ok(Response::new(TxWriteAndCommitReply {
                    result: Some(TR::Abort(Abort {})),
                }));
            }
            BeginFastCommitResult::NotSingleShard => {
                // Cannot happen: we just added exactly one participant above.
                return Err(Status::internal(
                    "unexpected NotSingleShard in write_and_commit",
                ));
            }
        };

        let mut client = self.shard_client(shard_id)?;
        let rpc_result = tokio::time::timeout(
            self.shard_rpc_timeout,
            client.write_and_fast_commit(ShardWriteAndFastCommitRequest {
                tx_id,
                start_ts,
                key,
                value,
            }),
        )
        .await;

        let reply = match rpc_result {
            Ok(Ok(r)) => r.into_inner(),
            Ok(Err(e)) => return Err(e),
            Err(_elapsed) => {
                self.abort_and_notify(tx_id).await;
                return Ok(Response::new(TxWriteAndCommitReply {
                    result: Some(TR::Abort(Abort {})),
                }));
            }
        };

        use crate::proto::fast_commit_reply::Result as R;
        match reply.result {
            Some(R::CommitTs(commit_ts)) => {
                match self.state.lock().finalize_fast_commit(tx_id, commit_ts) {
                    FinalizeFastCommitResult::Ok => {}
                    FinalizeFastCommitResult::NotReady => {
                        return Err(Status::internal(
                            "unexpected state after write_and_fast_commit",
                        ));
                    }
                }
                Ok(Response::new(TxWriteAndCommitReply {
                    result: Some(TR::CommitTs(commit_ts)),
                }))
            }
            Some(R::Abort(_)) => {
                self.abort_and_notify(tx_id).await;
                Ok(Response::new(TxWriteAndCommitReply {
                    result: Some(TR::Abort(Abort {})),
                }))
            }
            None => Err(Status::internal(
                "shard returned empty write_and_fast_commit reply",
            )),
        }
    }

    /// CoordAbortOnReply (manual): abort state and notify all participant shards.
    ///
    /// If the transaction is in `Preparing` phase (a fast-commit RPC is in flight),
    /// the abort is silently ignored and `AbortReply` is still returned to the caller.
    /// This matches the TLA+ spec: `CoordAbortOnReply` requires `tx_state = "ACTIVE"`,
    /// so it cannot fire once the coordinator is in PREPARING.  Aborting a PREPARING
    /// fast-commit would leave coordinator state = Aborted while the shard may already
    /// have committed the write, creating a permanent inconsistency.
    async fn abort(
        &self,
        request: Request<TxAbortRequest>,
    ) -> Result<Response<TxAbortReply>, Status> {
        let tx_id = request.into_inner().tx_id;
        // Do not abort a tx whose fast-commit RPC is in flight (CoordAbortOnReply
        // requires ACTIVE; once in PREPARING the abort window is closed).
        if self.state.lock().tx_phase(tx_id) == Some(TxPhase::Preparing) {
            return Ok(Response::new(TxAbortReply {}));
        }
        self.abort_and_notify(tx_id).await;
        Ok(Response::new(TxAbortReply {}))
    }

    /// CoordHandleInquire: answer a shard's question about tx_id commit status.
    ///
    /// Rejects requests with `hop_count >= MAX_INQUIRY_HOPS` to break circular
    /// inquiry deadlock cycles (see `CoordHandleInquire` in the TLA+ spec).
    async fn inquire(
        &self,
        request: Request<InquireRequest>,
    ) -> Result<Response<InquireReply>, Status> {
        let req = request.into_inner();
        if req.hop_count >= MAX_INQUIRY_HOPS {
            return Err(Status::deadline_exceeded(format!(
                "inquiry hop limit exceeded (hop_count={} >= MAX_INQUIRY_HOPS={})",
                req.hop_count, MAX_INQUIRY_HOPS,
            )));
        }
        let status = self.state.lock().handle_inquire(req.tx_id, req.prep_ts);
        use crate::proto::inquire_reply::Status as S;
        let proto_status = match status {
            InquiryStatus::Committed(ts) => S::CommittedAt(ts),
            InquiryStatus::Active => S::Active(Active {}),
        };
        Ok(Response::new(InquireReply {
            status: Some(proto_status),
        }))
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordinator::CoordinatorState;
    use crate::proto::shard_service_server::{ShardService, ShardServiceServer as ShardSvcServer};
    use crate::proto::{
        AbortReply, CommitReply, FastCommitReply, GetClockReply, ReadReply, UpdateReply,
    };

    fn test_server(my_port: u16, peers: Vec<SocketAddr>) -> CoordinatorServer {
        CoordinatorServer::new(
            CoordinatorState::new(),
            my_port,
            vec![],
            peers,
            Duration::from_secs(5),
            Duration::from_secs(5),
            ReadRetryPolicy::no_backoff(),
        )
        .unwrap()
    }

    /// Spawn a TCP listener that accepts connections but never sends any bytes,
    /// simulating a shard that is reachable at the transport layer but hangs on RPC.
    async fn spawn_hang_server() -> SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let mut held = vec![];
            loop {
                match listener.accept().await {
                    Ok((socket, _)) => held.push(socket),
                    Err(_) => break,
                }
            }
        });
        addr
    }

    // Trace: remote IPv4 peer in coordinator_uris → URI uses the configured address.
    #[tokio::test]
    async fn coordinator_uri_for_known_ipv4_peer() {
        let server = test_server(50052, vec!["192.0.2.1:50053".parse().unwrap()]);
        assert_eq!(
            server.coordinator_uri_for_port(50053),
            Some("http://192.0.2.1:50053"),
        );
    }

    // Trace: remote IPv6 peer → URI wraps host in brackets (std::net formatting).
    #[tokio::test]
    async fn coordinator_uri_for_known_ipv6_peer() {
        let server = test_server(50052, vec!["[::2]:50053".parse().unwrap()]);
        assert_eq!(
            server.coordinator_uri_for_port(50053),
            Some("http://[::2]:50053"),
        );
    }

    // Trace: port not present in coordinator_uris → None.
    #[test]
    fn coordinator_uri_for_unknown_peer_returns_none() {
        let server = test_server(50052, vec![]);
        assert_eq!(server.coordinator_uri_for_port(50099), None);
    }

    // Trace: multiple peers → each independently addressable by port.
    #[tokio::test]
    async fn coordinator_uri_for_multiple_peers() {
        let server = test_server(
            50052,
            vec![
                "192.0.2.1:50053".parse().unwrap(),
                "192.0.2.2:50054".parse().unwrap(),
            ],
        );
        assert_eq!(
            server.coordinator_uri_for_port(50053),
            Some("http://192.0.2.1:50053"),
        );
        assert_eq!(
            server.coordinator_uri_for_port(50054),
            Some("http://192.0.2.2:50054"),
        );
    }

    // Trace: resolve_inquiry for own tx_id is handled in-process (no URI lookup).
    // We can verify this by using a tx_id whose port encodes my_port.
    #[tokio::test]
    async fn resolve_inquiry_for_own_tx_id_answers_in_process() {
        let server = test_server(50052, vec![]);
        // Encode my_port=50052 into tx_id high 32 bits.
        let tx_id: u64 = (50052u64 << 32) | 1;
        // The tx is not in state (unknown), so handle_inquire returns Active.
        let result = server.resolve_inquiry(tx_id, 1).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), InquiryStatus::Active);
    }

    // Trace: resolve_inquiry for a tx whose port is not in coordinator_uris →
    // returns Status::unavailable immediately (does not attempt [::1]).
    #[tokio::test]
    async fn resolve_inquiry_for_unknown_remote_port_returns_unavailable() {
        let server = test_server(50052, vec![]);
        // Port 50099 is not my_port and not in coordinator_uris.
        let tx_id: u64 = (50099u64 << 32) | 1;
        let result = server.resolve_inquiry(tx_id, 1).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::Unavailable);
    }

    // Trace: resolve_inquiry for a configured remote port attempts the right URI
    // (not [::1]). Use 127.0.0.1 at an unused port so the OS immediately
    // refuses the connection (ECONNREFUSED), keeping the test fast.
    // The error must NOT say "no address configured" — it must be a
    // connection-level error, proving the code looked up the configured URI.
    #[tokio::test]
    async fn resolve_inquiry_for_configured_peer_uses_correct_address() {
        // Port 59999 on loopback — almost certainly not in use; OS refuses instantly.
        let peer_addr: SocketAddr = "127.0.0.1:59999".parse().unwrap();
        let server = test_server(50052, vec![peer_addr]);
        let tx_id: u64 = (59999u64 << 32) | 1;
        let result = server.resolve_inquiry(tx_id, 1).await;
        let err = result.unwrap_err();
        // Must NOT be the "no address configured" error (which would mean the
        // code fell back to hardcoded [::1] or skipped the lookup).
        let msg = err.message();
        assert!(
            !msg.contains("no address configured"),
            "expected connection attempt, got: {msg}",
        );
    }

    // Trace: shard_rpc_timeout is stored and retrievable.
    #[test]
    fn shard_rpc_timeout_is_stored() {
        let timeout = Duration::from_secs(7);
        let server = CoordinatorServer::new(
            CoordinatorState::new(),
            50052,
            vec![],
            vec![],
            timeout,
            Duration::from_secs(5),
            ReadRetryPolicy::no_backoff(),
        )
        .unwrap();
        assert_eq!(server.shard_rpc_timeout, timeout);
    }

    // Trace: CoordBeginCommit → PREPARING → deadline fires before all PREPARE replies
    // arrive → coordinator aborts (CoordAbortOnReply path in TLA+).
    //
    // Two hang shards → 2 participants → 2PC path (not fast-commit).
    // The coordinator's shard_rpc_timeout is set to 20 ms; the test waits for
    // the commit future to complete, which happens once the timeout fires.
    #[tokio::test]
    async fn prepare_timeout_aborts_transaction() {
        let hang1 = spawn_hang_server().await;
        let hang2 = spawn_hang_server().await;
        let p1 = hang1.port() as u64;
        let p2 = hang2.port() as u64;

        let server = CoordinatorServer::new(
            CoordinatorState::new(),
            50052,
            vec![hang1, hang2],
            vec![],
            Duration::from_millis(20),
            Duration::from_secs(5),
            ReadRetryPolicy::no_backoff(),
        )
        .unwrap();

        // Register a transaction with 2 participants directly, bypassing begin/update RPCs.
        let tx_id: u64 = (50052u64 << 32) | 1;
        {
            let mut state = server.state.lock();
            state.start_tx(tx_id);
            state.add_participant(tx_id, p1);
            state.add_participant(tx_id, p2);
        }

        // commit() internally runs join_all(prepare_futs) wrapped in shard_rpc_timeout.
        // With two hung shards, the timeout fires and the coordinator returns Abort.
        let reply = server
            .commit(Request::new(TxCommitRequest { tx_id }))
            .await
            .unwrap()
            .into_inner();

        use crate::proto::tx_commit_reply::Result as TR;
        assert_eq!(reply.result, Some(TR::Abort(Abort {})));
    }

    // Trace: CoordFastCommit → PREPARING → deadline fires before FastCommitReply
    // arrives → coordinator aborts (same CoordAbortOnReply path in TLA+).
    //
    // One hang shard → 1 participant → fast-commit path.
    #[tokio::test]
    async fn fast_commit_timeout_aborts_transaction() {
        let hang = spawn_hang_server().await;
        let port = hang.port() as u64;

        let server = CoordinatorServer::new(
            CoordinatorState::new(),
            50052,
            vec![hang],
            vec![],
            Duration::from_millis(20),
            Duration::from_secs(5),
            ReadRetryPolicy::no_backoff(),
        )
        .unwrap();

        let tx_id: u64 = (50052u64 << 32) | 2;
        {
            let mut state = server.state.lock();
            state.start_tx(tx_id);
            state.add_participant(tx_id, port);
        }

        // With 1 participant the fast-commit path is taken. The hang shard never
        // replies, so shard_rpc_timeout fires and the coordinator returns Abort.
        let reply = server
            .commit(Request::new(TxCommitRequest { tx_id }))
            .await
            .unwrap()
            .into_inner();

        use crate::proto::tx_commit_reply::Result as TR;
        assert_eq!(reply.result, Some(TR::Abort(Abort {})));
    }

    // ── write_and_commit tests ────────────────────────────────────────────────

    // Trace WF1: write_and_commit with a hung shard → shard_rpc_timeout fires → Abort.
    // (Same pattern as fast_commit_timeout_aborts_transaction, but via WriteAndCommit path.)
    #[tokio::test]
    async fn write_and_commit_timeout_aborts_transaction() {
        let hang = spawn_hang_server().await;

        let server = CoordinatorServer::new(
            CoordinatorState::new(),
            50052,
            vec![hang],
            vec![],
            Duration::from_millis(20),
            Duration::from_secs(5),
            ReadRetryPolicy::no_backoff(),
        )
        .unwrap();

        let tx_id: u64 = (50052u64 << 32) | 5;
        server.state.lock().start_tx(tx_id);

        let key = 42u64; // routes to the only shard (hang)
        let reply = server
            .write_and_commit(Request::new(TxWriteAndCommitRequest {
                tx_id,
                key,
                value: 100,
            }))
            .await
            .unwrap()
            .into_inner();

        use crate::proto::tx_write_and_commit_reply::Result as TR;
        assert_eq!(reply.result, Some(TR::Abort(Abort {})));
    }

    // Trace WF2: write_and_commit on unknown tx_id → Status::not_found.
    #[tokio::test]
    async fn write_and_commit_unknown_tx_returns_not_found() {
        let server = test_server(50052, vec![]);
        let tx_id: u64 = (50052u64 << 32) | 99;
        // tx_id was never registered via start_tx.
        let result = server
            .write_and_commit(Request::new(TxWriteAndCommitRequest {
                tx_id,
                key: 1,
                value: 1,
            }))
            .await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::NotFound);
    }

    // Trace WF3: write_and_commit with no shards registered → Status::unavailable.
    #[tokio::test]
    async fn write_and_commit_no_shards_returns_unavailable() {
        let server = test_server(50052, vec![]);
        let tx_id: u64 = (50052u64 << 32) | 6;
        server.state.lock().start_tx(tx_id);
        let result = server
            .write_and_commit(Request::new(TxWriteAndCommitRequest {
                tx_id,
                key: 1,
                value: 1,
            }))
            .await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::Unavailable);
    }

    // Trace WF4: after write_and_commit timeout, the tx is aborted in coordinator state.
    #[tokio::test]
    async fn write_and_commit_timeout_marks_tx_aborted() {
        let hang = spawn_hang_server().await;

        let server = CoordinatorServer::new(
            CoordinatorState::new(),
            50052,
            vec![hang],
            vec![],
            Duration::from_millis(20),
            Duration::from_secs(5),
            ReadRetryPolicy::no_backoff(),
        )
        .unwrap();

        let tx_id: u64 = (50052u64 << 32) | 7;
        server.state.lock().start_tx(tx_id);

        server
            .write_and_commit(Request::new(TxWriteAndCommitRequest {
                tx_id,
                key: 1,
                value: 1,
            }))
            .await
            .unwrap();

        // After abort, coordinator phase must be Aborted.
        assert_eq!(
            server.state.lock().tx_phase(tx_id),
            Some(TxPhase::Aborted),
            "tx must be aborted after write_and_commit timeout"
        );
    }

    // Trace 4: read_loop_timeout is stored on the struct.
    #[test]
    fn read_loop_timeout_is_stored() {
        let rlt = Duration::from_secs(42);
        let server = CoordinatorServer::new(
            CoordinatorState::new(),
            50052,
            vec![],
            vec![],
            Duration::from_secs(5),
            rlt,
            ReadRetryPolicy::no_backoff(),
        )
        .unwrap();
        assert_eq!(server.read_loop_timeout, rlt);
    }

    // Trace T2: read_retry_policy is stored and accessible.
    #[test]
    fn read_retry_policy_is_stored() {
        let policy = ReadRetryPolicy::new(
            Duration::from_millis(7),
            Duration::from_millis(50),
            Duration::from_millis(3),
        );
        let server = CoordinatorServer::new(
            CoordinatorState::new(),
            50052,
            vec![],
            vec![],
            Duration::from_secs(5),
            Duration::from_secs(5),
            policy.clone(),
        )
        .unwrap();
        assert_eq!(server.read_retry_policy, policy);
    }

    // Trace 2: CoordAbortReadDeadline — read_loop_timeout fires before the shard
    // replies. With read_loop_timeout < shard_rpc_timeout and a hung shard, the
    // outer deadline cancels the loop first, causing the coordinator to abort the
    // read transaction and return Abort to the client.
    #[tokio::test]
    async fn read_loop_deadline_aborts_when_shard_hangs() {
        let hang = spawn_hang_server().await;

        let server = CoordinatorServer::new(
            CoordinatorState::new(),
            50052,
            vec![hang],
            vec![],
            Duration::from_millis(200), // shard_rpc_timeout (inner — fires later)
            Duration::from_millis(20),  // read_loop_timeout (outer — fires first)
            ReadRetryPolicy::no_backoff(),
        )
        .unwrap();

        let tx_id: u64 = (50052u64 << 32) | 3;
        server.state.lock().start_tx(tx_id);

        let reply = server
            .read(Request::new(TxReadRequest { tx_id, key: 1 }))
            .await
            .unwrap()
            .into_inner();

        use crate::proto::tx_read_reply::Result as TR;
        assert_eq!(reply.result, Some(TR::Abort(Abort {})));
    }

    // Trace T3: CoordAbortOnReply disabled during PREPARING — a client abort() RPC
    // on a transaction in Preparing phase (fast-commit in-flight) must be ignored.
    // The abort RPC returns success (idempotent) but the coordinator phase stays Preparing
    // so that finalize_fast_commit can still transition it to Committed when the shard
    // replies.  This prevents the race where coordinator = Aborted while shard = Committed.
    #[tokio::test]
    async fn abort_rpc_is_no_op_when_tx_is_preparing() {
        use crate::coordinator::TxPhase;

        let server = test_server(50052, vec![]);

        let tx_id: u64 = (50052u64 << 32) | 10;
        {
            let mut state = server.state.lock();
            state.start_tx(tx_id);
            state.add_participant(tx_id, 9999); // arbitrary shard port

            // Manually drive the tx into Preparing (as begin_fast_commit would).
            // We call begin_fast_commit via the state machine.
            use crate::coordinator::BeginFastCommitResult;
            let r = state.begin_fast_commit(tx_id);
            assert!(
                matches!(r, BeginFastCommitResult::Ok(_)),
                "begin_fast_commit must succeed"
            );
        }

        // Verify phase is Preparing before the abort arrives.
        assert_eq!(
            server.state.lock().tx_phase(tx_id),
            Some(TxPhase::Preparing),
            "phase must be Preparing after begin_fast_commit"
        );

        // Client sends an abort RPC (e.g. client-side timeout while commit is in-flight).
        let reply = server
            .abort(Request::new(TxAbortRequest { tx_id }))
            .await
            .unwrap()
            .into_inner();
        let _ = reply; // AbortReply has no fields; just verify it succeeds

        // Phase must remain Preparing — the abort must not preempt the in-flight fast-commit.
        assert_eq!(
            server.state.lock().tx_phase(tx_id),
            Some(TxPhase::Preparing),
            "abort RPC must not change phase from Preparing (CoordAbortOnReply requires ACTIVE)"
        );
    }

    // Trace T1: hop_count=0 < MAX_INQUIRY_HOPS → CoordHandleInquire enabled → returns Ok.
    #[tokio::test]
    async fn inquire_with_zero_hop_count_succeeds() {
        let server = test_server(50052, vec![]);
        let reply = server
            .inquire(Request::new(InquireRequest {
                tx_id: 1,
                prep_ts: 0,
                hop_count: 0,
            }))
            .await;
        assert!(reply.is_ok(), "hop_count=0 must succeed");
    }

    // Trace T2: hop_count=MAX-1 → last allowed hop → returns Ok.
    #[tokio::test]
    async fn inquire_with_hop_count_just_below_max_succeeds() {
        let server = test_server(50052, vec![]);
        let reply = server
            .inquire(Request::new(InquireRequest {
                tx_id: 1,
                prep_ts: 0,
                hop_count: MAX_INQUIRY_HOPS - 1,
            }))
            .await;
        assert!(
            reply.is_ok(),
            "hop_count=MAX-1 must succeed (last allowed hop)"
        );
    }

    // Trace T3: hop_count=MAX_INQUIRY_HOPS → CoordHandleInquire disabled →
    // returns deadline_exceeded, which propagates as an error in the read() loop
    // and causes CoordAbortReadDeadline to fire for the blocked reader.
    #[tokio::test]
    async fn inquire_with_hop_count_at_max_returns_deadline_exceeded() {
        let server = test_server(50052, vec![]);
        let reply = server
            .inquire(Request::new(InquireRequest {
                tx_id: 1,
                prep_ts: 0,
                hop_count: MAX_INQUIRY_HOPS,
            }))
            .await;
        assert!(reply.is_err(), "hop_count=MAX must be rejected");
        assert_eq!(
            reply.unwrap_err().code(),
            tonic::Code::DeadlineExceeded,
            "must return deadline_exceeded, not another error"
        );
    }

    // ── Timeout-boundary tests ────────────────────────────────────────────────

    /// A minimal ShardService that answers Prepare with timestamp=1 and hangs on
    /// every other RPC. Models a shard that responds to PREPARE but then becomes
    /// unresponsive before the COMMIT message arrives.
    ///
    /// Used to verify that the coordinator's COMMIT-phase join_all is bounded by
    /// shard_rpc_timeout (the bug: without the fix, commit() blocks forever once
    /// the COMMIT RPCs are dispatched, even though the tx is already COMMITTED at
    /// the coordinator).
    struct PrepareOkCommitHangShard;

    #[tonic::async_trait]
    impl ShardService for PrepareOkCommitHangShard {
        async fn read(&self, _: Request<ReadRequest>) -> Result<Response<ReadReply>, Status> {
            futures::future::pending().await
        }
        async fn update(&self, _: Request<UpdateRequest>) -> Result<Response<UpdateReply>, Status> {
            futures::future::pending().await
        }
        async fn prepare(
            &self,
            _: Request<PrepareRequest>,
        ) -> Result<Response<crate::proto::PrepareReply>, Status> {
            use crate::proto::prepare_reply::Result as R;
            Ok(Response::new(crate::proto::PrepareReply {
                result: Some(R::Timestamp(1)),
            }))
        }
        async fn commit(
            &self,
            _: Request<ShardCommitRequest>,
        ) -> Result<Response<CommitReply>, Status> {
            futures::future::pending().await // hangs — this is what we're testing against
        }
        async fn abort(
            &self,
            _: Request<ShardAbortRequest>,
        ) -> Result<Response<AbortReply>, Status> {
            futures::future::pending().await
        }
        async fn fast_commit(
            &self,
            _: Request<ShardFastCommitRequest>,
        ) -> Result<Response<FastCommitReply>, Status> {
            futures::future::pending().await
        }
        async fn write_and_fast_commit(
            &self,
            _: Request<ShardWriteAndFastCommitRequest>,
        ) -> Result<Response<FastCommitReply>, Status> {
            futures::future::pending().await
        }
        async fn get_clock(
            &self,
            _: Request<GetClockRequest>,
        ) -> Result<Response<GetClockReply>, Status> {
            Ok(Response::new(GetClockReply { clock: 0 }))
        }
    }

    /// Bind a random port and start a `PrepareOkCommitHangShard` on it.
    async fn spawn_prepare_ok_commit_hang_shard() -> SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let incoming = futures::stream::unfold(listener, |l| async move {
            let r = l.accept().await.map(|(s, _)| s);
            Some((r, l))
        });
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(ShardSvcServer::new(PrepareOkCommitHangShard))
                .serve_with_incoming(incoming),
        );
        tokio::time::sleep(Duration::from_millis(5)).await; // let server start
        addr
    }

    // Trace: COMMIT phase bounded by shard_rpc_timeout.
    //
    // Both participant shards respond to PREPARE with a timestamp (fast), but
    // then hang indefinitely on COMMIT. Without the fix, commit() blocks forever
    // once CoordSendCommit fires. With the fix, commit() returns
    // deadline_exceeded within ~shard_rpc_timeout after dispatching COMMIT.
    //
    // The tx IS committed at the coordinator after CoordSendCommit; the error
    // tells the client the acknowledgement timed out (retriable).
    #[tokio::test]
    async fn commit_phase_timeout_returns_deadline_exceeded() {
        let s1 = spawn_prepare_ok_commit_hang_shard().await;
        let s2 = spawn_prepare_ok_commit_hang_shard().await;
        let p1 = s1.port() as u64;
        let p2 = s2.port() as u64;

        let server = CoordinatorServer::new(
            CoordinatorState::new(),
            50052,
            vec![s1, s2],
            vec![],
            Duration::from_millis(50), // shard_rpc_timeout
            Duration::from_secs(5),
            ReadRetryPolicy::no_backoff(),
        )
        .unwrap();

        let tx_id: u64 = (50052u64 << 32) | 20;
        {
            let mut state = server.state.lock();
            state.start_tx(tx_id);
            state.add_participant(tx_id, p1);
            state.add_participant(tx_id, p2);
        }

        // Prepare phase completes quickly (mock shards respond). COMMIT hangs.
        // The coordinator should return deadline_exceeded within shard_rpc_timeout.
        let result = tokio::time::timeout(
            Duration::from_millis(500), // outer guard so test doesn't hang forever
            server.commit(Request::new(TxCommitRequest { tx_id })),
        )
        .await
        .expect("commit() must not hang — must be bounded by shard_rpc_timeout");

        assert!(
            result.is_err(),
            "timed-out COMMIT phase must return an error"
        );
        assert_eq!(
            result.unwrap_err().code(),
            tonic::Code::DeadlineExceeded,
            "must be deadline_exceeded so the client knows the tx is committed and can retry"
        );
    }

    // Trace: post-prepare-abort ABORT notification is bounded by shard_rpc_timeout.
    //
    // When prepare succeeds (all shards respond) but one returns Abort, the
    // coordinator sends ABORT to all participants. Without the fix, the
    // join_all(abort_futs) inside the prepare-aborted branch has no timeout and
    // stalls indefinitely if any shard becomes unresponsive after prepare.
    //
    // We cannot drive the inline path from commit() without shards that respond
    // to PREPARE with Abort then hang on ABORT — so we test abort_and_notify
    // (which uses the same bounded pattern) to verify the bounding works, and
    // separately confirm the inline path uses `tokio::time::timeout` by code
    // inspection (the change mirrors abort_and_notify exactly).
    #[tokio::test]
    async fn abort_and_notify_bounded_by_shard_rpc_timeout() {
        let hang1 = spawn_hang_server().await;
        let hang2 = spawn_hang_server().await;

        let server = CoordinatorServer::new(
            CoordinatorState::new(),
            50052,
            vec![hang1, hang2],
            vec![],
            Duration::from_millis(20),
            Duration::from_secs(5),
            ReadRetryPolicy::no_backoff(),
        )
        .unwrap();

        let tx_id: u64 = (50052u64 << 32) | 30;
        {
            let mut state = server.state.lock();
            state.start_tx(tx_id);
            state.add_participant(tx_id, hang1.port() as u64);
            state.add_participant(tx_id, hang2.port() as u64);
        }

        // abort_and_notify must complete within ~shard_rpc_timeout despite hung shards.
        let result =
            tokio::time::timeout(Duration::from_millis(200), server.abort_and_notify(tx_id)).await;
        assert!(
            result.is_ok(),
            "abort_and_notify must not hang — must be bounded by shard_rpc_timeout"
        );
    }

    // Trace: sync_clock_from_shards bounded by shard_rpc_timeout.
    //
    // Without the fix (sequential loop with no timeout), a single unresponsive
    // shard blocks coordinator startup indefinitely. With the fix (concurrent
    // join_all wrapped in shard_rpc_timeout), all shards are queried in parallel
    // and the whole batch is bounded by shard_rpc_timeout.
    #[tokio::test]
    async fn sync_clock_from_shards_bounded_by_shard_rpc_timeout() {
        let hang1 = spawn_hang_server().await;
        let hang2 = spawn_hang_server().await;

        let server = CoordinatorServer::new(
            CoordinatorState::new(),
            50052,
            vec![hang1, hang2],
            vec![],
            Duration::from_millis(20), // short timeout
            Duration::from_secs(5),
            ReadRetryPolicy::no_backoff(),
        )
        .unwrap();

        // Must complete within ~shard_rpc_timeout, not hang forever.
        let result =
            tokio::time::timeout(Duration::from_millis(200), server.sync_clock_from_shards()).await;
        assert!(
            result.is_ok(),
            "sync_clock_from_shards must not hang when shards are unresponsive"
        );
        // Clock stays 0 — no shard replied.
        assert_eq!(server.state.lock().clock, 0);
    }
}
