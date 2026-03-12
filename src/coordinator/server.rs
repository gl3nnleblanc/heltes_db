use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Mutex;
use std::time::Duration;

use futures::future::join_all;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Response, Status};

use crate::coordinator::{
    coord_port_from_tx_id, routing::ConsistentHashRouter, BeginCommitResult, BeginFastCommitResult,
    CollectPrepareResult, CoordinatorState, FinalizeFastCommitResult, SendCommitResult, TxIdGen,
};
use crate::proto::coordinator_service_client::CoordinatorServiceClient;
use crate::proto::coordinator_service_server::CoordinatorService;
use crate::proto::shard_service_client::ShardServiceClient;
use crate::proto::{
    Abort, AbortRequest as ShardAbortRequest, Active, CommitRequest as ShardCommitRequest,
    FastCommitRequest as ShardFastCommitRequest, InquireReply, InquireRequest, InquiryResult,
    NeedsInquirySet, PrepareRequest, ReadRequest, TxAbortReply, TxAbortRequest, TxBeginReply,
    TxBeginRequest, TxCommitReply, TxCommitRequest, TxReadReply, TxReadRequest, TxUpdateReply,
    TxUpdateRequest, UpdateRequest,
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
    /// port → gRPC URI for peer coordinators (for cross-coordinator Inquire forwarding).
    /// Built from `peer_coordinator_addrs` supplied at construction time.
    coordinator_uris: HashMap<u16, String>,
    /// Maximum time to wait for any individual shard RPC (or the full prepare phase).
    /// A timeout is treated as an ABORT from the shard: the coordinator aborts the
    /// transaction and returns Abort to the client. Matches CoordAbortOnReply in TLA+.
    shard_rpc_timeout: Duration,
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
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let router = ConsistentHashRouter::new(shard_addrs.iter().copied());
        let mut shard_clients = HashMap::new();
        for addr in shard_addrs {
            let channel = Endpoint::from_shared(format!("http://{addr}"))?.connect_lazy();
            shard_clients.insert(addr.port() as u64, ShardServiceClient::new(channel));
        }
        let coordinator_uris = peer_coordinator_addrs
            .iter()
            .map(|addr| (addr.port(), format!("http://{addr}")))
            .collect();
        Ok(CoordinatorServer {
            state: Mutex::new(state),
            tx_id_gen: Mutex::new(TxIdGen::new(my_port)),
            router,
            shard_clients,
            my_port,
            coordinator_uris,
            shard_rpc_timeout,
        })
    }

    /// Return the gRPC URI for a peer coordinator by its port, or `None` if
    /// no address was configured for that port.
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
            return Ok(self
                .state
                .lock()
                .unwrap()
                .handle_inquire(tx_id, reader_start_ts));
        }
        let uri = self
            .coordinator_uri_for_port(port)
            .ok_or_else(|| {
                Status::unavailable(format!("no address configured for coordinator port {port}"))
            })?
            .to_owned();
        let mut client = CoordinatorServiceClient::connect(uri)
            .await
            .map_err(|e| Status::unavailable(format!("coordinator :{port} unreachable: {e}")))?;
        let reply = client
            .inquire(InquireRequest {
                tx_id,
                prep_ts: reader_start_ts,
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

    /// Mark `tx_id` aborted in state and best-effort ABORT to every participant shard.
    /// The abort RPCs are bounded by `shard_rpc_timeout`; hung shards are abandoned.
    /// Matches CoordAbortOnReply in the TLA+ spec.
    async fn abort_and_notify(&self, tx_id: u64) {
        let participants = self.state.lock().unwrap().abort_tx(tx_id);
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
            .unwrap()
            .next()
            .expect("TxIdGen never exhausts");
        let start_ts = self.state.lock().unwrap().start_tx(tx_id);
        Ok(Response::new(TxBeginReply { tx_id, start_ts }))
    }

    /// CoordRead: route to the owning shard, resolve any NeedsInquiry loops.
    async fn read(&self, request: Request<TxReadRequest>) -> Result<Response<TxReadReply>, Status> {
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

        self.state
            .lock()
            .unwrap()
            .add_participant(tx_id, shard_port);

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

        let fast_shard = self.state.lock().unwrap().begin_fast_commit(tx_id);
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
                        match self
                            .state
                            .lock()
                            .unwrap()
                            .finalize_fast_commit(tx_id, commit_ts)
                        {
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
            let mut state = self.state.lock().unwrap();
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
            match self
                .state
                .lock()
                .unwrap()
                .collect_prepare_reply(tx_id, port, ts)
            {
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
            join_all(futs).await;
            return Ok(Response::new(TxCommitReply {
                result: Some(TR::Abort(Abort {})),
            }));
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

        Ok(Response::new(TxCommitReply {
            result: Some(TR::CommitTs(commit_ts)),
        }))
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

    fn test_server(my_port: u16, peers: Vec<SocketAddr>) -> CoordinatorServer {
        CoordinatorServer::new(
            CoordinatorState::new(),
            my_port,
            vec![],
            peers,
            Duration::from_secs(5),
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
    #[test]
    fn coordinator_uri_for_known_ipv4_peer() {
        let server = test_server(50052, vec!["192.0.2.1:50053".parse().unwrap()]);
        assert_eq!(
            server.coordinator_uri_for_port(50053),
            Some("http://192.0.2.1:50053"),
        );
    }

    // Trace: remote IPv6 peer → URI wraps host in brackets (std::net formatting).
    #[test]
    fn coordinator_uri_for_known_ipv6_peer() {
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
    #[test]
    fn coordinator_uri_for_multiple_peers() {
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
        let server =
            CoordinatorServer::new(CoordinatorState::new(), 50052, vec![], vec![], timeout)
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
        )
        .unwrap();

        // Register a transaction with 2 participants directly, bypassing begin/update RPCs.
        let tx_id: u64 = (50052u64 << 32) | 1;
        {
            let mut state = server.state.lock().unwrap();
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
        )
        .unwrap();

        let tx_id: u64 = (50052u64 << 32) | 2;
        {
            let mut state = server.state.lock().unwrap();
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
}
