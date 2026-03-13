/// End-to-end integration tests: spawn real coordinator + shard gRPC servers
/// and drive full transaction cycles over the wire.
///
/// These tests exercise the complete request path (client → coordinator → shard)
/// and catch protocol or routing bugs that unit tests miss.
use std::net::SocketAddr;
use std::time::Duration;

use tonic::transport::{Channel, Endpoint, Server};

use heltes_db::coordinator::{
    routing::ConsistentHashRouter,
    server::{CoordinatorServer, CoordinatorServiceServer},
    CoordinatorState, ReadRetryPolicy,
};
use heltes_db::proto::{
    coordinator_service_client::CoordinatorServiceClient, shard_service_client::ShardServiceClient,
    Abort, GetClockRequest, TxBeginRequest, TxCommitRequest, TxReadRequest, TxUpdateRequest,
};
use heltes_db::shard::{
    server::{ShardServer, ShardServiceServer},
    Key, PrepareResult, ShardState, UpdateResult, Value,
};

// ── Harness ────────────────────────────────────────────────────────────────────

/// Bind to a random port, spin up a `ShardServer`, return its address.
async fn spawn_shard() -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let incoming = futures::stream::unfold(listener, |l| async move {
        let r = l.accept().await.map(|(s, _)| s);
        Some((r, l))
    });
    tokio::spawn(
        Server::builder()
            .add_service(ShardServiceServer::new(ShardServer::new(ShardState::new())))
            .serve_with_incoming(incoming),
    );
    addr
}

/// Like `spawn_shard` but starts with a pre-populated `ShardState`.
/// Useful for injecting prepared transactions without going through the gRPC path.
async fn spawn_shard_with_state(state: ShardState) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let incoming = futures::stream::unfold(listener, |l| async move {
        let r = l.accept().await.map(|(s, _)| s);
        Some((r, l))
    });
    tokio::spawn(
        Server::builder()
            .add_service(ShardServiceServer::new(ShardServer::new(state)))
            .serve_with_incoming(incoming),
    );
    addr
}

/// Bind a TCP listener and return `(listener, port)` without starting anything.
/// Pass the listener to `spawn_coordinator_on` after setting up any pre-populated
/// shard state that needs to encode the coordinator's port into a tx_id.
async fn bind_coordinator_port() -> (tokio::net::TcpListener, u16) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    (listener, port)
}

/// Start a coordinator on a pre-bound listener (obtained from `bind_coordinator_port`).
/// Syncs clock from shards before serving and returns a connected client.
async fn spawn_coordinator_on(
    listener: tokio::net::TcpListener,
    shard_addrs: Vec<SocketAddr>,
) -> CoordinatorServiceClient<Channel> {
    let addr = listener.local_addr().unwrap();
    let port = addr.port();
    let incoming = futures::stream::unfold(listener, |l| async move {
        let r = l.accept().await.map(|(s, _)| s);
        Some((r, l))
    });
    let server = CoordinatorServer::new(
        CoordinatorState::new(),
        port,
        shard_addrs,
        vec![],
        Duration::from_secs(5),
        Duration::from_secs(5),
        ReadRetryPolicy::no_backoff(),
    )
    .unwrap();
    server.sync_clock_from_shards().await;
    tokio::spawn(
        Server::builder()
            .add_service(CoordinatorServiceServer::new(server))
            .serve_with_incoming(incoming),
    );
    tokio::time::sleep(Duration::from_millis(20)).await;
    CoordinatorServiceClient::connect(format!("http://{addr}"))
        .await
        .unwrap()
}

/// Bind to a random port, spin up a `CoordinatorServer` pointing at `shard_addrs`,
/// wait briefly for it to start, and return a connected client.
///
/// The coordinator's clock is synced from the shards before it begins serving,
/// so this function is safe to call even after other coordinators have already
/// committed transactions on the same shards.
async fn spawn_coordinator(shard_addrs: Vec<SocketAddr>) -> CoordinatorServiceClient<Channel> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let port = addr.port();
    let incoming = futures::stream::unfold(listener, |l| async move {
        let r = l.accept().await.map(|(s, _)| s);
        Some((r, l))
    });
    let server = CoordinatorServer::new(
        CoordinatorState::new(),
        port,
        shard_addrs,
        vec![],
        Duration::from_secs(5),
        Duration::from_secs(5),
        ReadRetryPolicy::no_backoff(),
    )
    .unwrap();
    server.sync_clock_from_shards().await;
    tokio::spawn(
        Server::builder()
            .add_service(CoordinatorServiceServer::new(server))
            .serve_with_incoming(incoming),
    );
    // Give the server a moment to accept connections.
    tokio::time::sleep(Duration::from_millis(20)).await;
    CoordinatorServiceClient::connect(format!("http://{addr}"))
        .await
        .unwrap()
}

// ── Tests ──────────────────────────────────────────────────────────────────────

/// Single-shard fast-commit path: write a key, commit, read it back in a new tx.
#[tokio::test]
async fn single_shard_write_read() {
    let shard = spawn_shard().await;
    let mut client = spawn_coordinator(vec![shard]).await;

    // Tx 1: write key=1 → value=42 and commit.
    let begin = client.begin(TxBeginRequest {}).await.unwrap().into_inner();
    let tx_id = begin.tx_id;

    let update = client
        .update(TxUpdateRequest {
            tx_id,
            key: 1,
            value: 42,
        })
        .await
        .unwrap()
        .into_inner();
    use heltes_db::proto::tx_update_reply::Result as UR;
    assert_eq!(update.result, Some(UR::Ok(true)));

    let commit = client
        .commit(TxCommitRequest { tx_id })
        .await
        .unwrap()
        .into_inner();
    use heltes_db::proto::tx_commit_reply::Result as CR;
    assert!(
        matches!(commit.result, Some(CR::CommitTs(_))),
        "expected CommitTs, got {:?}",
        commit.result
    );

    // Tx 2: read key=1 — must see value 42.
    let begin2 = client.begin(TxBeginRequest {}).await.unwrap().into_inner();
    let read = client
        .read(TxReadRequest {
            tx_id: begin2.tx_id,
            key: 1,
        })
        .await
        .unwrap()
        .into_inner();
    use heltes_db::proto::tx_read_reply::Result as RR;
    assert_eq!(
        read.result,
        Some(RR::Value(42)),
        "expected value 42, got {:?}",
        read.result
    );
}

/// Two-shard 2PC path: write keys on two different shards, commit, read both back.
#[tokio::test]
async fn two_shard_write_read_2pc() {
    let shard1 = spawn_shard().await;
    let shard2 = spawn_shard().await;

    // Find two keys that route to different shards using the same router the
    // coordinator will build internally.
    let router = ConsistentHashRouter::new([shard1, shard2]);
    let (key_a, key_b) = (0u64..10_000)
        .flat_map(|a| (a + 1..10_000).map(move |b| (a, b)))
        .find(|&(a, b)| router.shard_for_key(a) != router.shard_for_key(b))
        .expect("should find two keys on different shards");

    let mut client = spawn_coordinator(vec![shard1, shard2]).await;

    // Tx 1: write key_a → 10, key_b → 20 (touches 2 shards → full 2PC).
    let begin = client.begin(TxBeginRequest {}).await.unwrap().into_inner();
    let tx_id = begin.tx_id;

    for (key, value) in [(key_a, 10u64), (key_b, 20u64)] {
        let r = client
            .update(TxUpdateRequest { tx_id, key, value })
            .await
            .unwrap()
            .into_inner();
        use heltes_db::proto::tx_update_reply::Result as UR;
        assert_eq!(
            r.result,
            Some(UR::Ok(true)),
            "update of key {key} failed: {:?}",
            r.result
        );
    }

    let commit = client
        .commit(TxCommitRequest { tx_id })
        .await
        .unwrap()
        .into_inner();
    use heltes_db::proto::tx_commit_reply::Result as CR;
    assert!(
        matches!(commit.result, Some(CR::CommitTs(_))),
        "2PC commit failed: {:?}",
        commit.result
    );

    // Tx 2: read key_a and key_b in a new snapshot — must see 10 and 20.
    let begin2 = client.begin(TxBeginRequest {}).await.unwrap().into_inner();
    let tx2 = begin2.tx_id;

    for (key, expected) in [(key_a, 10u64), (key_b, 20u64)] {
        let r = client
            .read(TxReadRequest { tx_id: tx2, key })
            .await
            .unwrap()
            .into_inner();
        use heltes_db::proto::tx_read_reply::Result as RR;
        assert_eq!(
            r.result,
            Some(RR::Value(expected)),
            "read key {key}: expected {expected}, got {:?}",
            r.result
        );
    }
}

/// Write-write conflict: two concurrent transactions write the same key; the
/// second one must be aborted by the shard's write-buffer conflict check.
#[tokio::test]
async fn write_conflict_causes_abort() {
    let shard = spawn_shard().await;
    let mut client = spawn_coordinator(vec![shard]).await;

    // Tx 1: write key=5 → 100 (but do not commit yet).
    let begin1 = client.begin(TxBeginRequest {}).await.unwrap().into_inner();
    let tx1 = begin1.tx_id;

    let r1 = client
        .update(TxUpdateRequest {
            tx_id: tx1,
            key: 5,
            value: 100,
        })
        .await
        .unwrap()
        .into_inner();
    use heltes_db::proto::tx_update_reply::Result as UR;
    assert_eq!(r1.result, Some(UR::Ok(true)));

    // Tx 2: also write key=5 → must conflict with Tx 1's write lock.
    let begin2 = client.begin(TxBeginRequest {}).await.unwrap().into_inner();
    let tx2 = begin2.tx_id;

    let r2 = client
        .update(TxUpdateRequest {
            tx_id: tx2,
            key: 5,
            value: 200,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(r2.result, Some(UR::Abort(Abort {})));

    // Tx 1 must still commit successfully.
    let commit = client
        .commit(TxCommitRequest { tx_id: tx1 })
        .await
        .unwrap()
        .into_inner();
    use heltes_db::proto::tx_commit_reply::Result as CR;
    assert!(
        matches!(commit.result, Some(CR::CommitTs(_))),
        "tx1 commit after conflict: {:?}",
        commit.result
    );
}

/// Read-your-own-writes: a transaction should see its own buffered writes before
/// committing.
#[tokio::test]
async fn read_own_writes_before_commit() {
    let shard = spawn_shard().await;
    let mut client = spawn_coordinator(vec![shard]).await;

    let begin = client.begin(TxBeginRequest {}).await.unwrap().into_inner();
    let tx_id = begin.tx_id;

    // Write key=7 → 99 and then read key=7 in the same transaction.
    let u = client
        .update(TxUpdateRequest {
            tx_id,
            key: 7,
            value: 99,
        })
        .await
        .unwrap()
        .into_inner();
    use heltes_db::proto::tx_update_reply::Result as UR;
    assert_eq!(u.result, Some(UR::Ok(true)));

    let r = client
        .read(TxReadRequest { tx_id, key: 7 })
        .await
        .unwrap()
        .into_inner();
    use heltes_db::proto::tx_read_reply::Result as RR;
    assert_eq!(
        r.result,
        Some(RR::Value(99)),
        "read-your-write: expected 99, got {:?}",
        r.result
    );
}

/// Snapshot isolation: a transaction started before a commit does not see the
/// committed value; one started after does.
#[tokio::test]
async fn snapshot_isolation_stale_reader() {
    let shard = spawn_shard().await;
    let mut client = spawn_coordinator(vec![shard]).await;

    // Tx A: begin before any writes.
    let a_begin = client.begin(TxBeginRequest {}).await.unwrap().into_inner();
    let tx_a = a_begin.tx_id;

    // Tx W: write key=3 → 777 and commit.
    let w_begin = client.begin(TxBeginRequest {}).await.unwrap().into_inner();
    let tx_w = w_begin.tx_id;
    client
        .update(TxUpdateRequest {
            tx_id: tx_w,
            key: 3,
            value: 777,
        })
        .await
        .unwrap();
    let commit = client
        .commit(TxCommitRequest { tx_id: tx_w })
        .await
        .unwrap()
        .into_inner();
    use heltes_db::proto::tx_commit_reply::Result as CR;
    assert!(matches!(commit.result, Some(CR::CommitTs(_))));

    // Tx A (started before the write) must not see 777.
    let r_a = client
        .read(TxReadRequest {
            tx_id: tx_a,
            key: 3,
        })
        .await
        .unwrap()
        .into_inner();
    use heltes_db::proto::tx_read_reply::Result as RR;
    assert_eq!(
        r_a.result,
        Some(RR::NotFound(true)),
        "stale reader should not see committed write, got {:?}",
        r_a.result
    );

    // Tx B: begin after the commit — must see 777.
    let b_begin = client.begin(TxBeginRequest {}).await.unwrap().into_inner();
    let r_b = client
        .read(TxReadRequest {
            tx_id: b_begin.tx_id,
            key: 3,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        r_b.result,
        Some(RR::Value(777)),
        "fresh reader should see committed write, got {:?}",
        r_b.result
    );
}

/// Multi-coordinator clock divergence regression: a second coordinator joining a
/// cluster where shards already have committed versions must not abort every write
/// due to CommittedConflict from a stale start_ts.
///
/// Without clock sync: coordinator 2 starts with c_clock=0, assigns start_ts=1,
/// shards have committed versions at ts≫1 → 100% abort rate.
/// With clock sync (GetClock RPC): coordinator 2 advances c_clock to max(s_clock)
/// before serving requests, so its start_ts values are strictly greater than all
/// committed timestamps.
#[tokio::test]
async fn second_coordinator_clock_sync_prevents_abort() {
    let shard = spawn_shard().await;

    // Coordinator 1: commit 50 transactions to drive the shard clock well above 0.
    let mut client1 = spawn_coordinator(vec![shard]).await;
    for key in 0u64..50 {
        let begin = client1.begin(TxBeginRequest {}).await.unwrap().into_inner();
        let tx_id = begin.tx_id;
        client1
            .update(TxUpdateRequest {
                tx_id,
                key,
                value: key * 10,
            })
            .await
            .unwrap();
        let commit = client1
            .commit(TxCommitRequest { tx_id })
            .await
            .unwrap()
            .into_inner();
        use heltes_db::proto::tx_commit_reply::Result as CR;
        assert!(
            matches!(commit.result, Some(CR::CommitTs(_))),
            "coordinator 1 commit failed at key {key}: {:?}",
            commit.result
        );
    }

    // Verify the shard clock is now meaningfully above 0.
    let shard_clock = {
        let channel = Endpoint::from_shared(format!("http://{shard}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let mut sc = ShardServiceClient::new(channel);
        sc.get_clock(GetClockRequest {})
            .await
            .unwrap()
            .into_inner()
            .clock
    };
    assert!(
        shard_clock > 10,
        "shard clock should be well above 0 after 50 commits, got {shard_clock}"
    );

    // Coordinator 2: same shards, fresh state.  spawn_coordinator calls
    // sync_clock_from_shards() before serving — this is the fix under test.
    let mut client2 = spawn_coordinator(vec![shard]).await;

    // Coordinator 2 must be able to commit a transaction without CommittedConflict.
    let begin = client2.begin(TxBeginRequest {}).await.unwrap().into_inner();
    let tx_id = begin.tx_id;

    let update = client2
        .update(TxUpdateRequest {
            tx_id,
            key: 9999,
            value: 42,
        })
        .await
        .unwrap()
        .into_inner();
    use heltes_db::proto::tx_update_reply::Result as UR;
    assert_eq!(
        update.result,
        Some(UR::Ok(true)),
        "coordinator 2 update must not abort due to clock divergence"
    );

    let commit = client2
        .commit(TxCommitRequest { tx_id })
        .await
        .unwrap()
        .into_inner();
    use heltes_db::proto::tx_commit_reply::Result as CR;
    assert!(
        matches!(commit.result, Some(CR::CommitTs(_))),
        "coordinator 2 commit must succeed after clock sync, got {:?}",
        commit.result
    );

    // Coordinator 2's committed value must be readable by a subsequent transaction.
    let read_begin = client2.begin(TxBeginRequest {}).await.unwrap().into_inner();
    let read = client2
        .read(TxReadRequest {
            tx_id: read_begin.tx_id,
            key: 9999,
        })
        .await
        .unwrap()
        .into_inner();
    use heltes_db::proto::tx_read_reply::Result as RR;
    assert_eq!(
        read.result,
        Some(RR::Value(42)),
        "coordinator 2 committed value must be readable"
    );
}

// ── NeedsInquiry integration tests ─────────────────────────────────────────────
//
// These tests exercise the full NeedsInquiry → coordinator inquiry → resolved-status
// → retry → correct version path — the most complex branch of the SI read protocol.
//
// Strategy: bind the coordinator's port first, then pre-populate the shard with a
// synthetic "prepared writer" whose tx_id encodes that coordinator's port.  When the
// coordinator receives a NeedsInquiry for that tx_id it resolves the inquiry in-process
// (no second coordinator needed): the tx_id is unknown in the coordinator's own
// transaction table, so `handle_inquire` returns `Active`.  The shard then proceeds
// with the read using the Active inquiry result.
//
// Spec traces exercised (ShardSendInquire → CoordHandleInquire → ShardHandleRead):
//   T1: prepared writer T2, no prior committed version → NeedsInquiry → Active → NotFound
//   T2: T_prev commits K=55, then T2 prepared to overwrite K →
//         NeedsInquiry → T2 Active → Value(55) (last committed snapshot before T2)

/// Helper: build a ShardState that already has a prepared writer for `key`.
/// `coord_port` is encoded into the tx_id so the coordinator treats it as "unknown → Active".
/// Returns `(state, t2_id)`.
fn shard_with_prepared_writer(coord_port: u16, key: Key, value: Value) -> (ShardState, u64) {
    // Encode coord_port in the high 32 bits so resolve_inquiry routes to that coordinator.
    let t2_id: u64 = (coord_port as u64) << 32 | 999;
    let mut state = ShardState::new();
    let ur = state.handle_update(t2_id, 1, key, value);
    assert_eq!(ur, UpdateResult::Ok, "helper: T2 update must succeed");
    let pr = state.handle_prepare(t2_id);
    assert!(
        matches!(pr, PrepareResult::Timestamp(_)),
        "helper: T2 prepare must succeed"
    );
    (state, t2_id)
}

/// Trace T1 — NeedsInquiry path with no prior committed version.
///
/// T2 is a prepared writer for key K; K has never been committed.
/// T3 reads K after T2 is prepared.
///
/// Expected protocol:
///   shard → NeedsInquiry([T2])
///   coordinator → handle_inquire(T2) → Active (T2 unknown in coordinator state)
///   shard (retry with Active result) → LatestVersionBefore(K, T3.start_ts) = nothing
///   → NotFound
#[tokio::test]
async fn needs_inquiry_active_writer_no_prior_version_returns_not_found() {
    const KEY: Key = 42;

    // Step 1: reserve the coordinator's port without starting the server yet.
    let (listener, coord_port) = bind_coordinator_port().await;

    // Step 2: build shard state with T2 prepared.  T2's tx_id encodes coord_port so
    // the coordinator resolves it in-process as Active (unknown tx → Active).
    let (state, _t2_id) = shard_with_prepared_writer(coord_port, KEY, Value(99));

    // Step 3: spawn shard with the pre-populated state, then start the coordinator
    // on the pre-bound listener (same port that T2's tx_id encodes).
    let shard_addr = spawn_shard_with_state(state).await;
    let mut client = spawn_coordinator_on(listener, vec![shard_addr]).await;

    // T3: begin and read K.
    // The read loop will:
    //   1. Send READ_KEY → shard returns NeedsInquiry([T2])
    //   2. Coordinator resolves T2 as Active (in-process lookup, unknown tx → Active)
    //   3. Retry READ_KEY with inquiry_results={T2: Active} → shard returns NotFound
    let begin = client.begin(TxBeginRequest {}).await.unwrap().into_inner();
    let read = client
        .read(TxReadRequest {
            tx_id: begin.tx_id,
            key: KEY,
        })
        .await
        .unwrap()
        .into_inner();

    use heltes_db::proto::tx_read_reply::Result as RR;
    assert_eq!(
        read.result,
        Some(RR::NotFound(true)),
        "T3 must see pre-T2 snapshot (NotFound) when T2 is prepared but not committed; got {:?}",
        read.result,
    );
}

/// Trace T2 — NeedsInquiry path with a prior committed version visible to T3.
///
/// T_prev commits K=55.  T2 is then prepared to overwrite K (not yet committed).
/// T3 reads K after T2 is prepared; T3.start_ts > T2.prep_ts > T_prev.commit_ts.
///
/// Expected protocol:
///   shard → NeedsInquiry([T2])
///   coordinator → handle_inquire(T2) → Active
///   shard (retry with Active) → LatestVersionBefore(K, T3.start_ts) = (55, T_prev.commit_ts)
///   → Value(55)
#[tokio::test]
async fn needs_inquiry_active_writer_returns_last_committed_value() {
    const KEY: Key = 43;
    const PREV_VALUE: u64 = 55;
    const T2_VALUE: u64 = 999;

    // Step 1: commit T_prev so K has a committed version at a low timestamp.
    let shard_addr_tmp = spawn_shard().await;
    let mut bootstrap = spawn_coordinator(vec![shard_addr_tmp]).await;

    let b_prev = bootstrap
        .begin(TxBeginRequest {})
        .await
        .unwrap()
        .into_inner();
    let tx_prev = b_prev.tx_id;
    let ur = bootstrap
        .update(TxUpdateRequest {
            tx_id: tx_prev,
            key: KEY,
            value: PREV_VALUE,
        })
        .await
        .unwrap()
        .into_inner();
    use heltes_db::proto::tx_update_reply::Result as UR;
    assert_eq!(ur.result, Some(UR::Ok(true)));
    let commit = bootstrap
        .commit(TxCommitRequest { tx_id: tx_prev })
        .await
        .unwrap()
        .into_inner();
    use heltes_db::proto::tx_commit_reply::Result as CR;
    let prev_commit_ts = match commit.result {
        Some(CR::CommitTs(ts)) => ts,
        other => panic!("T_prev commit failed: {other:?}"),
    };

    // Step 2: read back the committed MVCC history directly from the shard so we
    // can reconstruct a ShardState with that version already installed plus T2 prepared.
    // Simpler approach: build the state by replaying the same writes on a fresh ShardState.
    let mut state = ShardState::new();
    // Advance state to match what the shard looks like after T_prev committed.
    // We replay the same write at the same commit timestamp by using handle_update +
    // handle_fast_commit (which atomically assigns commit_ts = clock+1).
    // T_prev's tx_id doesn't matter here — we just need K's version at prev_commit_ts.
    let dummy_prev: u64 = (55555u64 << 32) | 1;
    let ur2 = state.handle_update(dummy_prev, 1, KEY, Value(PREV_VALUE));
    assert_eq!(ur2, UpdateResult::Ok);
    // Fast-commit assigns commit_ts = clock+1 = 2 (clock was 1 after handle_update).
    use heltes_db::shard::FastCommitResult;
    let fc = state.handle_fast_commit(dummy_prev);
    let installed_ts = match fc {
        FastCommitResult::Ok(ts) => ts,
        other => panic!("fast-commit of dummy_prev failed: {other:?}"),
    };
    // installed_ts is independent of prev_commit_ts; what matters is that K has a
    // committed version and T2's start_ts is strictly greater than installed_ts.
    let _ = prev_commit_ts; // used only to document the scenario

    // Step 3: reserve the coordinator port before building the injected state,
    // so T2's tx_id encodes the correct port.
    let (coord_listener, coord_port) = bind_coordinator_port().await;

    let t2_start_ts = installed_ts + 1;
    let t2_id: u64 = (coord_port as u64) << 32 | 998;
    let ur3 = state.handle_update(t2_id, t2_start_ts, KEY, Value(T2_VALUE));
    assert_eq!(ur3, UpdateResult::Ok, "T2 update must not conflict");
    let pr = state.handle_prepare(t2_id);
    assert!(
        matches!(pr, PrepareResult::Timestamp(_)),
        "T2 prepare must succeed"
    );

    // Step 4: spawn shard with the injected state, then start the coordinator
    // on the pre-bound listener (same port that T2's tx_id encodes).
    let shard_addr = spawn_shard_with_state(state).await;
    let mut client = spawn_coordinator_on(coord_listener, vec![shard_addr]).await;

    // Step 5: T3 reads K.  Shard returns NeedsInquiry([T2]).
    // Coordinator resolves T2 as Active.  Shard returns Value(PREV_VALUE).
    let begin = client.begin(TxBeginRequest {}).await.unwrap().into_inner();
    let read = client
        .read(TxReadRequest {
            tx_id: begin.tx_id,
            key: KEY,
        })
        .await
        .unwrap()
        .into_inner();

    use heltes_db::proto::tx_read_reply::Result as RR;
    assert_eq!(
        read.result,
        Some(RR::Value(PREV_VALUE)),
        "T3 must see last committed value ({PREV_VALUE}) when T2 is prepared but active; got {:?}",
        read.result,
    );
}
