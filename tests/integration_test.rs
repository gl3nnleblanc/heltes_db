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
    CoordinatorState,
};
use heltes_db::proto::{
    coordinator_service_client::CoordinatorServiceClient, shard_service_client::ShardServiceClient,
    Abort, GetClockRequest, TxBeginRequest, TxCommitRequest, TxReadRequest, TxUpdateRequest,
};
use heltes_db::shard::{
    server::{ShardServer, ShardServiceServer},
    ShardState,
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
