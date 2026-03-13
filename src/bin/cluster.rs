/// Start a local HeltesDB cluster with N coordinators and M shards.
///
/// Usage: cluster [-n <coordinators>] [-m <shards>]
///
///   -n  number of coordinator processes to spawn (default: 1)
///   -m  number of shard processes to spawn (default: 3)
///
/// All servers bind to random localhost ports. Addresses are printed to stderr.
/// Each coordinator is configured with the full shard list and all peer
/// coordinator addresses (for cross-coordinator Inquire forwarding).
/// The coordinator clock is synced from the shards before each coordinator
/// begins serving requests.
///
/// Press Ctrl-C to stop the cluster.
use std::net::SocketAddr;
use std::time::Duration;

use futures::stream;
use tonic::transport::Server;

use heltes_db::coordinator::{
    server::{CoordinatorServer, CoordinatorServiceServer},
    CoordinatorState,
};
use heltes_db::shard::{
    server::{ShardServer, ShardServiceServer},
    ShardState,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (n_coords, m_shards) = parse_args()?;

    // ── Spawn M shards ────────────────────────────────────────────────────────

    let mut shard_addrs: Vec<SocketAddr> = Vec::new();

    for i in 0..m_shards {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        shard_addrs.push(addr);

        let incoming = stream::unfold(listener, |l| async move {
            let r = l.accept().await.map(|(s, _)| s);
            Some((r, l))
        });
        tokio::spawn(
            Server::builder()
                .add_service(ShardServiceServer::new(ShardServer::new(ShardState::new())))
                .serve_with_incoming(incoming),
        );
        eprintln!("shard[{i}]       {addr}");
    }

    // Give shards a moment to start accepting connections before coordinators
    // attempt clock-sync RPCs against them.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // ── Bind coordinator ports up-front so each knows its peers' addresses ───

    let mut coord_listeners = Vec::new();
    let mut coord_addrs: Vec<SocketAddr> = Vec::new();

    for _ in 0..n_coords {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        coord_addrs.push(addr);
        coord_listeners.push(listener);
    }

    // ── Spawn N coordinators ──────────────────────────────────────────────────

    for (i, listener) in coord_listeners.into_iter().enumerate() {
        let addr = coord_addrs[i];
        let port = addr.port();

        let peers: Vec<SocketAddr> = coord_addrs
            .iter()
            .enumerate()
            .filter(|&(j, _)| j != i)
            .map(|(_, &a)| a)
            .collect();

        let server = CoordinatorServer::new(
            CoordinatorState::new(),
            port,
            shard_addrs.clone(),
            peers.clone(),
            Duration::from_secs(30),
            Duration::from_secs(30),
        )?;

        // Sync clock before serving to avoid CommittedConflict on a live cluster.
        server.sync_clock_from_shards().await;

        let incoming = stream::unfold(listener, |l| async move {
            let r = l.accept().await.map(|(s, _)| s);
            Some((r, l))
        });
        tokio::spawn(
            Server::builder()
                .add_service(CoordinatorServiceServer::new(server))
                .serve_with_incoming(incoming),
        );
        eprintln!("coordinator[{i}]  {addr}  (peers: {peers:?})");
    }

    eprintln!();
    eprintln!("Cluster ready: {n_coords} coordinator(s), {m_shards} shard(s). Ctrl-C to stop.");

    tokio::signal::ctrl_c().await?;
    eprintln!("Shutting down.");
    Ok(())
}

fn parse_args() -> Result<(usize, usize), Box<dyn std::error::Error>> {
    let mut n = 1usize;
    let mut m = 3usize;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-n" => {
                let v = args.next().ok_or("-n requires a value")?;
                n = v.parse().map_err(|_| format!("invalid -n value: {v}"))?;
            }
            "-m" => {
                let v = args.next().ok_or("-m requires a value")?;
                m = v.parse().map_err(|_| format!("invalid -m value: {v}"))?;
            }
            "--help" | "-h" => {
                eprintln!("Usage: cluster [-n <coordinators>] [-m <shards>]");
                eprintln!("  -n  number of coordinators (default: 1)");
                eprintln!("  -m  number of shards       (default: 3)");
                std::process::exit(0);
            }
            other => return Err(format!("unknown argument: {other}").into()),
        }
    }
    Ok((n, m))
}
