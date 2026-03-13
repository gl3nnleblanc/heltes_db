/// Start a local HeltesDB cluster with N coordinators and M shards.
///
/// Usage: cluster [-n <coordinators>] [-m <shards>]
///                [--prepare-ttl-ms N] [--shard-rpc-timeout-ms N]
///                [--read-loop-timeout-ms N]
///
///   -n  number of coordinator processes to spawn (default: 1)
///   -m  number of shard processes to spawn (default: 3)
///   --prepare-ttl-ms       shard prepared-entry TTL in ms (default: 30000)
///   --shard-rpc-timeout-ms coordinator per-RPC shard timeout in ms (default: 30000)
///   --read-loop-timeout-ms coordinator read inquiry-loop timeout in ms (default: 30000)
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

struct ClusterArgs {
    n_coords: usize,
    m_shards: usize,
    prepare_ttl: Duration,
    shard_rpc_timeout: Duration,
    read_loop_timeout: Duration,
}

fn parse_args() -> Result<ClusterArgs, Box<dyn std::error::Error>> {
    let mut n = 1usize;
    let mut m = 3usize;
    let mut prepare_ttl = Duration::from_secs(30);
    let mut shard_rpc_timeout = Duration::from_secs(30);
    let mut read_loop_timeout = Duration::from_secs(30);

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
            "--prepare-ttl-ms" => {
                let v = args.next().ok_or("--prepare-ttl-ms requires a value")?;
                let ms: u64 = v
                    .parse()
                    .map_err(|_| format!("invalid --prepare-ttl-ms value: {v}"))?;
                prepare_ttl = Duration::from_millis(ms);
            }
            "--shard-rpc-timeout-ms" => {
                let v = args
                    .next()
                    .ok_or("--shard-rpc-timeout-ms requires a value")?;
                let ms: u64 = v
                    .parse()
                    .map_err(|_| format!("invalid --shard-rpc-timeout-ms value: {v}"))?;
                shard_rpc_timeout = Duration::from_millis(ms);
            }
            "--read-loop-timeout-ms" => {
                let v = args
                    .next()
                    .ok_or("--read-loop-timeout-ms requires a value")?;
                let ms: u64 = v
                    .parse()
                    .map_err(|_| format!("invalid --read-loop-timeout-ms value: {v}"))?;
                read_loop_timeout = Duration::from_millis(ms);
            }
            "--help" | "-h" => {
                eprintln!(
                    "Usage: cluster [-n <coordinators>] [-m <shards>]\n\
                     \x20              [--prepare-ttl-ms N] [--shard-rpc-timeout-ms N]\n\
                     \x20              [--read-loop-timeout-ms N]"
                );
                eprintln!("  -n  number of coordinators (default: 1)");
                eprintln!("  -m  number of shards       (default: 3)");
                eprintln!("  --prepare-ttl-ms       prepared-entry TTL in ms (default: 30000)");
                eprintln!("  --shard-rpc-timeout-ms per-RPC shard timeout in ms (default: 30000)");
                eprintln!(
                    "  --read-loop-timeout-ms read inquiry-loop timeout in ms (default: 30000)"
                );
                std::process::exit(0);
            }
            other => return Err(format!("unknown argument: {other}").into()),
        }
    }
    Ok(ClusterArgs {
        n_coords: n,
        m_shards: m,
        prepare_ttl,
        shard_rpc_timeout,
        read_loop_timeout,
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ClusterArgs {
        n_coords,
        m_shards,
        prepare_ttl,
        shard_rpc_timeout,
        read_loop_timeout,
    } = parse_args()?;

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
        let mut state = ShardState::new();
        state.prepare_ttl = prepare_ttl;
        tokio::spawn(
            Server::builder()
                .add_service(ShardServiceServer::new(ShardServer::new(state)))
                .serve_with_incoming(incoming),
        );
        eprintln!("shard[{i}]       {addr}  (prepare_ttl={prepare_ttl:?})");
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
            shard_rpc_timeout,
            read_loop_timeout,
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
        eprintln!(
            "coordinator[{i}]  {addr}  (peers: {peers:?}, \
             shard_rpc_timeout={shard_rpc_timeout:?}, read_loop_timeout={read_loop_timeout:?})"
        );
    }

    eprintln!();
    eprintln!("Cluster ready: {n_coords} coordinator(s), {m_shards} shard(s). Ctrl-C to stop.");

    tokio::signal::ctrl_c().await?;
    eprintln!("Shutting down.");
    Ok(())
}
