use std::net::SocketAddr;

use tonic::transport::Server;

use heltes_db::coordinator::{
    server::{CoordinatorServer, CoordinatorServiceServer},
    CoordinatorState,
};

/// Usage: coordinator [BIND_ADDR] [SHARD_ADDR...] [-- PEER_COORD_ADDR...]
///
///   coordinator [::1]:50052 [::1]:50051 -- [::1]:50053
///
/// BIND_ADDR defaults to [::1]:50052.
/// SHARD_ADDRs are the shards this coordinator routes to (before --).
/// PEER_COORD_ADDRs (after --) are other coordinators this one may forward
/// cross-coordinator Inquire RPCs to. Required for multi-coordinator deployments.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);

    let bind: SocketAddr = args
        .next()
        .unwrap_or_else(|| "[::1]:50052".to_string())
        .parse()?;

    // Split remaining args at "--": before → shard addrs, after → peer coordinator addrs.
    let rest: Vec<String> = args.collect();
    let sep = rest.iter().position(|a| a == "--");
    let (shard_part, peer_part) = match sep {
        Some(i) => (&rest[..i], &rest[i + 1..]),
        None => (rest.as_slice(), [].as_slice()),
    };

    let shard_addrs: Vec<SocketAddr> = shard_part
        .iter()
        .map(|s| s.parse::<SocketAddr>())
        .collect::<Result<_, _>>()?;

    let peer_coordinator_addrs: Vec<SocketAddr> = peer_part
        .iter()
        .map(|s| s.parse::<SocketAddr>())
        .collect::<Result<_, _>>()?;

    let my_port = bind.port();

    eprintln!(
        "coordinator listening on {bind} (port={my_port}, shards={shard_addrs:?}, \
         peers={peer_coordinator_addrs:?})"
    );

    if shard_addrs.is_empty() {
        eprintln!("warning: no shard addresses provided — all RPCs will return Unavailable");
    }

    let server = CoordinatorServer::new(
        CoordinatorState::new(),
        my_port,
        shard_addrs,
        peer_coordinator_addrs,
    )?;

    Server::builder()
        .add_service(CoordinatorServiceServer::new(server))
        .serve(bind)
        .await?;

    Ok(())
}
