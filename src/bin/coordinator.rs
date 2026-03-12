use std::net::SocketAddr;

use tonic::transport::Server;

use heltes_db::coordinator::{
    server::{CoordinatorServer, CoordinatorServiceServer},
    CoordinatorState,
};

/// Usage: coordinator [BIND_ADDR] [SHARD_ADDR...]
///
///   coordinator [::1]:50052 [::1]:50051 [::1]:50053
///
/// BIND_ADDR defaults to [::1]:50052.
/// At least one SHARD_ADDR is required to serve real traffic.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);

    let bind: SocketAddr = args
        .next()
        .unwrap_or_else(|| "[::1]:50052".to_string())
        .parse()?;

    let shard_addrs: Vec<SocketAddr> = args
        .map(|s| s.parse::<SocketAddr>())
        .collect::<Result<_, _>>()?;

    let my_port = bind.port();

    eprintln!(
        "coordinator listening on {bind} (port={my_port}, shards={shard_addrs:?})"
    );

    if shard_addrs.is_empty() {
        eprintln!("warning: no shard addresses provided — all RPCs will return Unavailable");
    }

    let server = CoordinatorServer::new(CoordinatorState::new(), my_port, shard_addrs)?;

    Server::builder()
        .add_service(CoordinatorServiceServer::new(server))
        .serve(bind)
        .await?;

    Ok(())
}
