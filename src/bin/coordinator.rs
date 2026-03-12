use std::net::SocketAddr;

use tonic::transport::Server;

use heltes_db::coordinator::{
    server::{CoordinatorServer, CoordinatorServiceServer},
    CoordinatorState, TxIdGen,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr: SocketAddr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "[::1]:50052".to_string())
        .parse()?;

    let port = addr.port();
    eprintln!("coordinator listening on {addr} (coordinator_port={port})");

    // TxIdGen encodes this coordinator's port into every tx_id it issues,
    // letting shards route Inquire RPCs back here without a lookup service.
    let _gen = TxIdGen::new(port);

    Server::builder()
        .add_service(CoordinatorServiceServer::new(CoordinatorServer::new(
            CoordinatorState::new(),
        )))
        .serve(addr)
        .await?;

    Ok(())
}
