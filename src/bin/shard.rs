use std::net::SocketAddr;

use tonic::transport::Server;

use heltes_db::shard::{
    server::{ShardServer, ShardServiceServer},
    ShardState,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr: SocketAddr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "[::1]:50051".to_string())
        .parse()?;

    eprintln!("shard listening on {addr}");

    Server::builder()
        .add_service(ShardServiceServer::new(ShardServer::new(ShardState::new())))
        .serve(addr)
        .await?;

    Ok(())
}
