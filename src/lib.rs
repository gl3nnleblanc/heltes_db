pub mod coordinator;
pub mod shard;

pub mod proto {
    tonic::include_proto!("heltes_db");
}
