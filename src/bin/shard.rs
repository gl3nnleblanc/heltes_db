use std::net::SocketAddr;
use std::time::Duration;

use tonic::transport::Server;

use heltes_db::shard::{
    server::{ShardServer, ShardServiceServer},
    ShardState,
};

struct ShardArgs {
    addr: SocketAddr,
    prepare_ttl: Duration,
    max_writes_per_tx: usize,
}

fn parse_args(
    mut args: impl Iterator<Item = String>,
) -> Result<ShardArgs, Box<dyn std::error::Error>> {
    let mut addr: SocketAddr = "[::1]:50051".parse().unwrap();
    let mut prepare_ttl = Duration::from_secs(30);
    let mut max_writes_per_tx: usize = usize::MAX;
    let mut addr_parsed = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--prepare-ttl-ms" => {
                let v = args.next().ok_or("--prepare-ttl-ms requires a value")?;
                let ms: u64 = v
                    .parse()
                    .map_err(|_| format!("invalid --prepare-ttl-ms value: {v}"))?;
                prepare_ttl = Duration::from_millis(ms);
            }
            "--max-writes-per-tx" => {
                let v = args.next().ok_or("--max-writes-per-tx requires a value")?;
                max_writes_per_tx = v
                    .parse::<usize>()
                    .map_err(|_| format!("invalid --max-writes-per-tx value: {v}"))?;
                if max_writes_per_tx == 0 {
                    return Err("--max-writes-per-tx must be >= 1".into());
                }
            }
            "--help" | "-h" => {
                eprintln!("Usage: shard [BIND_ADDR] [OPTIONS]");
                eprintln!("  BIND_ADDR              bind address (default: [::1]:50051)");
                eprintln!(
                    "  --prepare-ttl-ms N     prepared-entry TTL in milliseconds (default: 30000)"
                );
                eprintln!(
                    "  --max-writes-per-tx N  max distinct keys per tx write buffer (default: unlimited)"
                );
                std::process::exit(0);
            }
            other if !addr_parsed => {
                addr = other
                    .parse()
                    .map_err(|e| format!("invalid bind address '{other}': {e}"))?;
                addr_parsed = true;
            }
            other => return Err(format!("unknown argument: {other}").into()),
        }
    }

    Ok(ShardArgs {
        addr,
        prepare_ttl,
        max_writes_per_tx,
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ShardArgs {
        addr,
        prepare_ttl,
        max_writes_per_tx,
    } = parse_args(std::env::args().skip(1))?;

    eprintln!(
        "shard listening on {addr} (prepare_ttl={prepare_ttl:?}, max_writes_per_tx={})",
        if max_writes_per_tx == usize::MAX {
            "unlimited".to_string()
        } else {
            max_writes_per_tx.to_string()
        }
    );

    let mut state = ShardState::new();
    state.prepare_ttl = prepare_ttl;
    state.max_writes_per_tx = max_writes_per_tx;

    Server::builder()
        .add_service(ShardServiceServer::new(ShardServer::new(state)))
        .serve(addr)
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args<'a>(s: &'a [&'a str]) -> impl Iterator<Item = String> + 'a {
        s.iter().map(|a| a.to_string())
    }

    // Trace: default invocation — no flags → default addr, 30 s TTL, unlimited writes.
    #[test]
    fn defaults() {
        let a = parse_args(std::iter::empty()).unwrap();
        assert_eq!(a.addr, "[::1]:50051".parse::<SocketAddr>().unwrap());
        assert_eq!(a.prepare_ttl, Duration::from_secs(30));
        assert_eq!(a.max_writes_per_tx, usize::MAX);
    }

    // Trace: custom bind address only.
    #[test]
    fn custom_addr() {
        let a = parse_args(args(&["127.0.0.1:9001"])).unwrap();
        assert_eq!(a.addr, "127.0.0.1:9001".parse::<SocketAddr>().unwrap());
        assert_eq!(a.prepare_ttl, Duration::from_secs(30));
    }

    // Trace: --prepare-ttl-ms flag overrides default TTL.
    #[test]
    fn custom_ttl() {
        let a = parse_args(args(&["--prepare-ttl-ms", "5000"])).unwrap();
        assert_eq!(a.addr, "[::1]:50051".parse::<SocketAddr>().unwrap());
        assert_eq!(a.prepare_ttl, Duration::from_millis(5000));
    }

    // Trace: custom addr + custom TTL together.
    #[test]
    fn custom_addr_and_ttl() {
        let a = parse_args(args(&["127.0.0.1:9001", "--prepare-ttl-ms", "500"])).unwrap();
        assert_eq!(a.addr, "127.0.0.1:9001".parse::<SocketAddr>().unwrap());
        assert_eq!(a.prepare_ttl, Duration::from_millis(500));
    }

    // Trace: --prepare-ttl-ms with no following value → error.
    #[test]
    fn ttl_missing_value() {
        assert!(parse_args(args(&["--prepare-ttl-ms"])).is_err());
    }

    // Trace: --prepare-ttl-ms with non-numeric value → error.
    #[test]
    fn ttl_invalid_value() {
        assert!(parse_args(args(&["--prepare-ttl-ms", "not_a_number"])).is_err());
    }

    // Trace: --max-writes-per-tx sets the limit.
    #[test]
    fn max_writes_per_tx_flag() {
        let a = parse_args(args(&["--max-writes-per-tx", "100"])).unwrap();
        assert_eq!(a.max_writes_per_tx, 100);
    }

    // Trace: --max-writes-per-tx = 0 is rejected (must be >= 1).
    #[test]
    fn max_writes_per_tx_zero_is_error() {
        assert!(parse_args(args(&["--max-writes-per-tx", "0"])).is_err());
    }

    // Trace: --max-writes-per-tx with no value → error.
    #[test]
    fn max_writes_per_tx_missing_value() {
        assert!(parse_args(args(&["--max-writes-per-tx"])).is_err());
    }

    // Trace: unrecognised flag → error (fails fast, doesn't silently ignore).
    #[test]
    fn unknown_flag() {
        assert!(parse_args(args(&["--unknown"])).is_err());
    }
}
