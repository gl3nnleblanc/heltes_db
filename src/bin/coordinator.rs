use std::net::SocketAddr;
use std::time::Duration;

use tonic::transport::Server;

use heltes_db::coordinator::{
    server::{CoordinatorServer, CoordinatorServiceServer},
    CoordinatorState, ReadRetryPolicy,
};

/// Usage: coordinator [BIND_ADDR] [SHARD_ADDR...] [-- PEER_COORD_ADDR...]
///                    [--shard-rpc-timeout-ms N] [--read-loop-timeout-ms N]
///                    [--clock-sync-interval-ms N]
///
///   coordinator [::1]:50052 [::1]:50051 -- [::1]:50053
///
/// BIND_ADDR defaults to [::1]:50052.
/// SHARD_ADDRs are the shards this coordinator routes to (before --).
/// PEER_COORD_ADDRs (after --) are other coordinators this one may forward
/// cross-coordinator Inquire RPCs to. Required for multi-coordinator deployments.
/// --shard-rpc-timeout-ms    per-RPC shard timeout in milliseconds (default: 30000)
/// --read-loop-timeout-ms    read inquiry-loop timeout in milliseconds (default: 30000)
/// --clock-sync-interval-ms  background clock re-sync cadence in milliseconds (default: 100)
///                           Set to 0 to disable periodic re-sync (startup sync only).
struct CoordArgs {
    bind: SocketAddr,
    shard_addrs: Vec<SocketAddr>,
    peer_coordinator_addrs: Vec<SocketAddr>,
    shard_rpc_timeout: Duration,
    read_loop_timeout: Duration,
    clock_sync_interval: Option<Duration>,
}

/// Default interval between background coordinator clock re-syncs.
/// Matches the shard's DEFAULT_COMPACT_INTERVAL so both background tasks
/// tick at the same cadence, keeping drift bounded to one compact interval.
const DEFAULT_CLOCK_SYNC_INTERVAL: Duration = Duration::from_millis(100);

fn parse_args(args: impl Iterator<Item = String>) -> Result<CoordArgs, Box<dyn std::error::Error>> {
    let mut shard_rpc_timeout = Duration::from_secs(30);
    let mut read_loop_timeout = Duration::from_secs(30);
    let mut clock_sync_interval: Option<Duration> = Some(DEFAULT_CLOCK_SYNC_INTERVAL);
    let mut positional: Vec<String> = Vec::new();

    let mut args = args.peekable();
    while let Some(arg) = args.next() {
        match arg.as_str() {
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
            "--clock-sync-interval-ms" => {
                let v = args
                    .next()
                    .ok_or("--clock-sync-interval-ms requires a value")?;
                let ms: u64 = v
                    .parse()
                    .map_err(|_| format!("invalid --clock-sync-interval-ms value: {v}"))?;
                clock_sync_interval = if ms == 0 {
                    None
                } else {
                    Some(Duration::from_millis(ms))
                };
            }
            "--help" | "-h" => {
                eprintln!(
                    "Usage: coordinator [BIND_ADDR] [SHARD_ADDR...] \
                     [-- PEER_COORD_ADDR...]\n\
                     \x20              [--shard-rpc-timeout-ms N] [--read-loop-timeout-ms N]\n\
                     \x20              [--clock-sync-interval-ms N]"
                );
                std::process::exit(0);
            }
            other => positional.push(other.to_string()),
        }
    }

    // Split positional args at "--": before → bind addr + shard addrs, after → peer coords.
    let sep = positional.iter().position(|a| a == "--");
    let (before_sep, after_sep) = match sep {
        Some(i) => (&positional[..i], &positional[i + 1..]),
        None => (positional.as_slice(), [].as_slice()),
    };

    let bind: SocketAddr = before_sep
        .first()
        .map(|s| s.as_str())
        .unwrap_or("[::1]:50052")
        .parse()?;

    let shard_addrs: Vec<SocketAddr> = before_sep
        .get(1..)
        .unwrap_or(&[])
        .iter()
        .map(|s| s.parse::<SocketAddr>())
        .collect::<Result<_, _>>()?;

    let peer_coordinator_addrs: Vec<SocketAddr> = after_sep
        .iter()
        .map(|s| s.parse::<SocketAddr>())
        .collect::<Result<_, _>>()?;

    Ok(CoordArgs {
        bind,
        shard_addrs,
        peer_coordinator_addrs,
        shard_rpc_timeout,
        read_loop_timeout,
        clock_sync_interval,
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let CoordArgs {
        bind,
        shard_addrs,
        peer_coordinator_addrs,
        shard_rpc_timeout,
        read_loop_timeout,
        clock_sync_interval,
    } = parse_args(std::env::args().skip(1))?;

    let my_port = bind.port();

    eprintln!(
        "coordinator listening on {bind} (port={my_port}, shards={shard_addrs:?}, \
         peers={peer_coordinator_addrs:?}, shard_rpc_timeout={shard_rpc_timeout:?}, \
         read_loop_timeout={read_loop_timeout:?}, clock_sync_interval={clock_sync_interval:?})"
    );

    if shard_addrs.is_empty() {
        eprintln!("warning: no shard addresses provided — all RPCs will return Unavailable");
    }

    let server = CoordinatorServer::new(
        CoordinatorState::new(),
        my_port,
        shard_addrs,
        peer_coordinator_addrs,
        shard_rpc_timeout,
        read_loop_timeout,
        ReadRetryPolicy::default_policy(),
    )?;

    // Sync coordinator clock from all shards before serving requests.
    // This prevents CommittedConflict aborts when joining a cluster that already
    // has committed transactions (CoordSyncClock in TLA+).
    server.sync_clock_from_shards().await;

    // Start periodic background clock re-sync so the coordinator's clock stays
    // current even when it processes only reads and no commits.
    // Matches CoordSyncClockBatch in TLA+, which fires any number of times.
    if let Some(interval) = clock_sync_interval {
        server.start_clock_sync(interval);
    }

    Server::builder()
        .add_service(CoordinatorServiceServer::new(server))
        .serve(bind)
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args<'a>(s: &'a [&'a str]) -> impl Iterator<Item = String> + 'a {
        s.iter().map(|a| a.to_string())
    }

    // Trace: default invocation — no flags → all defaults.
    #[test]
    fn defaults() {
        let a = parse_args(std::iter::empty()).unwrap();
        assert_eq!(a.bind, "[::1]:50052".parse::<SocketAddr>().unwrap());
        assert!(a.shard_addrs.is_empty());
        assert!(a.peer_coordinator_addrs.is_empty());
        assert_eq!(a.shard_rpc_timeout, Duration::from_secs(30));
        assert_eq!(a.read_loop_timeout, Duration::from_secs(30));
        assert_eq!(a.clock_sync_interval, Some(DEFAULT_CLOCK_SYNC_INTERVAL));
    }

    // Trace: --clock-sync-interval-ms overrides the periodic sync cadence.
    #[test]
    fn custom_clock_sync_interval() {
        let a = parse_args(args(&["--clock-sync-interval-ms", "500"])).unwrap();
        assert_eq!(a.clock_sync_interval, Some(Duration::from_millis(500)));
    }

    // Trace: --clock-sync-interval-ms 0 disables periodic sync (startup sync only).
    #[test]
    fn clock_sync_interval_zero_disables_periodic_sync() {
        let a = parse_args(args(&["--clock-sync-interval-ms", "0"])).unwrap();
        assert_eq!(a.clock_sync_interval, None);
    }

    // Trace: --clock-sync-interval-ms with no value → error.
    #[test]
    fn clock_sync_interval_missing_value() {
        assert!(parse_args(args(&["--clock-sync-interval-ms"])).is_err());
    }

    // Trace: --clock-sync-interval-ms with non-numeric value → error.
    #[test]
    fn clock_sync_interval_invalid_value() {
        assert!(parse_args(args(&["--clock-sync-interval-ms", "abc"])).is_err());
    }

    // Trace: --shard-rpc-timeout-ms overrides the shard RPC timeout.
    #[test]
    fn custom_shard_rpc_timeout() {
        let a = parse_args(args(&["--shard-rpc-timeout-ms", "2000"])).unwrap();
        assert_eq!(a.shard_rpc_timeout, Duration::from_millis(2000));
        assert_eq!(a.read_loop_timeout, Duration::from_secs(30));
    }

    // Trace: --read-loop-timeout-ms overrides the read-loop timeout.
    #[test]
    fn custom_read_loop_timeout() {
        let a = parse_args(args(&["--read-loop-timeout-ms", "1000"])).unwrap();
        assert_eq!(a.read_loop_timeout, Duration::from_millis(1000));
        assert_eq!(a.shard_rpc_timeout, Duration::from_secs(30));
    }

    // Trace: both timeouts customised simultaneously.
    #[test]
    fn both_timeouts() {
        let a = parse_args(args(&[
            "--shard-rpc-timeout-ms",
            "3000",
            "--read-loop-timeout-ms",
            "5000",
        ]))
        .unwrap();
        assert_eq!(a.shard_rpc_timeout, Duration::from_millis(3000));
        assert_eq!(a.read_loop_timeout, Duration::from_millis(5000));
    }

    // Trace: positional args (bind + shards + peers) combined with timeout flags.
    #[test]
    fn full_args_with_timeouts() {
        let a = parse_args(args(&[
            "[::1]:50052",
            "127.0.0.1:9001",
            "127.0.0.1:9002",
            "--",
            "127.0.0.1:9010",
            "--shard-rpc-timeout-ms",
            "3000",
            "--read-loop-timeout-ms",
            "4000",
        ]))
        .unwrap();
        assert_eq!(a.bind, "[::1]:50052".parse::<SocketAddr>().unwrap());
        assert_eq!(a.shard_addrs.len(), 2);
        assert_eq!(a.peer_coordinator_addrs.len(), 1);
        assert_eq!(a.shard_rpc_timeout, Duration::from_millis(3000));
        assert_eq!(a.read_loop_timeout, Duration::from_millis(4000));
    }

    // Trace: flags may appear before or between positional args.
    #[test]
    fn flags_interleaved_with_positional() {
        let a = parse_args(args(&[
            "[::1]:50052",
            "--shard-rpc-timeout-ms",
            "500",
            "127.0.0.1:9001",
        ]))
        .unwrap();
        assert_eq!(a.shard_rpc_timeout, Duration::from_millis(500));
        assert_eq!(
            a.shard_addrs,
            vec!["127.0.0.1:9001".parse::<SocketAddr>().unwrap()]
        );
    }

    // Trace: --shard-rpc-timeout-ms with no value → error.
    #[test]
    fn shard_rpc_timeout_missing_value() {
        assert!(parse_args(args(&["--shard-rpc-timeout-ms"])).is_err());
    }

    // Trace: --read-loop-timeout-ms with no value → error.
    #[test]
    fn read_loop_timeout_missing_value() {
        assert!(parse_args(args(&["--read-loop-timeout-ms"])).is_err());
    }

    // Trace: non-numeric timeout value → error.
    #[test]
    fn shard_rpc_timeout_invalid_value() {
        assert!(parse_args(args(&["--shard-rpc-timeout-ms", "oops"])).is_err());
    }
}
