//! HeltesDB benchmark binary.
//!
//! Spins up an in-process cluster and runs a configurable transaction workload,
//! emitting JSON with throughput (tx/s), latency percentiles, and abort rate.
//!
//! Usage: bench [OPTIONS]
//!
//! Options:
//!   --shards <N>                number of shards (default: 4)
//!   --workers <N>               concurrent workers (default: 50)
//!   --duration <secs>           measurement window in seconds (default: 10)
//!   --warmup <secs>             warmup period in seconds (default: 2)
//!   --keyspace <N>              distinct keys (default: 10000)
//!   --zipf-alpha <f>            Zipf skew parameter (default: 0.0 = uniform)
//!   --read-fraction <f>         fraction of read-only txs (default: 0.0)
//!   --multi-shard-fraction <f>  fraction of write txs that span 2 shards (default: 0.0)
//!   --updates-per-tx <N>        writes per write transaction (default: 1)
//!   --profile <name>            preset: uniform-write | hot-key | read-heavy | multi-shard
//!   --out <path>                write JSON to file instead of stdout

use heltes_db::bench::{run_benchmark, WorkloadConfig};

fn parse_args() -> Result<(WorkloadConfig, Option<String>), Box<dyn std::error::Error>> {
    let mut config = WorkloadConfig::default();
    let mut out_path: Option<String> = None;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--profile" => {
                let v = args.next().ok_or("--profile requires a value")?;
                config = match v.as_str() {
                    "uniform-write" => WorkloadConfig::uniform_write(),
                    "hot-key" => WorkloadConfig::hot_key(),
                    "read-heavy" => WorkloadConfig::read_heavy(),
                    "multi-shard" => WorkloadConfig::multi_shard(),
                    other => return Err(format!("unknown profile: {other}").into()),
                };
            }
            "--shards" => {
                let v = args.next().ok_or("--shards requires a value")?;
                config.shards = v.parse().map_err(|_| format!("invalid --shards: {v}"))?;
            }
            "--workers" => {
                let v = args.next().ok_or("--workers requires a value")?;
                config.workers = v.parse().map_err(|_| format!("invalid --workers: {v}"))?;
            }
            "--duration" => {
                let v = args.next().ok_or("--duration requires a value")?;
                config.duration_secs = v.parse().map_err(|_| format!("invalid --duration: {v}"))?;
            }
            "--warmup" => {
                let v = args.next().ok_or("--warmup requires a value")?;
                config.warmup_secs = v.parse().map_err(|_| format!("invalid --warmup: {v}"))?;
            }
            "--keyspace" => {
                let v = args.next().ok_or("--keyspace requires a value")?;
                config.keyspace = v.parse().map_err(|_| format!("invalid --keyspace: {v}"))?;
            }
            "--zipf-alpha" => {
                let v = args.next().ok_or("--zipf-alpha requires a value")?;
                config.zipf_alpha = v
                    .parse()
                    .map_err(|_| format!("invalid --zipf-alpha: {v}"))?;
            }
            "--read-fraction" => {
                let v = args.next().ok_or("--read-fraction requires a value")?;
                config.read_fraction = v
                    .parse()
                    .map_err(|_| format!("invalid --read-fraction: {v}"))?;
            }
            "--multi-shard-fraction" => {
                let v = args
                    .next()
                    .ok_or("--multi-shard-fraction requires a value")?;
                config.multi_shard_fraction = v
                    .parse()
                    .map_err(|_| format!("invalid --multi-shard-fraction: {v}"))?;
            }
            "--updates-per-tx" => {
                let v = args.next().ok_or("--updates-per-tx requires a value")?;
                config.updates_per_tx = v
                    .parse()
                    .map_err(|_| format!("invalid --updates-per-tx: {v}"))?;
            }
            "--out" => {
                out_path = Some(args.next().ok_or("--out requires a value")?);
            }
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            other => return Err(format!("unknown argument: {other}").into()),
        }
    }
    Ok((config, out_path))
}

fn print_usage() {
    eprintln!("Usage: bench [OPTIONS]");
    eprintln!();
    eprintln!("Options:");
    eprintln!("  --shards <N>                number of shards (default: 4)");
    eprintln!("  --workers <N>               concurrent workers (default: 50)");
    eprintln!("  --duration <secs>           measurement window (default: 10)");
    eprintln!("  --warmup <secs>             warmup period (default: 2)");
    eprintln!("  --keyspace <N>              distinct keys (default: 10000)");
    eprintln!("  --zipf-alpha <f>            Zipf skew parameter (default: 0.0 = uniform)");
    eprintln!("  --read-fraction <f>         fraction of read-only txs (default: 0.0)");
    eprintln!(
        "  --multi-shard-fraction <f>  fraction of write txs that span 2 shards (default: 0.0)"
    );
    eprintln!("  --updates-per-tx <N>        writes per write tx (default: 1)");
    eprintln!(
        "  --profile <name>            preset: uniform-write | hot-key | read-heavy | multi-shard"
    );
    eprintln!("  --out <path>                write JSON to file (default: stdout)");
}

#[tokio::main]
async fn main() {
    let (config, out_path) = match parse_args() {
        Ok(v) => v,
        Err(e) => {
            eprintln!("Error: {e}");
            print_usage();
            std::process::exit(1);
        }
    };

    eprintln!(
        "Starting: {} shard(s), {} worker(s), {}s measurement + {}s warmup",
        config.shards, config.workers, config.duration_secs, config.warmup_secs
    );
    eprintln!(
        "  keyspace={}, zipf_alpha={:.2}, read_frac={:.2}, \
         multi_shard_frac={:.2}, updates_per_tx={}",
        config.keyspace,
        config.zipf_alpha,
        config.read_fraction,
        config.multi_shard_fraction,
        config.updates_per_tx
    );

    let result = run_benchmark(config).await;

    eprintln!(
        "Done: {:.0} tx/s  p50={} µs  p95={} µs  p99={} µs  \
         abort_rate={:.1}%  committed={}  aborted={}  errors={}",
        result.throughput_tps,
        result.latency_p50_us,
        result.latency_p95_us,
        result.latency_p99_us,
        result.abort_rate * 100.0,
        result.total_committed,
        result.total_aborted,
        result.total_errors,
    );

    let json = result.to_json();
    match out_path {
        Some(ref path) => {
            std::fs::write(path, &json).unwrap_or_else(|e| {
                eprintln!("Error writing {path}: {e}");
                std::process::exit(1);
            });
            eprintln!("JSON written to {path}");
        }
        None => println!("{json}"),
    }
}
