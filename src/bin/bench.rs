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
//!   --save-baseline <path>      run benchmark and save result as a new baseline file
//!   --check-baseline <path>     run benchmark and fail (exit 1) if it regresses vs baseline

use heltes_db::bench::{run_benchmark, BenchBaseline, WorkloadConfig};

struct Args {
    config: WorkloadConfig,
    out_path: Option<String>,
    save_baseline: Option<String>,
    check_baseline: Option<String>,
}

fn parse_args() -> Result<Args, Box<dyn std::error::Error>> {
    let mut config = WorkloadConfig::default();
    let mut out_path: Option<String> = None;
    let mut save_baseline: Option<String> = None;
    let mut check_baseline: Option<String> = None;

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
            "--coordinators" => {
                let v = args.next().ok_or("--coordinators requires a value")?;
                config.coordinators = v
                    .parse()
                    .map_err(|_| format!("invalid --coordinators: {v}"))?;
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
            "--save-baseline" => {
                save_baseline = Some(args.next().ok_or("--save-baseline requires a value")?);
            }
            "--check-baseline" => {
                check_baseline = Some(args.next().ok_or("--check-baseline requires a value")?);
            }
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            other => return Err(format!("unknown argument: {other}").into()),
        }
    }
    Ok(Args {
        config,
        out_path,
        save_baseline,
        check_baseline,
    })
}

fn print_usage() {
    eprintln!("Usage: bench [OPTIONS]");
    eprintln!();
    eprintln!("Options:");
    eprintln!("  --coordinators <N>          number of coordinators (default: 1)");
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
    eprintln!("  --save-baseline <path>      save current result as baseline for future checks");
    eprintln!("  --check-baseline <path>     fail (exit 1) if result regresses vs stored baseline");
    eprintln!();
    eprintln!("Thresholds (when --check-baseline is used):");
    eprintln!("  Fail if throughput drops more than 10 % below baseline.");
    eprintln!("  Fail if p99 latency rises more than 20 % above baseline.");
}

#[tokio::main]
async fn main() {
    let Args {
        config,
        out_path,
        save_baseline,
        check_baseline,
    } = match parse_args() {
        Ok(v) => v,
        Err(e) => {
            eprintln!("Error: {e}");
            print_usage();
            std::process::exit(1);
        }
    };

    // Load and validate the baseline file before running so a bad path fails fast.
    let baseline = match &check_baseline {
        Some(path) => {
            let text = std::fs::read_to_string(path).unwrap_or_else(|e| {
                eprintln!("Error reading baseline file {path}: {e}");
                std::process::exit(1);
            });
            let b = BenchBaseline::from_json(&text).unwrap_or_else(|e| {
                eprintln!("Error parsing baseline file {path}: {e}");
                std::process::exit(1);
            });
            eprintln!(
                "Baseline: {:.0} tx/s  p99={} µs",
                b.throughput_tps, b.latency_p99_us
            );
            Some(b)
        }
        None => None,
    };

    eprintln!(
        "Starting: {} coordinator(s), {} shard(s), {} worker(s), {}s measurement + {}s warmup",
        config.coordinators,
        config.shards,
        config.workers,
        config.duration_secs,
        config.warmup_secs
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

    // ── --save-baseline ───────────────────────────────────────────────────────
    if let Some(ref path) = save_baseline {
        let b = BenchBaseline::from(&result);
        let text = b.to_json();
        std::fs::write(path, &text).unwrap_or_else(|e| {
            eprintln!("Error writing baseline {path}: {e}");
            std::process::exit(1);
        });
        eprintln!("Baseline saved to {path}");
    }

    // ── --check-baseline ──────────────────────────────────────────────────────
    if let Some(b) = baseline {
        // 10 % throughput drop  /  20 % p99 rise are the failure thresholds.
        let reg = b.check(&result, 0.10, 0.20);
        eprintln!(
            "Regression check: tps_ratio={:.3}  p99_ratio={:.3}",
            reg.tps_ratio, reg.p99_ratio
        );
        if reg.tps_regressed {
            eprintln!(
                "REGRESSION: throughput {:.0} tx/s is more than 10% below baseline {:.0} tx/s",
                result.throughput_tps, b.throughput_tps
            );
        }
        if reg.p99_regressed {
            eprintln!(
                "REGRESSION: p99 {} µs is more than 20% above baseline {} µs",
                result.latency_p99_us, b.latency_p99_us
            );
        }
        if reg.any_regression() {
            std::process::exit(1);
        }
        eprintln!("No regression detected.");
    }
}
