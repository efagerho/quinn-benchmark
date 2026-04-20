//! QUIC server-side handshake throughput benchmark.
//!
//! Drives N concurrent client workers against a single server endpoint over loopback,
//! completing --total handshakes total. Each handshake is short-lived (open, wait for
//! `Connecting` to resolve, close, reap), so the server's accept hot path dominates the
//! workload. Reports throughput (connections/second) and per-handshake latency percentiles.
//!
//! The benchmark is designed to exercise exactly the code path changed by the
//! `partial_accept` / `commit_accept` refactor in `proto::Endpoint` /
//! `quinn::EndpointInner::accept`: rustls session setup, payload decrypt, `Connection::new`
//! and initial-packet processing, which on `main` run under the endpoint's state mutex and
//! on this branch run off-lock.
//!
//! Usage (from the repo root, after building both branches of the `quinn/` submodule):
//!
//! ```text
//! # Build + run against current submodule state
//! cargo run --release --bin handshake_bench -- \
//!     --total 20000 --concurrency 512 --server-threads 8 --client-threads 4 --warmup 500
//! ```
//!
//! To compare branches, check out the submodule at a branch, rebuild, record, repeat:
//!
//! ```text
//! (cd quinn && git checkout main) && cargo build --release --bin handshake_bench
//! target/release/handshake_bench --total 20000 --concurrency 512 ... > main.txt
//! (cd quinn && git checkout endpoint-two-phase-accept) && cargo build --release --bin handshake_bench
//! target/release/handshake_bench --total 20000 --concurrency 512 ... > refactor.txt
//! ```

use std::{
    net::{IpAddr, Ipv6Addr, SocketAddr},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use clap::Parser;
use quinn_benchmark::{
    build_client_config, build_server_config, ensure_crypto_provider, generate_self_signed,
};
use tokio::{
    runtime::{Builder, Runtime},
    task::JoinSet,
};

#[derive(Parser, Debug, Clone)]
#[clap(
    name = "handshake_bench",
    about = "Measure QUIC server-side handshake throughput."
)]
struct Opt {
    /// Total number of measured handshakes to complete.
    #[clap(long, default_value_t = 10_000)]
    total: usize,

    /// Number of concurrent client workers (upper bound on in-flight handshakes).
    #[clap(long, default_value_t = 256)]
    concurrency: usize,

    /// Number of warmup handshakes (not counted in throughput/latency stats).
    #[clap(long, default_value_t = 500)]
    warmup: usize,

    /// Worker threads for the server tokio runtime. 0 = CPU count.
    #[clap(long, default_value_t = 0)]
    server_threads: usize,

    /// Worker threads for the client tokio runtime. 0 = CPU count.
    #[clap(long, default_value_t = 0)]
    client_threads: usize,

    /// Print a machine-readable single-line summary after the human-readable report.
    #[clap(long)]
    csv: bool,

    /// Optional label to include in the CSV summary (e.g. branch name).
    #[clap(long)]
    label: Option<String>,
}

fn main() -> Result<()> {
    ensure_crypto_provider();
    // Keep tracing quiet by default so its cost doesn't pollute the measurement.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("error")),
        )
        .try_init();

    let opt = Opt::parse();
    if opt.concurrency == 0 {
        anyhow::bail!("--concurrency must be > 0");
    }
    if opt.total == 0 {
        anyhow::bail!("--total must be > 0");
    }

    let (cert, key) = generate_self_signed();
    let server_config = build_server_config(cert.clone(), key)?;
    let client_config = build_client_config(cert)?;

    let server_rt = build_rt("bench-server", opt.server_threads)?;
    let client_rt = build_rt("bench-client", opt.client_threads)?;

    // Start the server endpoint + accept loop.
    let shutdown = Arc::new(AtomicBool::new(false));
    let (server_addr, server_endpoint) = {
        let _guard = server_rt.enter();
        let endpoint = quinn::Endpoint::server(
            server_config,
            SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 0),
        )?;
        let addr = endpoint.local_addr()?;
        (addr, endpoint)
    };
    let server_task = server_rt.spawn(run_server(server_endpoint.clone(), shutdown.clone()));

    let stats = client_rt
        .block_on(run_workload(
            server_addr,
            client_config,
            opt.clone(),
        ))
        .context("workload failed")?;

    // Shut the server down cleanly so the Endpoint driver exits and drops its resources.
    shutdown.store(true, Ordering::Relaxed);
    server_endpoint.close(0u32.into(), b"bench done");
    server_rt.block_on(async {
        server_endpoint.wait_idle().await;
        // Abort the accept loop; it only exits when close() is observed *and* there is
        // no pending incoming.
        server_task.abort();
        let _ = server_task.await;
    });

    report(&opt, &stats);

    Ok(())
}

fn build_rt(name: &str, threads: usize) -> Result<Runtime> {
    let mut b = Builder::new_multi_thread();
    b.enable_all();
    b.thread_name(name);
    if threads > 0 {
        b.worker_threads(threads);
    }
    Ok(b.build()?)
}

/// Accept loop: for each `Incoming`, spawn a task that drives it through the handshake and
/// then closes the connection. Each spawned task performs the synchronous
/// `EndpointInner::accept` call on whichever worker thread picks it up; with a multi-threaded
/// runtime, that lets the refactor's off-lock crypto/TLS/Connection-setup work parallelise.
async fn run_server(endpoint: quinn::Endpoint, shutdown: Arc<AtomicBool>) {
    loop {
        let incoming = match endpoint.accept().await {
            Some(i) => i,
            None => return,
        };
        if shutdown.load(Ordering::Relaxed) {
            incoming.ignore();
            continue;
        }
        tokio::spawn(async move {
            let connecting = match incoming.accept() {
                Ok(c) => c,
                Err(_) => return,
            };
            let Ok(conn) = connecting.await else {
                return;
            };
            // Wait for the client-initiated close so the cleanup happens here instead of
            // piling onto the client's runtime.
            let _ = conn.closed().await;
        });
    }
}

struct WorkloadStats {
    wall: Duration,
    /// Per-handshake wall-clock durations, already sorted ascending.
    latencies: Vec<Duration>,
    errors: usize,
}

async fn run_workload(
    server_addr: SocketAddr,
    client_config: quinn::ClientConfig,
    opt: Opt,
) -> Result<WorkloadStats> {
    // One long-lived client endpoint per worker. This pins the client-side UDP socket and
    // `EndpointInner` across every handshake that worker performs, so the measurement is
    // dominated by *server-side* handshake cost rather than per-handshake client-endpoint
    // setup.
    let workers = opt.concurrency;
    let total = opt.total + opt.warmup;
    let mut per_worker = vec![total / workers; workers];
    for slot in per_worker.iter_mut().take(total % workers) {
        *slot += 1;
    }
    let warmup_per_worker = {
        let mut v = vec![opt.warmup / workers; workers];
        for slot in v.iter_mut().take(opt.warmup % workers) {
            *slot += 1;
        }
        v
    };

    let mut join = JoinSet::new();
    let start_barrier = Arc::new(tokio::sync::Barrier::new(workers + 1));

    for (count, warmup) in per_worker.into_iter().zip(warmup_per_worker.into_iter()) {
        if count == 0 {
            continue;
        }
        let config = client_config.clone();
        let barrier = start_barrier.clone();
        join.spawn(async move {
            let client_endpoint = quinn::Endpoint::client(SocketAddr::new(
                IpAddr::V6(Ipv6Addr::LOCALHOST),
                0,
            ))
            .context("create client endpoint")?;

            // Warmup: untimed, discard stats.
            for _ in 0..warmup {
                let _ = handshake_once(&client_endpoint, &config, server_addr).await;
            }

            // Align the start of the measured region across workers so `wall` covers the
            // steady-state region rather than endpoint construction + warmup.
            barrier.wait().await;

            let measured = count - warmup;
            let mut latencies = Vec::with_capacity(measured);
            let mut errors = 0usize;
            for _ in 0..measured {
                let t0 = Instant::now();
                match handshake_once(&client_endpoint, &config, server_addr).await {
                    Ok(()) => latencies.push(t0.elapsed()),
                    Err(_) => errors += 1,
                }
            }

            // Keep the endpoint alive until reporting is done so dropping it doesn't race
            // with background drivers.
            drop(client_endpoint);
            Ok::<_, anyhow::Error>(WorkerOutput { latencies, errors })
        });
    }

    // Release the workers into the measured phase.
    start_barrier.wait().await;
    let wall_start = Instant::now();

    let mut all_latencies: Vec<Duration> = Vec::with_capacity(opt.total);
    let mut errors = 0usize;
    while let Some(res) = join.join_next().await {
        match res {
            Ok(Ok(out)) => {
                all_latencies.extend(out.latencies);
                errors += out.errors;
            }
            Ok(Err(e)) => {
                eprintln!("worker failed: {e:#}");
                errors += 1;
            }
            Err(e) => {
                eprintln!("worker panicked: {e}");
                errors += 1;
            }
        }
    }
    let wall = wall_start.elapsed();
    all_latencies.sort_unstable();
    Ok(WorkloadStats {
        wall,
        latencies: all_latencies,
        errors,
    })
}

struct WorkerOutput {
    latencies: Vec<Duration>,
    errors: usize,
}

async fn handshake_once(
    endpoint: &quinn::Endpoint,
    config: &quinn::ClientConfig,
    server_addr: SocketAddr,
) -> Result<()> {
    let connecting = endpoint.connect_with(config.clone(), server_addr, "localhost")?;
    let conn = connecting.await.context("handshake failed")?;
    conn.close(0u32.into(), b"done");
    let _ = conn.closed().await;
    Ok(())
}

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn report(opt: &Opt, stats: &WorkloadStats) {
    let measured = stats.latencies.len();
    let errors = stats.errors;
    let throughput = measured as f64 / stats.wall.as_secs_f64();
    let p50 = percentile(&stats.latencies, 0.50);
    let p90 = percentile(&stats.latencies, 0.90);
    let p95 = percentile(&stats.latencies, 0.95);
    let p99 = percentile(&stats.latencies, 0.99);
    let p999 = percentile(&stats.latencies, 0.999);
    let max = stats.latencies.last().copied().unwrap_or_default();
    let mean = if measured == 0 {
        Duration::ZERO
    } else {
        Duration::from_nanos(
            (stats
                .latencies
                .iter()
                .map(|d| d.as_nanos())
                .sum::<u128>()
                / measured as u128) as u64,
        )
    };

    println!("\n=== QUIC server-side handshake benchmark ===");
    println!("configuration:");
    println!("  --total           {}", opt.total);
    println!("  --concurrency     {}", opt.concurrency);
    println!("  --warmup          {}", opt.warmup);
    println!(
        "  --server-threads  {} ({} active)",
        opt.server_threads,
        if opt.server_threads == 0 {
            "auto".to_string()
        } else {
            opt.server_threads.to_string()
        }
    );
    println!(
        "  --client-threads  {} ({} active)",
        opt.client_threads,
        if opt.client_threads == 0 {
            "auto".to_string()
        } else {
            opt.client_threads.to_string()
        }
    );
    println!();
    println!("results:");
    println!(
        "  completed:   {} / {} (errors: {})",
        measured, opt.total, errors
    );
    println!("  wall:        {:.3?}", stats.wall);
    println!("  throughput:  {:.1} conn/s", throughput);
    println!("  latency:");
    println!("    mean       {:?}", mean);
    println!("    p50        {:?}", p50);
    println!("    p90        {:?}", p90);
    println!("    p95        {:?}", p95);
    println!("    p99        {:?}", p99);
    println!("    p99.9      {:?}", p999);
    println!("    max        {:?}", max);

    if opt.csv {
        let label = opt.label.as_deref().unwrap_or("-");
        println!(
            "CSV,{label},{total},{concurrency},{server_threads},{client_threads},{measured},{errors},{wall_ms:.3},{throughput:.1},{mean_us},{p50_us},{p90_us},{p95_us},{p99_us},{p999_us},{max_us}",
            total = opt.total,
            concurrency = opt.concurrency,
            server_threads = opt.server_threads,
            client_threads = opt.client_threads,
            wall_ms = stats.wall.as_secs_f64() * 1000.0,
            mean_us = mean.as_micros(),
            p50_us = p50.as_micros(),
            p90_us = p90.as_micros(),
            p95_us = p95.as_micros(),
            p99_us = p99.as_micros(),
            p999_us = p999.as_micros(),
            max_us = max.as_micros(),
        );
    }
}
