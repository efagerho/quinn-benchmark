//! QUIC server-side ingress throughput benchmark.
//!
//! Stresses the datagram-ingress code path that the `endpoint-ingress-offlock` refactor
//! changed: `EndpointShared::partial_handle` (payload/header decode + initial-key derivation)
//! now runs *off* the endpoint state mutex, and the `EndpointDriver::poll` loop acquires the
//! state mutex only for the commit phase and event dispatch.
//!
//! The workload:
//!   1. Establishes N long-lived connections from clients to a single server endpoint.
//!   2. Every client worker opens one bidirectional stream per iteration, writes a small
//!      payload, reads the echo, closes the stream, and repeats. Each iteration produces
//!      multiple datagrams in each direction, all of which traverse the server's ingress
//!      path.
//!   3. Runs for a fixed wall-clock duration under sustained concurrency.
//!
//! We intentionally keep payloads small so the datagram count (and therefore the ingress
//! hot path: `partial_handle` + `commit_handle`) dominates, rather than bulk
//! send/recv throughput.
//!
//! Reports aggregate throughput (datagrams/second approx by request-response rate and
//! bytes/second) plus per-request latency percentiles.
//!
//! Usage:
//!
//! ```text
//! cargo run --release --bin ingress_bench -- \
//!     --connections 64 --workers-per-conn 4 --duration-secs 10 \
//!     --server-threads 8 --client-threads 4
//! ```
//!
//! A/B procedure (three target branches: main, endpoint-two-phase-accept,
//! endpoint-ingress-offlock):
//!
//! ```text
//! (cd quinn && git checkout main)                         && cargo build --release --bin ingress_bench
//! target/release/ingress_bench --csv --label main ...
//! (cd quinn && git checkout endpoint-two-phase-accept)    && cargo build --release --bin ingress_bench
//! target/release/ingress_bench --csv --label two-phase ...
//! (cd quinn && git checkout endpoint-ingress-offlock)     && cargo build --release --bin ingress_bench
//! target/release/ingress_bench --csv --label ingress-off ...
//! ```

use std::{
    net::{IpAddr, Ipv6Addr, SocketAddr},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
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
    sync::Barrier,
    task::JoinSet,
};

#[derive(Parser, Debug, Clone)]
#[clap(
    name = "ingress_bench",
    about = "Measure QUIC server-side ingress throughput under sustained concurrency."
)]
struct Opt {
    /// Number of long-lived client connections to the server.
    #[clap(long, default_value_t = 64)]
    connections: usize,

    /// Concurrent request-response loops per connection (each opens its own bi stream
    /// per iteration).
    #[clap(long, default_value_t = 4)]
    workers_per_conn: usize,

    /// Duration of the measured phase, in seconds.
    #[clap(long, default_value_t = 5.0)]
    duration_secs: f64,

    /// Warmup duration before measurement, in seconds.
    #[clap(long, default_value_t = 1.0)]
    warmup_secs: f64,

    /// Bytes written (and echoed back) per request. Keep small to stress per-datagram
    /// ingress rather than bulk throughput.
    #[clap(long, default_value_t = 64)]
    request_bytes: usize,

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
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("error")),
        )
        .try_init();

    let opt = Opt::parse();
    if opt.connections == 0 || opt.workers_per_conn == 0 {
        anyhow::bail!("--connections and --workers-per-conn must be > 0");
    }
    if opt.duration_secs <= 0.0 {
        anyhow::bail!("--duration-secs must be > 0");
    }

    let (cert, key) = generate_self_signed();
    let server_config = build_server_config(cert.clone(), key)?;
    let client_config = build_client_config(cert)?;

    let server_rt = build_rt("bench-server", opt.server_threads)?;
    let client_rt = build_rt("bench-client", opt.client_threads)?;

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
        .block_on(run_workload(server_addr, client_config, opt.clone()))
        .context("workload failed")?;

    shutdown.store(true, Ordering::Relaxed);
    server_endpoint.close(0u32.into(), b"bench done");
    server_rt.block_on(async {
        server_endpoint.wait_idle().await;
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

/// Accept each incoming connection and for every bidirectional stream echo the first
/// `request_bytes` bytes back, then end the stream. The server performs the minimum amount
/// of per-stream work so that the datagram ingress path on the endpoint's main UDP socket
/// (which is exactly what this branch restructures) dominates the server-side wall time.
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
            loop {
                let (mut send, mut recv) = match conn.accept_bi().await {
                    Ok(pair) => pair,
                    Err(_) => return,
                };
                tokio::spawn(async move {
                    let mut buf = [0u8; 4096];
                    loop {
                        let n = match recv.read(&mut buf).await {
                            Ok(Some(n)) => n,
                            Ok(None) => break,
                            Err(_) => return,
                        };
                        if send.write_all(&buf[..n]).await.is_err() {
                            return;
                        }
                    }
                    let _ = send.finish();
                });
            }
        });
    }
}

struct WorkloadStats {
    wall: Duration,
    requests: u64,
    bytes: u64,
    errors: u64,
    /// Sorted ascending per-request round-trip latency samples.
    latencies: Vec<Duration>,
}

async fn run_workload(
    server_addr: SocketAddr,
    client_config: quinn::ClientConfig,
    opt: Opt,
) -> Result<WorkloadStats> {
    // Each connection has its own client endpoint so the client-side UDP socket / driver
    // does not become a bottleneck (the server has exactly one endpoint, which is the
    // subject under test).
    let client_endpoint =
        quinn::Endpoint::client(SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 0))
            .context("create client endpoint")?;

    // Establish N long-lived connections up front.
    let mut connections = Vec::with_capacity(opt.connections);
    for _ in 0..opt.connections {
        let c = client_endpoint
            .connect_with(client_config.clone(), server_addr, "localhost")
            .context("start connect")?
            .await
            .context("finish connect")?;
        connections.push(c);
    }

    let stop = Arc::new(AtomicBool::new(false));
    let request_counter = Arc::new(AtomicU64::new(0));
    let byte_counter = Arc::new(AtomicU64::new(0));
    let error_counter = Arc::new(AtomicU64::new(0));

    let worker_count = opt.connections * opt.workers_per_conn;
    let start_barrier = Arc::new(Barrier::new(worker_count + 1));

    let mut join = JoinSet::new();
    for conn in &connections {
        for _ in 0..opt.workers_per_conn {
            let conn = conn.clone();
            let stop = stop.clone();
            let requests = request_counter.clone();
            let bytes = byte_counter.clone();
            let errors = error_counter.clone();
            let barrier = start_barrier.clone();
            let request_bytes = opt.request_bytes;
            join.spawn(async move {
                // Warm up: do a handful of iterations before the barrier so the TLS/QUIC
                // state settles and flow-control windows are opened.
                let payload = vec![0xABu8; request_bytes];
                let mut scratch = vec![0u8; request_bytes];
                for _ in 0..4 {
                    let _ =
                        round_trip(&conn, &payload, &mut scratch, request_bytes).await;
                }
                barrier.wait().await;

                let mut local_latencies = Vec::with_capacity(1024);
                while !stop.load(Ordering::Relaxed) {
                    let t0 = Instant::now();
                    match round_trip(&conn, &payload, &mut scratch, request_bytes).await {
                        Ok(n) => {
                            local_latencies.push(t0.elapsed());
                            requests.fetch_add(1, Ordering::Relaxed);
                            bytes.fetch_add(n as u64, Ordering::Relaxed);
                        }
                        Err(_) => {
                            errors.fetch_add(1, Ordering::Relaxed);
                            break;
                        }
                    }
                }
                local_latencies
            });
        }
    }

    // Warmup is separate from the measured region: we rely on the per-worker warmup
    // iterations above plus a bounded warmup sleep. The fixed wait guarantees all workers
    // have crossed the barrier before measurement starts.
    start_barrier.wait().await;
    if opt.warmup_secs > 0.0 {
        tokio::time::sleep(Duration::from_secs_f64(opt.warmup_secs)).await;
    }

    // Reset counters so warmup activity doesn't pollute the measurement.
    request_counter.store(0, Ordering::Relaxed);
    byte_counter.store(0, Ordering::Relaxed);
    error_counter.store(0, Ordering::Relaxed);

    let wall_start = Instant::now();
    tokio::time::sleep(Duration::from_secs_f64(opt.duration_secs)).await;
    stop.store(true, Ordering::Relaxed);
    let wall = wall_start.elapsed();

    // Drain workers and aggregate latencies.
    let mut all_latencies = Vec::new();
    while let Some(res) = join.join_next().await {
        if let Ok(mut v) = res {
            all_latencies.append(&mut v);
        }
    }

    // Drop client connections so the server's wait_idle() returns.
    drop(connections);
    drop(client_endpoint);

    all_latencies.sort_unstable();
    Ok(WorkloadStats {
        wall,
        requests: request_counter.load(Ordering::Relaxed),
        bytes: byte_counter.load(Ordering::Relaxed),
        errors: error_counter.load(Ordering::Relaxed),
        latencies: all_latencies,
    })
}

/// Open a fresh bidirectional stream, write `len` bytes, read the echo, finish.
///
/// We use a fresh stream per iteration so the driver sees new STREAM/stream-open frames
/// each round, keeping the per-datagram ingress path exercised rather than steady-state
/// flow control on a single stream.
async fn round_trip(
    conn: &quinn::Connection,
    payload: &[u8],
    scratch: &mut [u8],
    len: usize,
) -> Result<usize> {
    let (mut send, mut recv) = conn.open_bi().await?;
    send.write_all(&payload[..len]).await?;
    send.finish()?;

    // Read until the peer signals EOF (finish).
    let mut total = 0usize;
    while total < len {
        match recv.read(&mut scratch[total..len]).await? {
            Some(n) => total += n,
            None => break,
        }
    }
    // Drain any trailing bytes / EOF marker.
    let _ = recv.read(&mut scratch[0..0]).await;
    Ok(total)
}

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn report(opt: &Opt, stats: &WorkloadStats) {
    let measured = stats.requests;
    let errors = stats.errors;
    let throughput = measured as f64 / stats.wall.as_secs_f64();
    let bytes_per_sec = stats.bytes as f64 / stats.wall.as_secs_f64();
    let p50 = percentile(&stats.latencies, 0.50);
    let p90 = percentile(&stats.latencies, 0.90);
    let p95 = percentile(&stats.latencies, 0.95);
    let p99 = percentile(&stats.latencies, 0.99);
    let p999 = percentile(&stats.latencies, 0.999);
    let max = stats.latencies.last().copied().unwrap_or_default();
    let mean = if stats.latencies.is_empty() {
        Duration::ZERO
    } else {
        Duration::from_nanos(
            (stats
                .latencies
                .iter()
                .map(|d| d.as_nanos())
                .sum::<u128>()
                / stats.latencies.len() as u128) as u64,
        )
    };

    println!("\n=== QUIC server-side ingress benchmark ===");
    println!("configuration:");
    println!("  --connections         {}", opt.connections);
    println!("  --workers-per-conn    {}", opt.workers_per_conn);
    println!(
        "  --duration-secs       {:.3} (warmup {:.3})",
        opt.duration_secs, opt.warmup_secs
    );
    println!("  --request-bytes       {}", opt.request_bytes);
    println!(
        "  --server-threads      {} ({} active)",
        opt.server_threads,
        if opt.server_threads == 0 {
            "auto".to_string()
        } else {
            opt.server_threads.to_string()
        }
    );
    println!(
        "  --client-threads      {} ({} active)",
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
        "  completed:   {} requests ({} bytes, errors: {})",
        measured, stats.bytes, errors
    );
    println!("  wall:        {:.3?}", stats.wall);
    println!("  throughput:  {:.1} req/s ({:.2} MiB/s)", throughput, bytes_per_sec / (1024.0 * 1024.0));
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
            "CSV,{label},{connections},{workers},{duration},{req_bytes},{server_threads},{client_threads},{measured},{errors},{wall_ms:.3},{throughput:.1},{bps:.1},{mean_us},{p50_us},{p90_us},{p95_us},{p99_us},{p999_us},{max_us}",
            connections = opt.connections,
            workers = opt.workers_per_conn,
            duration = opt.duration_secs,
            req_bytes = opt.request_bytes,
            server_threads = opt.server_threads,
            client_threads = opt.client_threads,
            wall_ms = stats.wall.as_secs_f64() * 1000.0,
            bps = bytes_per_sec,
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
