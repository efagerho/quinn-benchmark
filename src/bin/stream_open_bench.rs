//! QUIC server-side ingress benchmark variant: stream-open dominated.
//!
//! Each client iteration opens a *fresh unidirectional* stream, writes a
//! fixed 16-byte payload, and finishes the stream. No response, no echo: the
//! server accepts the stream, reads to EOF, and drops it. This isolates the
//! "new-stream + tiny-payload + FIN" hot path on the ingress side, where the
//! per-datagram overhead of NEW_STREAM accounting, frame dispatch, and
//! MAX_STREAMS flow-control updates dominates rather than bulk read/write
//! throughput.
//!
//! Compared to `ingress_bench`, which also opens a fresh stream per iteration
//! but runs a bidirectional echo (adding a full server->client response and
//! a per-iteration client-side read), this benchmark removes the response
//! path entirely so the measurement is a one-way stream-open rate stressed
//! by stream-flow-control (MAX_STREAMS) credit issuance rather than
//! request/response round-trip latency.
//!
//! Usage:
//!
//! ```text
//! cargo run --release --bin stream_open_bench -- \
//!     --connections 64 --workers-per-conn 4 --duration-secs 10 \
//!     --server-threads 8 --client-threads 4
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
use quinn::{TokioRuntime, default_runtime};
use quinn_benchmark::{
    build_client_config, build_server_config, ensure_crypto_provider, generate_self_signed,
};
use socket2::{Domain, Protocol, Socket, Type};
use tokio::{
    runtime::{Builder, Runtime},
    sync::Barrier,
    task::JoinSet,
};

/// Fixed payload written by every client iteration.
///
/// Contents are arbitrary: the sole purpose is to carry 16 bytes of STREAM
/// frame payload so the server-side decode/assembler path is exercised.
const PAYLOAD: [u8; 16] = [
    0x51, 0x55, 0x49, 0x4e, // "QUIN"
    0x4e, 0x2d, 0x4f, 0x50, // "N-OP"
    0x45, 0x4e, 0x2d, 0x42, // "EN-B"
    0x45, 0x4e, 0x43, 0x48, // "ENCH"
];

#[derive(Parser, Debug, Clone)]
#[clap(
    name = "stream_open_bench",
    about = "Measure QUIC server-side ingress throughput for open-uni + 16B + finish."
)]
struct Opt {
    /// Number of long-lived client connections to the server.
    #[clap(long, default_value_t = 64)]
    connections: usize,

    /// Concurrent stream-open loops per connection (each opens its own uni
    /// stream per iteration).
    #[clap(long, default_value_t = 4)]
    workers_per_conn: usize,

    /// Duration of the measured phase, in seconds.
    #[clap(long, default_value_t = 5.0)]
    duration_secs: f64,

    /// Warmup duration before measurement, in seconds.
    #[clap(long, default_value_t = 1.0)]
    warmup_secs: f64,

    /// Worker threads for the server tokio runtime. 0 = CPU count.
    #[clap(long, default_value_t = 0)]
    server_threads: usize,

    /// Worker threads for the client tokio runtime. 0 = CPU count.
    #[clap(long, default_value_t = 0)]
    client_threads: usize,

    /// Number of server-side UDP sockets bound with `SO_REUSEPORT` to the same port.
    #[clap(long, default_value_t = 1)]
    server_shards: usize,

    /// Number of distinct client-side endpoints (each with its own source port)
    /// used to open the long-lived connections. `0` means "match
    /// `--server-shards`".
    #[clap(long, default_value_t = 0)]
    client_endpoints: usize,

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
    if opt.server_shards == 0 {
        anyhow::bail!("--server-shards must be >= 1");
    }

    let (cert, key) = generate_self_signed();
    let server_config = build_server_config(cert.clone(), key)?;
    let client_config = build_client_config(cert)?;

    let server_rt = build_rt("bench-server", opt.server_threads)?;
    let client_rt = build_rt("bench-client", opt.client_threads)?;

    let shutdown = Arc::new(AtomicBool::new(false));
    let server_recv_count = Arc::new(AtomicU64::new(0));
    let (server_addr, server_endpoints) = {
        let _guard = server_rt.enter();
        build_server_endpoints(
            SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 0),
            opt.server_shards,
            server_config,
        )?
    };
    let mut server_tasks = Vec::with_capacity(server_endpoints.len());
    for ep in &server_endpoints {
        server_tasks.push(server_rt.spawn(run_server(
            ep.clone(),
            shutdown.clone(),
            server_recv_count.clone(),
        )));
    }

    let client_endpoints = if opt.client_endpoints == 0 {
        opt.server_shards
    } else {
        opt.client_endpoints
    };

    let stats = client_rt
        .block_on(run_workload(
            server_addr,
            client_config,
            client_endpoints,
            opt.clone(),
            server_recv_count.clone(),
        ))
        .context("workload failed")?;

    shutdown.store(true, Ordering::Relaxed);
    for ep in &server_endpoints {
        ep.close(0u32.into(), b"bench done");
    }
    server_rt.block_on(async {
        for ep in &server_endpoints {
            ep.wait_idle().await;
        }
        for t in server_tasks {
            t.abort();
            let _ = t.await;
        }
    });

    report(&opt, client_endpoints, &stats);
    Ok(())
}

/// Same sharded-server builder as `ingress_bench`.
fn build_server_endpoints(
    addr: SocketAddr,
    shards: usize,
    server_config: quinn::ServerConfig,
) -> Result<(SocketAddr, Vec<quinn::Endpoint>)> {
    assert!(shards >= 1);
    let runtime = default_runtime()
        .ok_or_else(|| anyhow::anyhow!("no default tokio runtime found for quinn"))?;
    let mut endpoints = Vec::with_capacity(shards);
    let mut bound_addr: Option<SocketAddr> = None;
    for i in 0..shards {
        let bind_addr = bound_addr.unwrap_or(addr);
        let socket = build_reuseport_socket(bind_addr, shards > 1)
            .with_context(|| format!("build server socket shard {i} bound at {bind_addr}"))?;
        let local = socket
            .local_addr()
            .context("read local_addr of server socket")?;
        if bound_addr.is_none() {
            bound_addr = Some(local);
        }
        let endpoint = quinn::Endpoint::new(
            quinn::EndpointConfig::default(),
            Some(server_config.clone()),
            socket,
            runtime.clone(),
        )
        .context("construct quinn::Endpoint from pre-bound socket")?;
        endpoints.push(endpoint);
    }
    let bound_addr = bound_addr.expect("at least one shard was constructed");
    Ok((bound_addr, endpoints))
}

fn build_reuseport_socket(addr: SocketAddr, reuse_port: bool) -> Result<std::net::UdpSocket> {
    let socket = Socket::new(Domain::for_address(addr), Type::DGRAM, Some(Protocol::UDP))
        .context("create UDP socket")?;
    if addr.is_ipv6() {
        let _ = socket.set_only_v6(false);
    }
    if reuse_port {
        #[cfg(unix)]
        {
            socket.set_reuse_address(true).context("SO_REUSEADDR")?;
            socket.set_reuse_port(true).context("SO_REUSEPORT")?;
        }
    }
    socket.bind(&addr.into()).context("bind UDP socket")?;
    Ok(socket.into())
}

fn build_client_endpoint() -> Result<quinn::Endpoint> {
    let socket = Socket::new(Domain::IPV6, Type::DGRAM, Some(Protocol::UDP))
        .context("create client UDP socket")?;
    let _ = socket.set_only_v6(false);
    let bind_addr = SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 0);
    socket
        .bind(&bind_addr.into())
        .context("bind client UDP socket")?;
    let socket: std::net::UdpSocket = socket.into();
    let endpoint = quinn::Endpoint::new(
        quinn::EndpointConfig::default(),
        None,
        socket,
        Arc::new(TokioRuntime),
    )
    .context("construct client quinn::Endpoint")?;
    Ok(endpoint)
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

/// Accept every incoming unidirectional stream and read it to EOF, dropping
/// the data. Each completed stream bumps `recv_count`. No response is sent.
async fn run_server(
    endpoint: quinn::Endpoint,
    shutdown: Arc<AtomicBool>,
    recv_count: Arc<AtomicU64>,
) {
    loop {
        let incoming = match endpoint.accept().await {
            Some(i) => i,
            None => return,
        };
        if shutdown.load(Ordering::Relaxed) {
            incoming.ignore();
            continue;
        }
        let recv_count = recv_count.clone();
        tokio::spawn(async move {
            let connecting = match incoming.accept() {
                Ok(c) => c,
                Err(_) => return,
            };
            let Ok(conn) = connecting.await else {
                return;
            };
            loop {
                let mut recv = match conn.accept_uni().await {
                    Ok(s) => s,
                    Err(_) => return,
                };
                let recv_count = recv_count.clone();
                tokio::spawn(async move {
                    // Payload is tiny (16 B expected) but we still loop so the
                    // server side correctly drains any future path that
                    // fragments it across multiple STREAM frames.
                    let mut buf = [0u8; 64];
                    loop {
                        match recv.read(&mut buf).await {
                            Ok(Some(_)) => continue,
                            Ok(None) => break,
                            Err(_) => return,
                        }
                    }
                    recv_count.fetch_add(1, Ordering::Relaxed);
                });
            }
        });
    }
}

struct WorkloadStats {
    wall: Duration,
    client_sends: u64,
    server_recvs: u64,
    bytes: u64,
    errors: u64,
    /// Sorted ascending per-iteration client-side latency samples (open_uni +
    /// write_all + finish).
    latencies: Vec<Duration>,
}

async fn run_workload(
    server_addr: SocketAddr,
    client_config: quinn::ClientConfig,
    client_endpoint_count: usize,
    opt: Opt,
    server_recv_count: Arc<AtomicU64>,
) -> Result<WorkloadStats> {
    let n_client_eps = client_endpoint_count.max(1);
    let mut client_endpoints = Vec::with_capacity(n_client_eps);
    for _ in 0..n_client_eps {
        client_endpoints.push(build_client_endpoint().context("create client endpoint")?);
    }

    let mut connections = Vec::with_capacity(opt.connections);
    for i in 0..opt.connections {
        let ep = &client_endpoints[i % n_client_eps];
        let c = ep
            .connect_with(client_config.clone(), server_addr, "localhost")
            .context("start connect")?
            .await
            .context("finish connect")?;
        connections.push(c);
    }

    let stop = Arc::new(AtomicBool::new(false));
    let send_counter = Arc::new(AtomicU64::new(0));
    let byte_counter = Arc::new(AtomicU64::new(0));
    let error_counter = Arc::new(AtomicU64::new(0));

    let worker_count = opt.connections * opt.workers_per_conn;
    let start_barrier = Arc::new(Barrier::new(worker_count + 1));

    let mut join = JoinSet::new();
    for conn in &connections {
        for _ in 0..opt.workers_per_conn {
            let conn = conn.clone();
            let stop = stop.clone();
            let sends = send_counter.clone();
            let bytes = byte_counter.clone();
            let errors = error_counter.clone();
            let barrier = start_barrier.clone();
            join.spawn(async move {
                for _ in 0..4 {
                    let _ = fire_and_forget(&conn).await;
                }
                barrier.wait().await;

                let mut local_latencies = Vec::with_capacity(1024);
                while !stop.load(Ordering::Relaxed) {
                    let t0 = Instant::now();
                    match fire_and_forget(&conn).await {
                        Ok(n) => {
                            local_latencies.push(t0.elapsed());
                            sends.fetch_add(1, Ordering::Relaxed);
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

    start_barrier.wait().await;
    if opt.warmup_secs > 0.0 {
        tokio::time::sleep(Duration::from_secs_f64(opt.warmup_secs)).await;
    }

    send_counter.store(0, Ordering::Relaxed);
    byte_counter.store(0, Ordering::Relaxed);
    error_counter.store(0, Ordering::Relaxed);
    // Snapshot the server-side receive count at measurement start so we can
    // report a true "server completed streams during the measured window"
    // number that excludes warmup traffic.
    let server_recvs_start = server_recv_count.load(Ordering::Relaxed);

    let wall_start = Instant::now();
    tokio::time::sleep(Duration::from_secs_f64(opt.duration_secs)).await;
    stop.store(true, Ordering::Relaxed);
    let wall = wall_start.elapsed();

    let mut all_latencies = Vec::new();
    while let Some(res) = join.join_next().await {
        if let Ok(mut v) = res {
            all_latencies.append(&mut v);
        }
    }

    // Final server-side snapshot. We take it *after* the client join-set has
    // drained, so any still-in-flight streams finished by the client before
    // it observed `stop` are counted. Briefly yield to let the server's
    // accept_uni tasks drain.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let server_recvs_end = server_recv_count.load(Ordering::Relaxed);

    drop(connections);
    drop(client_endpoints);

    all_latencies.sort_unstable();
    Ok(WorkloadStats {
        wall,
        client_sends: send_counter.load(Ordering::Relaxed),
        server_recvs: server_recvs_end.saturating_sub(server_recvs_start),
        bytes: byte_counter.load(Ordering::Relaxed),
        errors: error_counter.load(Ordering::Relaxed),
        latencies: all_latencies,
    })
}

/// Open a fresh unidirectional stream, write the fixed 16-byte payload, and
/// finish. No response is read -- the server drains the stream and drops it.
async fn fire_and_forget(conn: &quinn::Connection) -> Result<usize> {
    let mut send = conn.open_uni().await?;
    send.write_all(&PAYLOAD).await?;
    send.finish()?;
    Ok(PAYLOAD.len())
}

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn report(opt: &Opt, client_endpoint_count: usize, stats: &WorkloadStats) {
    let client_sends = stats.client_sends;
    let server_recvs = stats.server_recvs;
    let errors = stats.errors;
    // Primary "throughput" = server-observed completed streams / wall. This is
    // the ingress-side rate we care about. We also print the client-side send
    // rate for sanity (they should match closely at steady state).
    let server_throughput = server_recvs as f64 / stats.wall.as_secs_f64();
    let client_throughput = client_sends as f64 / stats.wall.as_secs_f64();
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

    println!("\n=== QUIC stream-open (uni, 16B, finish) bench ===");
    println!("configuration:");
    println!("  --connections         {}", opt.connections);
    println!("  --workers-per-conn    {}", opt.workers_per_conn);
    println!(
        "  --duration-secs       {:.3} (warmup {:.3})",
        opt.duration_secs, opt.warmup_secs
    );
    println!("  payload               {} bytes (fixed)", PAYLOAD.len());
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
    println!(
        "  --server-shards       {} (SO_REUSEPORT sockets on one port)",
        opt.server_shards
    );
    println!(
        "  --client-endpoints    {} (distinct source ports)",
        client_endpoint_count
    );
    println!();
    println!("results:");
    println!(
        "  client sent:        {} streams ({} bytes, errors: {})",
        client_sends, stats.bytes, errors
    );
    println!(
        "  server received:    {} streams",
        server_recvs
    );
    println!("  wall:               {:.3?}", stats.wall);
    println!(
        "  server throughput:  {:.1} streams/s ({:.2} MiB/s payload)",
        server_throughput,
        bytes_per_sec / (1024.0 * 1024.0)
    );
    println!(
        "  client throughput:  {:.1} streams/s (send-side)",
        client_throughput
    );
    println!("  client-side latency (open_uni + write + finish):");
    println!("    mean       {:?}", mean);
    println!("    p50        {:?}", p50);
    println!("    p90        {:?}", p90);
    println!("    p95        {:?}", p95);
    println!("    p99        {:?}", p99);
    println!("    p99.9      {:?}", p999);
    println!("    max        {:?}", max);

    if opt.csv {
        let label = opt.label.as_deref().unwrap_or("-");
        // Column layout:
        //   1 CSV, 2 label, 3 connections, 4 workers, 5 duration, 6 payload_bytes,
        //   7 server_threads, 8 client_threads, 9 client_sends, 10 server_recvs,
        //   11 errors, 12 wall_ms, 13 server_throughput, 14 client_throughput,
        //   15 bps, 16 mean_us, 17 p50_us, 18 p90_us, 19 p95_us, 20 p99_us,
        //   21 p999_us, 22 max_us, 23 server_shards, 24 client_endpoints
        println!(
            "CSV,{label},{connections},{workers},{duration},{payload_bytes},{server_threads},{client_threads},{client_sends},{server_recvs},{errors},{wall_ms:.3},{server_throughput:.1},{client_throughput:.1},{bps:.1},{mean_us},{p50_us},{p90_us},{p95_us},{p99_us},{p999_us},{max_us},{server_shards},{client_endpoints}",
            connections = opt.connections,
            workers = opt.workers_per_conn,
            duration = opt.duration_secs,
            payload_bytes = PAYLOAD.len(),
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
            server_shards = opt.server_shards,
            client_endpoints = client_endpoint_count,
        );
    }
}
