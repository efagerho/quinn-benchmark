//! QUIC contention benchmark: throughput vs. throughput-under-churn.
//!
//! In-process, self-contained harness that measures how much a concurrent
//! connection-churn workload degrades the throughput of a small number of
//! long-lived upload connections. Designed to surface server-side
//! `EndpointDriver` / state-mutex contention between accept work and the
//! datagram hot path.
//!
//! The workload runs in two phases against the same server endpoint:
//!
//! 1. **Baseline:** `connections` client connections each open
//!    `streams_per_connection` concurrent uni streams and upload
//!    `stream_size` bytes per stream in a tight loop for `duration_secs`. The
//!    server drains every uni stream. We record the aggregate MiB/s.
//! 2. **Mixed:** we spin up a connection-churn driver that opens + closes
//!    connections as fast as the server will let it, using `churn_workers`
//!    parallel workers. After a `warmup_secs` pause we re-run the same
//!    throughput workload concurrently with the churn. We record MiB/s
//!    again, the achieved churn rate, and the resulting degradation
//!    percentage.
//!
//! The throughput client, the churn client and the server each get their own
//! Tokio runtime so that cross-runtime scheduling does not make the churn
//! phase look latency-bound when it is really CPU-bound, and does not hide
//! server-side lock contention behind a saturated client send loop.
//!
//! Usage:
//!
//! ```text
//! cargo run --release --bin contention_bench -- \
//!     --connections 8 --streams-per-connection 4 --stream-size 128M \
//!     --duration-secs 15 --warmup-secs 3 \
//!     --churn-workers 64 \
//!     --server-threads 14 --client-threads 4 --churn-threads 4 \
//!     --csv --label main
//! ```

use std::{
    net::{IpAddr, Ipv6Addr, SocketAddr},
    num::ParseIntError,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use bytes::Bytes;
use clap::Parser;
use quinn_benchmark::{
    build_client_config, build_server_config, ensure_crypto_provider, generate_self_signed,
};
use tokio::{
    runtime::{Builder, Handle, Runtime},
    task::JoinSet,
};

#[derive(Parser, Debug, Clone)]
#[clap(
    name = "contention_bench",
    about = "Measure QUIC throughput in isolation vs. under concurrent connection churn."
)]
struct Opt {
    /// Number of long-lived throughput connections.
    #[clap(long, default_value_t = 4)]
    connections: usize,

    /// Concurrent uni streams opened per throughput connection.
    #[clap(long, default_value_t = 4)]
    streams_per_connection: usize,

    /// Bytes uploaded per stream attempt. Accepts SI suffixes (k/M/G/T).
    ///
    /// Keep this small enough that the throughput phase is *not* capped by
    /// per-stream client-side AEAD cost, otherwise there is no headroom for
    /// server-side contention to show up in the "mixed" number.
    #[clap(long, default_value = "128M", value_parser = parse_byte_size)]
    stream_size: u64,

    /// Duration of each measured throughput phase, in seconds.
    #[clap(long, default_value_t = 10.0)]
    duration_secs: f64,

    /// Sleep between starting churn and starting the mixed-phase throughput
    /// workload, in seconds.
    #[clap(long, default_value_t = 2.0)]
    warmup_secs: f64,

    /// Legacy soft cap on the aggregate new-connection rate. Retained as a
    /// CLI / CSV field for backwards compatibility with older driver scripts
    /// but **not** enforced: workers always loop open/close as fast as the
    /// server accepts them. The final `churn conn/s` column reports the
    /// actually-achieved rate.
    #[clap(long, default_value_t = 0)]
    churn_rate: u64,

    /// Number of parallel churn workers. Each worker serially opens + closes
    /// connections in a tight loop, so this is the concurrent-in-flight
    /// handshake ceiling.
    #[clap(long, default_value_t = 64)]
    churn_workers: usize,

    /// Initial MTU used by the churn client endpoint.
    #[clap(long, default_value_t = 1200)]
    churn_initial_mtu: u16,

    /// Initial MTU used by the throughput server + client.
    #[clap(long, default_value_t = 1200)]
    initial_mtu: u16,

    /// Max concurrent uni streams advertised by the endpoint.
    #[clap(long, default_value_t = 131_072)]
    max_concurrent_uni_streams: u64,

    /// Worker threads for the server tokio runtime. 0 = tokio default.
    #[clap(long, default_value_t = 0)]
    server_threads: usize,

    /// Worker threads for the throughput-client tokio runtime. 0 = tokio
    /// default.
    #[clap(long, default_value_t = 0)]
    client_threads: usize,

    /// Worker threads for the dedicated churn-client tokio runtime. 0 =
    /// tokio default. Kept separate from `--client-threads` so the churn
    /// workers do not contend with the throughput send loop for scheduler
    /// time.
    #[clap(long, default_value_t = 0)]
    churn_threads: usize,

    /// Print a machine-readable single-line summary after the human-readable
    /// report.
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
    if opt.connections == 0 || opt.streams_per_connection == 0 {
        anyhow::bail!("--connections and --streams-per-connection must be > 0");
    }
    if opt.duration_secs <= 0.0 {
        anyhow::bail!("--duration-secs must be > 0");
    }
    if opt.stream_size == 0 {
        anyhow::bail!("--stream-size must be > 0");
    }

    let (cert, key) = generate_self_signed();
    let mut server_config = build_server_config(cert.clone(), key)?;
    server_config.transport = Arc::new(contention_transport_config(
        opt.initial_mtu,
        opt.max_concurrent_uni_streams,
    ));

    let make_client_config = |initial_mtu: u16| -> Result<quinn::ClientConfig> {
        let mut cfg = build_client_config(cert.clone())?;
        cfg.transport_config(Arc::new(contention_transport_config(
            initial_mtu,
            opt.max_concurrent_uni_streams,
        )));
        Ok(cfg)
    };

    // Three runtimes: server, throughput-client, churn-client. Isolating the
    // churn-client runtime is the whole point of the benchmark — otherwise
    // the throughput send-loop starves the churn connect tasks and we end up
    // measuring scheduler behaviour rather than server-side contention.
    let server_rt = build_rt("cont-server", opt.server_threads)?;
    let throughput_rt = build_rt("cont-throughput", opt.client_threads)?;
    let churn_rt = build_rt("cont-churn", opt.churn_threads)?;

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

    let throughput_client_config = make_client_config(opt.initial_mtu)?;
    let churn_client_config = make_client_config(opt.churn_initial_mtu)?;

    let stats = run_benchmark(
        &opt,
        server_addr,
        throughput_client_config,
        churn_client_config,
        throughput_rt.handle(),
        churn_rt.handle(),
    )?;

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

/// Large-window transport config cloned from the upstream contention bench.
fn contention_transport_config(initial_mtu: u16, max_uni: u64) -> quinn::TransportConfig {
    let mut cfg = quinn::TransportConfig::default();
    cfg.initial_mtu(initial_mtu);
    cfg.max_concurrent_uni_streams(max_uni.try_into().unwrap());
    cfg.stream_receive_window((64_u32 * 1024 * 1024).into());
    cfg.receive_window((256_u32 * 1024 * 1024).into());
    cfg.send_window(256 * 1024 * 1024);

    let mut acks = quinn::AckFrequencyConfig::default();
    acks.ack_eliciting_threshold(10u32.into());
    cfg.ack_frequency_config(Some(acks));

    cfg
}

/// Accept every connection and drain every uni stream the peer opens. The
/// server does the bare minimum so the measurement reflects transport
/// throughput, not per-stream application logic.
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
            let connection = match incoming.await {
                Ok(c) => c,
                Err(_) => return,
            };
            while let Ok(mut stream) = connection.accept_uni().await {
                tokio::spawn(async move {
                    let mut buf = [0u8; 64 * 1024];
                    loop {
                        match stream.read(&mut buf).await {
                            Ok(Some(_)) => {}
                            Ok(None) | Err(_) => return,
                        }
                    }
                });
            }
        });
    }
}

struct Stats {
    baseline: PhaseStats,
    mixed: PhaseStats,
    churn_completed: u64,
    churn_wall: Duration,
}

#[derive(Default, Clone, Copy)]
struct PhaseStats {
    bytes: u64,
    streams: u64,
    wall: Duration,
}

impl PhaseStats {
    fn mib_per_sec(&self) -> f64 {
        if self.wall.is_zero() {
            0.0
        } else {
            self.bytes as f64 / self.wall.as_secs_f64() / (1024.0 * 1024.0)
        }
    }
}

/// Orchestrator. Drives the baseline and mixed phases on the throughput
/// runtime and the churn workers on a dedicated runtime, coordinating
/// start / stop through plain atomics so there's no cross-runtime async
/// plumbing needed.
fn run_benchmark(
    opt: &Opt,
    server_addr: SocketAddr,
    throughput_cfg: quinn::ClientConfig,
    churn_cfg: quinn::ClientConfig,
    throughput_rt: &Handle,
    churn_rt: &Handle,
) -> Result<Stats> {
    // Baseline (no churn running at all).
    let baseline = throughput_rt
        .block_on(run_throughput_phase(
            server_addr,
            throughput_cfg.clone(),
            opt,
        ))
        .context("baseline phase failed")?;

    // Spawn churn on its own runtime. The `ChurnHandle` holds a reference to
    // the churn-client endpoint + the JoinSet so we can shut them down
    // cleanly once the mixed phase is over.
    let stop = Arc::new(AtomicBool::new(false));
    let churn_counter = Arc::new(AtomicU64::new(0));
    let churn = if opt.churn_workers > 0 {
        Some(spawn_churn_driver(
            churn_rt,
            server_addr,
            churn_cfg,
            opt.churn_workers,
            stop.clone(),
            churn_counter.clone(),
        )?)
    } else {
        None
    };

    // Warmup: let the churn workers reach steady state before we start
    // measuring mixed throughput. Synchronous sleep on the driving thread is
    // fine — neither of the two client runtimes is blocked by it.
    if opt.warmup_secs > 0.0 {
        thread::sleep(Duration::from_secs_f64(opt.warmup_secs));
    }

    // Mixed: reset the churn counter so `achieved churn` reflects the
    // measurement window, then run the same throughput workload concurrently
    // with the still-running churn driver.
    churn_counter.store(0, Ordering::Relaxed);
    let churn_phase_start = Instant::now();
    let mixed = throughput_rt
        .block_on(run_throughput_phase(server_addr, throughput_cfg, opt))
        .context("mixed phase failed")?;
    let churn_wall = churn_phase_start.elapsed();
    stop.store(true, Ordering::Relaxed);
    let churn_completed = churn_counter.load(Ordering::Relaxed);

    if let Some(handle) = churn {
        churn_rt.block_on(handle.shutdown());
    }

    Ok(Stats {
        baseline,
        mixed,
        churn_completed,
        churn_wall,
    })
}

async fn run_throughput_phase(
    server_addr: SocketAddr,
    client_cfg: quinn::ClientConfig,
    opt: &Opt,
) -> Result<PhaseStats> {
    let client_endpoint =
        quinn::Endpoint::client(SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 0))
            .context("create throughput client endpoint")?;

    let mut connections = Vec::with_capacity(opt.connections);
    for _ in 0..opt.connections {
        let conn = client_endpoint
            .connect_with(client_cfg.clone(), server_addr, "localhost")
            .context("start throughput connect")?
            .await
            .context("finish throughput connect")?;
        connections.push(conn);
    }

    let stop = Arc::new(AtomicBool::new(false));
    let bytes = Arc::new(AtomicU64::new(0));
    let streams = Arc::new(AtomicU64::new(0));
    let barrier = Arc::new(tokio::sync::Barrier::new(
        opt.connections * opt.streams_per_connection + 1,
    ));

    let mut join = JoinSet::new();
    for conn in &connections {
        for _ in 0..opt.streams_per_connection {
            let conn = conn.clone();
            let stop = stop.clone();
            let bytes = bytes.clone();
            let streams = streams.clone();
            let barrier = barrier.clone();
            let stream_size = opt.stream_size;
            join.spawn(async move {
                barrier.wait().await;
                while !stop.load(Ordering::Relaxed) {
                    let mut stream = match conn.open_uni().await {
                        Ok(s) => s,
                        Err(_) => return,
                    };
                    if send_fixed(&mut stream, stream_size).await.is_err() {
                        return;
                    }
                    bytes.fetch_add(stream_size, Ordering::Relaxed);
                    streams.fetch_add(1, Ordering::Relaxed);
                }
            });
        }
    }

    barrier.wait().await;
    let phase_start = Instant::now();
    tokio::time::sleep(Duration::from_secs_f64(opt.duration_secs)).await;
    stop.store(true, Ordering::Relaxed);
    let wall = phase_start.elapsed();

    // Drain upload tasks. Any `finish()` error will already have been
    // absorbed; the workers just exit.
    while let Some(res) = join.join_next().await {
        let _ = res;
    }

    for conn in &connections {
        conn.close(0u32.into(), b"phase done");
    }
    drop(connections);
    client_endpoint.wait_idle().await;
    drop(client_endpoint);

    Ok(PhaseStats {
        bytes: bytes.load(Ordering::Relaxed),
        streams: streams.load(Ordering::Relaxed),
        wall,
    })
}

/// Upload `len` bytes in 1 MiB chunks on a uni stream and finish it.
async fn send_fixed(stream: &mut quinn::SendStream, len: u64) -> Result<()> {
    const CHUNK: usize = 1024 * 1024;
    static PAYLOAD: [u8; CHUNK] = [0xABu8; CHUNK];
    let payload = Bytes::from_static(&PAYLOAD);

    let full = len / CHUNK as u64;
    let tail = (len % CHUNK as u64) as usize;
    for _ in 0..full {
        stream.write_chunk(payload.clone()).await?;
    }
    if tail != 0 {
        stream.write_chunk(payload.slice(0..tail)).await?;
    }
    stream.finish()?;
    let _ = stream.stopped().await;
    Ok(())
}

struct ChurnHandle {
    set: JoinSet<()>,
    endpoint: quinn::Endpoint,
}

impl ChurnHandle {
    async fn shutdown(mut self) {
        self.set.abort_all();
        while let Some(res) = self.set.join_next().await {
            let _ = res;
        }
        self.endpoint.close(0u32.into(), b"churn done");
        self.endpoint.wait_idle().await;
    }
}

/// Spawn `workers` churn tasks on the given `churn_rt` handle. Each worker
/// serially opens a connection, counts the successful handshake, and closes
/// the connection — no rate limiting. The aggregate achieved rate is
/// therefore whatever the server can accept under concurrent throughput
/// load, which is the quantity the contention benchmark is trying to
/// measure.
fn spawn_churn_driver(
    churn_rt: &Handle,
    server_addr: SocketAddr,
    client_cfg: quinn::ClientConfig,
    workers: usize,
    stop: Arc<AtomicBool>,
    counter: Arc<AtomicU64>,
) -> Result<ChurnHandle> {
    let endpoint = {
        // The endpoint must be constructed inside the churn runtime's
        // context so its driver task is bound to that runtime.
        let _g = churn_rt.enter();
        quinn::Endpoint::client(SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 0))
            .context("create churn client endpoint")?
    };

    let mut set = JoinSet::new();
    for _ in 0..workers {
        let endpoint = endpoint.clone();
        let config = client_cfg.clone();
        let stop = stop.clone();
        let counter = counter.clone();
        set.spawn_on(
            async move {
                while !stop.load(Ordering::Relaxed) {
                    match endpoint.connect_with(config.clone(), server_addr, "localhost") {
                        Ok(connecting) => {
                            if let Ok(conn) = connecting.await {
                                counter.fetch_add(1, Ordering::Relaxed);
                                conn.close(0u32.into(), b"churn");
                            }
                        }
                        Err(_) => {
                            // Typically "too many open connections" /
                            // endpoint exhaustion; yield a bit so we do not
                            // busy-loop on a transient failure.
                            tokio::task::yield_now().await;
                        }
                    }
                }
            },
            churn_rt,
        );
    }

    Ok(ChurnHandle { set, endpoint })
}

fn report(opt: &Opt, stats: &Stats) {
    let baseline_mib = stats.baseline.mib_per_sec();
    let mixed_mib = stats.mixed.mib_per_sec();
    let achieved_churn = if stats.churn_wall.is_zero() {
        0.0
    } else {
        stats.churn_completed as f64 / stats.churn_wall.as_secs_f64()
    };
    let degradation_pct = if baseline_mib > 0.0 {
        (1.0 - mixed_mib / baseline_mib) * 100.0
    } else {
        0.0
    };

    println!("\n=== QUIC contention benchmark ===");
    println!("configuration:");
    println!("  --connections                {}", opt.connections);
    println!(
        "  --streams-per-connection     {}",
        opt.streams_per_connection
    );
    println!("  --stream-size                {} B", opt.stream_size);
    println!(
        "  --duration-secs              {:.3} (warmup {:.3})",
        opt.duration_secs, opt.warmup_secs
    );
    println!(
        "  --churn-workers              {} (rate limit disabled, legacy --churn-rate = {})",
        opt.churn_workers, opt.churn_rate
    );
    println!(
        "  --server-threads             {} ({} active)",
        opt.server_threads,
        if opt.server_threads == 0 {
            "auto".to_string()
        } else {
            opt.server_threads.to_string()
        }
    );
    println!(
        "  --client-threads             {} ({} active)",
        opt.client_threads,
        if opt.client_threads == 0 {
            "auto".to_string()
        } else {
            opt.client_threads.to_string()
        }
    );
    println!(
        "  --churn-threads              {} ({} active)",
        opt.churn_threads,
        if opt.churn_threads == 0 {
            "auto".to_string()
        } else {
            opt.churn_threads.to_string()
        }
    );
    println!();
    println!("results:");
    println!(
        "  baseline: {:.2} MiB/s ({} bytes, {} streams, {:.3?})",
        baseline_mib, stats.baseline.bytes, stats.baseline.streams, stats.baseline.wall
    );
    println!(
        "  mixed:    {:.2} MiB/s ({} bytes, {} streams, {:.3?})",
        mixed_mib, stats.mixed.bytes, stats.mixed.streams, stats.mixed.wall
    );
    println!(
        "  churn:    {:.2} conn/s achieved ({} completed in {:.3?})",
        achieved_churn, stats.churn_completed, stats.churn_wall
    );
    println!("  degradation: {:.2}%", degradation_pct);

    if opt.csv {
        let label = opt.label.as_deref().unwrap_or("-");
        let duration_ms = stats.mixed.wall.as_secs_f64() * 1000.0;
        println!(
            "CSV,{label},{connections},{streams},{stream_bytes},{churn_rate},{churn_workers},{server_threads},{client_threads},{duration_ms:.3},{baseline_mib:.2},{mixed_mib:.2},{achieved_churn:.2},{degradation_pct:.2}",
            connections = opt.connections,
            streams = opt.streams_per_connection,
            stream_bytes = opt.stream_size,
            churn_rate = opt.churn_rate,
            churn_workers = opt.churn_workers,
            server_threads = opt.server_threads,
            client_threads = opt.client_threads,
        );
    }
}

fn parse_byte_size(s: &str) -> Result<u64, ParseIntError> {
    let s = s.trim();
    let multiplier: u64 = match s.chars().last() {
        Some('T') => 1024 * 1024 * 1024 * 1024,
        Some('G') => 1024 * 1024 * 1024,
        Some('M') => 1024 * 1024,
        Some('k') => 1024,
        _ => 1,
    };
    let num = match multiplier {
        1 => s,
        _ => &s[..s.len() - 1],
    };
    Ok(u64::from_str(num)? * multiplier)
}
