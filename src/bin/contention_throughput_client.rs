//! Standalone throughput client for the contention benchmark, ported from
//! `bench/src/bin/throughput_client.rs` on quinn's
//! `endpoint-lock-optimization-accept-split` branch.
//!
//! Spawns one OS thread per long-lived connection; each thread owns its own
//! current-thread tokio runtime and its own `quinn::Endpoint` so the client
//! side exercises a genuine multi-process-style fan-out rather than a single
//! shared runtime. Emits `key=value` lines on stdout that
//! `run_contention_compare.sh` parses with `awk -F=`.

use std::{
    net::SocketAddr,
    sync::{
        Arc, Barrier,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use clap::Parser;
use quinn_benchmark::{
    contention::{
        self, client_endpoint, configure_tracing_subscriber, insecure_client_config, rt,
        send_data_on_stream,
    },
    ensure_crypto_provider,
};

fn main() -> Result<()> {
    ensure_crypto_provider();
    configure_tracing_subscriber();

    let opt = Opt::parse();

    let barrier = Arc::new(Barrier::new(opt.connections.max(1)));
    let mut threads = Vec::with_capacity(opt.connections);

    for _ in 0..opt.connections {
        let barrier = barrier.clone();
        let opt = opt;
        threads.push(std::thread::spawn(move || run_connection(opt, barrier)));
    }

    let mut total_bytes = 0_u64;
    let mut total_streams = 0_u64;
    let mut elapsed = Duration::ZERO;
    for thread in threads {
        let result = thread.join().expect("throughput-client thread")?;
        total_bytes += result.bytes;
        total_streams += result.streams;
        elapsed = elapsed.max(result.elapsed);
    }

    let mib_per_s = if elapsed.is_zero() {
        0.0
    } else {
        total_bytes as f64 / elapsed.as_secs_f64() / 1024.0 / 1024.0
    };

    println!("duration_secs={}", opt.duration_secs);
    println!("elapsed_secs={:.3}", elapsed.as_secs_f64());
    println!("connections={}", opt.connections);
    println!("streams_per_connection={}", opt.streams_per_connection);
    println!("stream_size_bytes={}", opt.stream_size);
    if let Some(stream_runs) = opt.stream_runs {
        println!("stream_runs_per_connection={stream_runs}");
    }
    println!("bytes={}", total_bytes);
    println!("streams={}", total_streams);
    println!("throughput_mib_per_s={mib_per_s:.2}");

    Ok(())
}

fn run_connection(opt: Opt, barrier: Arc<Barrier>) -> Result<ConnectionResult> {
    let runtime = rt();
    let endpoint = client_endpoint(&runtime, opt.connect)?;
    let client_config = insecure_client_config(
        opt.initial_mtu,
        (opt.streams_per_connection.max(1) * 4) as u64,
    )?;
    runtime.block_on(async move {
        let connection = endpoint
            .connect_with(client_config, opt.connect, "localhost")
            .context("start throughput connect")?
            .await
            .context("unable to connect throughput client")?;

        // `std::sync::Barrier` blocks the OS thread, which is fine: each
        // client has its own current-thread runtime and its own OS thread,
        // so no executor is being starved.
        barrier.wait();

        let stream_budget = opt.stream_runs.map(|runs| Arc::new(AtomicU64::new(runs)));
        let deadline = Instant::now() + Duration::from_secs(opt.duration_secs);
        let start = Instant::now();
        let connection = Arc::new(connection);
        let bytes = Arc::new(AtomicU64::new(0));
        let streams = Arc::new(AtomicU64::new(0));

        let mut tasks = Vec::with_capacity(opt.streams_per_connection);
        for _ in 0..opt.streams_per_connection {
            let connection = connection.clone();
            let bytes = bytes.clone();
            let streams = streams.clone();
            let stream_budget = stream_budget.clone();
            let stream_size = opt.stream_size;
            tasks.push(tokio::spawn(async move {
                loop {
                    if let Some(stream_budget) = &stream_budget {
                        let next = stream_budget.fetch_update(
                            Ordering::Relaxed,
                            Ordering::Relaxed,
                            |remaining| remaining.checked_sub(1),
                        );
                        if next.is_err() {
                            break Ok::<(), anyhow::Error>(());
                        }
                    } else if Instant::now() >= deadline {
                        break Ok::<(), anyhow::Error>(());
                    }

                    let mut stream = connection
                        .open_uni()
                        .await
                        .context("failed to open throughput stream")?;
                    send_data_on_stream(&mut stream, stream_size).await?;
                    bytes.fetch_add(stream_size, Ordering::Relaxed);
                    streams.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }

        for task in tasks {
            task.await.context("throughput-client task panicked")??;
        }

        connection.close(0u32.into(), b"throughput done");
        endpoint.wait_idle().await;

        Ok(ConnectionResult {
            bytes: bytes.load(Ordering::Relaxed),
            streams: streams.load(Ordering::Relaxed),
            elapsed: start.elapsed(),
        })
    })
}

#[derive(Debug, Parser, Clone, Copy)]
#[command(name = "contention_throughput_client")]
struct Opt {
    /// Server `addr:port` to dial.
    #[arg(long)]
    connect: SocketAddr,

    /// Upper bound on the upload phase, in seconds.
    #[arg(long, default_value = "10")]
    duration_secs: u64,

    /// Number of concurrent long-lived upload connections.
    #[arg(long, default_value = "1")]
    connections: usize,

    /// Concurrent uni streams opened per connection.
    #[arg(long, default_value = "1")]
    streams_per_connection: usize,

    /// Bytes uploaded per stream attempt. Accepts SI suffixes (k/M/G/T).
    #[arg(long, default_value = "16M", value_parser = contention::parse_byte_size)]
    stream_size: u64,

    /// Run exactly N full upload streams per connection instead of using the
    /// duration-based cutoff (handy for reproducible data volumes).
    #[arg(long)]
    stream_runs: Option<u64>,

    /// Initial MTU used when dialling.
    #[arg(long, default_value = "1200")]
    initial_mtu: u16,
}

#[derive(Debug)]
struct ConnectionResult {
    bytes: u64,
    streams: u64,
    elapsed: Duration,
}
