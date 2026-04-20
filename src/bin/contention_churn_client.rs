//! Standalone new-connection (churn) client for the contention benchmark,
//! ported from `bench/src/bin/new_conn_client.rs` on quinn's
//! `endpoint-lock-optimization-accept-split` branch.
//!
//! Spawns one OS thread per worker; each worker owns its own current-thread
//! tokio runtime and its own `quinn::Endpoint`, and opens + closes
//! short-lived connections at a paced `connections_per_second / workers`
//! rate for `duration_secs`. Emits `key=value` lines that
//! `run_contention_compare.sh` parses with `awk -F=`.
//!
//! Note: the pacer uses `next_attempt += interval` and does not catch up if
//! a worker's handshake takes longer than its interval; the achieved rate is
//! therefore bounded by `workers / handshake_latency` and may be well below
//! the target. That matches upstream behaviour; tune `workers` accordingly.

use std::{
    net::SocketAddr,
    sync::{Arc, Barrier},
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use clap::Parser;
use quinn_benchmark::{
    contention::{client_endpoint, configure_tracing_subscriber, insecure_client_config, rt},
    ensure_crypto_provider,
};

fn main() -> Result<()> {
    ensure_crypto_provider();
    configure_tracing_subscriber();

    let opt = Opt::parse();

    if opt.connections_per_second == 0 || opt.workers == 0 {
        println!("duration_secs={}", opt.duration_secs);
        println!(
            "target_connections_per_second={}",
            opt.connections_per_second
        );
        println!("workers={}", opt.workers);
        println!("connections=0");
        println!("achieved_connections_per_second=0.00");
        return Ok(());
    }

    let barrier = Arc::new(Barrier::new(opt.workers));
    let mut threads = Vec::with_capacity(opt.workers);
    for worker_index in 0..opt.workers {
        let barrier = barrier.clone();
        let opt = opt;
        threads.push(std::thread::spawn(move || {
            run_worker(opt, barrier, worker_index)
        }));
    }

    let mut connections = 0_u64;
    for thread in threads {
        connections += thread.join().expect("new-conn thread")?;
    }

    let duration = Duration::from_secs(opt.duration_secs);
    let achieved = connections as f64 / duration.as_secs_f64();

    println!("duration_secs={}", opt.duration_secs);
    println!(
        "target_connections_per_second={}",
        opt.connections_per_second
    );
    println!("workers={}", opt.workers);
    println!("connections={}", connections);
    println!("achieved_connections_per_second={achieved:.2}");

    Ok(())
}

fn run_worker(opt: Opt, barrier: Arc<Barrier>, worker_index: usize) -> Result<u64> {
    let rate = opt.connections_per_second as f64 / opt.workers as f64;
    if rate == 0.0 {
        return Ok(0);
    }

    let runtime = rt();
    let endpoint = client_endpoint(&runtime, opt.connect)?;
    // The churn client doesn't open any streams, but `insecure_client_config`
    // still requires a `max_concurrent_uni_streams`; 16 matches upstream.
    let client_config = insecure_client_config(opt.initial_mtu, 16)?;
    runtime.block_on(async move {
        let interval = Duration::from_secs_f64(1.0 / rate);

        barrier.wait();

        let start = Instant::now();
        let stop_at = start + Duration::from_secs(opt.duration_secs);
        // Stagger worker starts so the server doesn't see N simultaneous
        // Initials on every interval tick.
        let mut next_attempt = start
            + Duration::from_nanos(
                ((interval.as_nanos() / opt.workers as u128) * worker_index as u128) as u64,
            );
        let mut completed = 0_u64;

        while next_attempt < stop_at {
            let now = Instant::now();
            if next_attempt > now {
                std::thread::sleep(next_attempt - now);
            }

            let connection = endpoint
                .connect_with(client_config.clone(), opt.connect, "localhost")
                .context("start churn connect")?
                .await
                .context("unable to connect churn client")?;
            connection.close(0u32.into(), b"churn");
            completed += 1;
            next_attempt += interval;
        }

        endpoint.wait_idle().await;
        Ok::<u64, anyhow::Error>(completed)
    })
}

#[derive(Debug, Parser, Clone, Copy)]
#[command(name = "contention_churn_client")]
struct Opt {
    /// Server `addr:port` to dial.
    #[arg(long)]
    connect: SocketAddr,

    /// How long to keep churning, in seconds.
    #[arg(long, default_value = "10")]
    duration_secs: u64,

    /// Target aggregate rate of new connections per second, split evenly
    /// across `workers`.
    #[arg(long, default_value = "2500")]
    connections_per_second: u64,

    /// Number of parallel worker threads (each with its own endpoint).
    #[arg(long, default_value = "16")]
    workers: usize,

    /// Initial MTU used when dialling.
    #[arg(long, default_value = "1200")]
    initial_mtu: u16,
}
