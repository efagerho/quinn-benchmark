use std::{
    fs,
    net::SocketAddr,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use bytes::Bytes;
use clap::Parser;
use quinn_benchmark::{build_client_config, ensure_crypto_provider};
use rustls::pki_types::CertificateDer;
use tokio::{task::JoinSet, time};

#[derive(Parser, Debug, Clone)]
struct Args {
    #[arg(long, default_value = "127.0.0.1:4433")]
    server: SocketAddr,
    #[arg(long, default_value = "cert.der")]
    cert: PathBuf,
    #[arg(long, default_value_t = 10)]
    persistent_connections: usize,
    #[arg(long, default_value_t = 50_000.0)]
    packet_rate: f64,
    #[arg(long, default_value_t = 200.0)]
    new_connection_rate: f64,
    #[arg(long, default_value_t = 256)]
    packet_size: usize,
    #[arg(long, default_value_t = 10)]
    duration_secs: u64,
    #[arg(long, default_value_t = 1)]
    report_interval_secs: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    ensure_crypto_provider();

    let cert = fs::read(&args.cert).context("failed to read certificate file")?;
    let client_cfg = build_client_config(CertificateDer::from(cert))?;
    let endpoint = quinn::Endpoint::client("[::]:0".parse()?)?;
    endpoint.set_default_client_config(client_cfg);

    let packet_counter = Arc::new(AtomicU64::new(0));
    let connection_counter = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let started = Instant::now();
    let duration = Duration::from_secs(args.duration_secs);
    let payload = Bytes::from(vec![0xAB; args.packet_size]);

    let mut tasks = JoinSet::new();

    for _ in 0..args.persistent_connections {
        let endpoint = endpoint.clone();
        let per_connection_rate = if args.persistent_connections == 0 {
            0.0
        } else {
            args.packet_rate / args.persistent_connections as f64
        };
        let stop = stop.clone();
        let packets = packet_counter.clone();
        let payload = payload.clone();
        tasks.spawn(async move {
            run_persistent_sender(
                endpoint,
                args.server,
                per_connection_rate,
                stop,
                packets,
                payload,
            )
            .await
        });
    }

    {
        let endpoint = endpoint.clone();
        let args = args.clone();
        let stop = stop.clone();
        let connections = connection_counter.clone();
        tasks.spawn(async move {
            run_connection_churn(endpoint, args.server, args.new_connection_rate, stop, connections).await
        });
    }

    {
        let args = args.clone();
        let stop = stop.clone();
        let packets = packet_counter.clone();
        let connections = connection_counter.clone();
        tasks.spawn(async move {
            run_reporter(started, args.report_interval_secs, stop, packets, connections).await
        });
    }

    tokio::select! {
        _ = time::sleep(duration) => {}
        _ = tokio::signal::ctrl_c() => {}
    }
    stop.store(true, Ordering::Relaxed);

    while let Some(res) = tasks.join_next().await {
        res??;
    }

    let elapsed = started.elapsed().as_secs_f64().max(1e-9);
    let total_packets = packet_counter.load(Ordering::Relaxed);
    let total_connections = connection_counter.load(Ordering::Relaxed);
    println!(
        "final packets_per_sec={:.2} connections_per_sec={:.2} total_packets={} total_connections={}",
        total_packets as f64 / elapsed,
        total_connections as f64 / elapsed,
        total_packets,
        total_connections
    );

    endpoint.wait_idle().await;
    Ok(())
}

async fn run_persistent_sender(
    endpoint: quinn::Endpoint,
    server: SocketAddr,
    per_connection_packet_rate: f64,
    stop: Arc<AtomicBool>,
    packet_counter: Arc<AtomicU64>,
    payload: Bytes,
) -> Result<()> {
    let connection = endpoint
        .connect(server, "localhost")?
        .await
        .context("persistent connect failed")?;

    let mut ticker = interval_for_rate(per_connection_packet_rate.max(0.0));

    while !stop.load(Ordering::Relaxed) {
        if let Some(interval) = ticker.as_mut() {
            interval.tick().await;
        } else {
            time::sleep(Duration::from_millis(50)).await;
            continue;
        }

        let (mut send, mut recv) = connection.open_bi().await?;
        send.write_all(payload.as_ref()).await?;
        send.finish()?;
        let echoed = recv.read_to_end(payload.len() + 1).await?;
        if echoed.len() == payload.len() {
            packet_counter.fetch_add(1, Ordering::Relaxed);
        }
    }

    connection.close(0u32.into(), b"done");
    Ok(())
}

async fn run_connection_churn(
    endpoint: quinn::Endpoint,
    server: SocketAddr,
    new_connection_rate: f64,
    stop: Arc<AtomicBool>,
    connection_counter: Arc<AtomicU64>,
) -> Result<()> {
    let mut ticker = interval_for_rate(new_connection_rate.max(0.0));
    while !stop.load(Ordering::Relaxed) {
        if let Some(interval) = ticker.as_mut() {
            interval.tick().await;
        } else {
            time::sleep(Duration::from_millis(50)).await;
            continue;
        }

        if let Ok(connecting) = endpoint.connect(server, "localhost")
            && let Ok(conn) = connecting.await
        {
            connection_counter.fetch_add(1, Ordering::Relaxed);
            conn.close(0u32.into(), b"done");
        }
    }
    Ok(())
}

async fn run_reporter(
    started: Instant,
    report_interval_secs: u64,
    stop: Arc<AtomicBool>,
    packet_counter: Arc<AtomicU64>,
    connection_counter: Arc<AtomicU64>,
) -> Result<()> {
    let interval = Duration::from_secs(report_interval_secs.max(1));
    let mut ticker = time::interval(interval);
    loop {
        ticker.tick().await;
        let elapsed = started.elapsed().as_secs_f64().max(1e-9);
        let packets = packet_counter.load(Ordering::Relaxed);
        let connections = connection_counter.load(Ordering::Relaxed);
        println!(
            "elapsed={:.2}s packets_per_sec={:.2} connections_per_sec={:.2} total_packets={} total_connections={}",
            elapsed,
            packets as f64 / elapsed,
            connections as f64 / elapsed,
            packets,
            connections
        );
        if stop.load(Ordering::Relaxed) {
            break;
        }
    }
    Ok(())
}

fn interval_for_rate(rate_per_sec: f64) -> Option<time::Interval> {
    if rate_per_sec <= 0.0 {
        return None;
    }
    let period = Duration::from_secs_f64(1.0 / rate_per_sec);
    let mut interval = time::interval(period);
    interval.set_missed_tick_behavior(time::MissedTickBehavior::Burst);
    Some(interval)
}
