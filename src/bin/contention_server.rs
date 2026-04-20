//! Standalone contention-benchmark server, ported from `bench/src/bin/server.rs`
//! on quinn's `endpoint-lock-optimization-accept-split` branch.
//!
//! Accepts every incoming connection and drains every uni stream the peer
//! opens, doing the bare minimum so measurements reflect transport
//! throughput / endpoint-lock behaviour rather than application logic.
//!
//! Paired with `contention_throughput_client` and `contention_churn_client`
//! via `scripts/run_contention_compare.sh`.

use std::net::SocketAddr;

use anyhow::Result;
use clap::Parser;
use quinn_benchmark::{
    contention::{configure_tracing_subscriber, drain_stream, mt_rt, server_endpoint},
    ensure_crypto_provider,
};
use tracing::{info, warn};

fn main() -> Result<()> {
    ensure_crypto_provider();
    configure_tracing_subscriber();

    let opt = Opt::parse();
    let runtime = mt_rt(opt.worker_threads);
    let endpoint = server_endpoint(
        &runtime,
        opt.listen,
        opt.initial_mtu,
        opt.max_concurrent_uni_streams,
    )?;
    info!("listening on {}", endpoint.local_addr()?);

    runtime.block_on(async move {
        while let Some(incoming) = endpoint.accept().await {
            let read_unordered = opt.read_unordered;
            tokio::spawn(async move {
                let connection = match incoming.await {
                    Ok(c) => c,
                    Err(error) => {
                        warn!("handshake failed: {error}");
                        return;
                    }
                };

                while let Ok(mut stream) = connection.accept_uni().await {
                    tokio::spawn(async move {
                        let _ = drain_stream(&mut stream, read_unordered).await;
                    });
                }
            });
        }
    });

    Ok(())
}

#[derive(Debug, Parser, Clone, Copy)]
#[command(name = "contention_server")]
struct Opt {
    /// UDP socket to bind the QUIC listener on.
    #[arg(long, default_value = "127.0.0.1:4433")]
    listen: SocketAddr,

    /// Worker threads for the server tokio runtime.
    #[arg(long, default_value = "8")]
    worker_threads: usize,

    /// Initial MTU advertised to the peer.
    #[arg(long, default_value = "1200")]
    initial_mtu: u16,

    /// Max concurrent uni streams advertised by the endpoint.
    #[arg(long, default_value = "131072")]
    max_concurrent_uni_streams: u64,

    /// Use the unordered `read_chunk` API instead of ordered reads.
    #[arg(long)]
    read_unordered: bool,
}
