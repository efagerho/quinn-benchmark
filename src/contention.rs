//! Shared helpers for the multi-process contention benchmark
//! (`contention_server` + `contention_throughput_client` +
//! `contention_churn_client`) and for the single-process `contention_bench`
//! binary.
//!
//! Ported from the upstream `bench/` crate on quinn's
//! `endpoint-lock-optimization-accept-split` branch. Keep the knobs,
//! certificate handling and helper signatures close to upstream so the
//! numbers are comparable across the two repos.

use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    num::ParseIntError,
    str::FromStr,
    sync::Arc,
};

use anyhow::{Context, Result};
use bytes::Bytes;
use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use tokio::runtime::{Builder, Runtime};

use crate::generate_self_signed;

/// ALPN protocol advertised by every contention-benchmark client and
/// server. rustls' QUIC profile rejects the handshake with
/// `NO_APPLICATION_PROTOCOL` when one side advertises ALPN and the other
/// does not, so both sides must agree on a non-empty list.
pub const ALPN: &[&[u8]] = &[b"quinn-bench"];

/// Pick a wildcard bind address in the same family as `remote`.
///
/// Needed so a client process can always bind regardless of whether the
/// server is on `127.0.0.1` or `[::1]`.
pub fn bind_addr_for(remote: SocketAddr) -> SocketAddr {
    let ip = match remote {
        SocketAddr::V4(_) => IpAddr::V4(Ipv4Addr::UNSPECIFIED),
        SocketAddr::V6(_) => IpAddr::V6(Ipv6Addr::UNSPECIFIED),
    };
    SocketAddr::new(ip, 0)
}

/// Parse a byte count with an optional SI suffix (`k`, `M`, `G`, `T`).
pub fn parse_byte_size(s: &str) -> Result<u64, ParseIntError> {
    let s = s.trim();

    let multiplier: u64 = match s.chars().last() {
        Some('T') => 1024 * 1024 * 1024 * 1024,
        Some('G') => 1024 * 1024 * 1024,
        Some('M') => 1024 * 1024,
        Some('k') => 1024,
        _ => 1,
    };

    let s = match multiplier {
        1 => s,
        _ => &s[..s.len() - 1],
    };

    Ok(u64::from_str(s)? * multiplier)
}

/// Large-window transport config used by the contention workload.
pub fn make_transport_config(initial_mtu: u16, max_concurrent_uni_streams: u64) -> quinn::TransportConfig {
    let mut config = quinn::TransportConfig::default();
    config.initial_mtu(initial_mtu);
    config.max_concurrent_uni_streams(max_concurrent_uni_streams.try_into().unwrap());
    config.stream_receive_window((64_u32 * 1024 * 1024).into());
    config.receive_window((256_u32 * 1024 * 1024).into());
    config.send_window(256 * 1024 * 1024);

    let mut acks = quinn::AckFrequencyConfig::default();
    acks.ack_eliciting_threshold(10u32.into());
    config.ack_frequency_config(Some(acks));

    config
}

/// Build a standalone contention-bench server endpoint. Generates a fresh
/// self-signed cert on every call — the client-side trusts any cert via
/// [`insecure_client_config`], so the pair is ignored for measurement
/// purposes.
pub fn server_endpoint(
    rt: &Runtime,
    listen: SocketAddr,
    initial_mtu: u16,
    max_concurrent_uni_streams: u64,
) -> Result<quinn::Endpoint> {
    let (cert, key) = generate_self_signed();

    // Build the rustls ServerConfig ourselves so we can set ALPN before
    // wrapping it in QuicServerConfig; `quinn::ServerConfig::with_crypto`
    // does not expose a way to retrofit ALPN afterwards.
    let mut crypto = rustls::ServerConfig::builder_with_provider(
        rustls::crypto::ring::default_provider().into(),
    )
    .with_protocol_versions(&[&rustls::version::TLS13])
    .context("select TLS 1.3 for contention server")?
    .with_no_client_auth()
    .with_single_cert(vec![cert], key)
    .context("install contention server certificate")?;
    crypto.alpn_protocols = ALPN.iter().map(|p| p.to_vec()).collect();

    let quic = QuicServerConfig::try_from(crypto).context("build QUIC server crypto")?;
    let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(quic));
    server_config.transport = Arc::new(make_transport_config(
        initial_mtu,
        max_concurrent_uni_streams,
    ));

    let _guard = rt.enter();
    quinn::Endpoint::server(server_config, listen)
        .context("unable to create contention server endpoint")
}

/// Build a client-side `quinn::Endpoint` bound to a wildcard address
/// matching `remote`'s family, registered on `rt`.
pub fn client_endpoint(rt: &Runtime, remote: SocketAddr) -> Result<quinn::Endpoint> {
    let _guard = rt.enter();
    quinn::Endpoint::client(bind_addr_for(remote))
        .context("unable to create contention client endpoint")
}

/// Build a `quinn::ClientConfig` that trusts any server certificate.
///
/// Only appropriate for localhost benchmarks where the server generates an
/// ephemeral self-signed cert per run and no secure channel exists to ferry
/// it to the client process.
pub fn insecure_client_config(
    initial_mtu: u16,
    max_concurrent_uni_streams: u64,
) -> Result<quinn::ClientConfig> {
    let default_provider = rustls::crypto::ring::default_provider();
    let provider = Arc::new(rustls::crypto::CryptoProvider {
        cipher_suites: default_provider.cipher_suites.to_vec(),
        ..default_provider
    });

    let mut crypto = rustls::ClientConfig::builder_with_provider(provider.clone())
        .with_protocol_versions(&[&rustls::version::TLS13])
        .context("select TLS 1.3 for insecure client")?
        .dangerous()
        .with_custom_certificate_verifier(SkipServerVerification::new(provider))
        .with_no_client_auth();
    crypto.alpn_protocols = ALPN.iter().map(|p| p.to_vec()).collect();

    let mut client_config = quinn::ClientConfig::new(Arc::new(QuicClientConfig::try_from(crypto)?));
    client_config.transport_config(Arc::new(make_transport_config(
        initial_mtu,
        max_concurrent_uni_streams,
    )));
    Ok(client_config)
}

/// Drain a uni recv stream to EOF. `read_unordered=true` uses the unordered
/// chunk API; otherwise ordered reads into a 32-buffer batch.
pub async fn drain_stream(stream: &mut quinn::RecvStream, read_unordered: bool) -> Result<usize> {
    let mut read = 0;

    if read_unordered {
        while let Some(chunk) = stream.read_chunk(usize::MAX, false).await? {
            read += chunk.bytes.len();
        }
    } else {
        // 32 buffers × ~1 KiB each → ~32 KiB drain per syscall.
        #[rustfmt::skip]
        let mut bufs = [
            Bytes::new(), Bytes::new(), Bytes::new(), Bytes::new(),
            Bytes::new(), Bytes::new(), Bytes::new(), Bytes::new(),
            Bytes::new(), Bytes::new(), Bytes::new(), Bytes::new(),
            Bytes::new(), Bytes::new(), Bytes::new(), Bytes::new(),
            Bytes::new(), Bytes::new(), Bytes::new(), Bytes::new(),
            Bytes::new(), Bytes::new(), Bytes::new(), Bytes::new(),
            Bytes::new(), Bytes::new(), Bytes::new(), Bytes::new(),
            Bytes::new(), Bytes::new(), Bytes::new(), Bytes::new(),
        ];

        while let Some(n) = stream.read_chunks(&mut bufs[..]).await? {
            read += bufs.iter().take(n).map(|buf| buf.len()).sum::<usize>();
        }
    }

    Ok(read)
}

/// Upload `stream_size` bytes on a uni send stream in 1 MiB chunks and
/// finish it cleanly.
pub async fn send_data_on_stream(stream: &mut quinn::SendStream, stream_size: u64) -> Result<()> {
    const DATA: &[u8] = &[0xAB; 1024 * 1024];
    let bytes_data = Bytes::from_static(DATA);

    let full_chunks = stream_size / (DATA.len() as u64);
    let remaining = (stream_size % (DATA.len() as u64)) as usize;

    for _ in 0..full_chunks {
        stream
            .write_chunk(bytes_data.clone())
            .await
            .context("failed sending data")?;
    }

    if remaining != 0 {
        stream
            .write_chunk(bytes_data.slice(0..remaining))
            .await
            .context("failed sending data")?;
    }

    stream.finish().context("failed finishing stream")?;
    let _ = stream.stopped().await;
    Ok(())
}

/// Single-threaded current-thread tokio runtime. Matches upstream
/// `bench::rt()`. Used by the per-connection / per-worker client threads.
pub fn rt() -> Runtime {
    Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current-thread runtime")
}

/// Multi-thread tokio runtime with `worker_threads` workers (minimum 1).
pub fn mt_rt(worker_threads: usize) -> Runtime {
    Builder::new_multi_thread()
        .worker_threads(worker_threads.max(1))
        .enable_all()
        .build()
        .expect("multi-thread runtime")
}

/// Install the tracing subscriber a benchmark binary should use.
///
/// Uses `RUST_LOG` (or `error` if unset) so benchmark output stays quiet by
/// default but stack traces / warnings remain visible when debugging.
pub fn configure_tracing_subscriber() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("error"));
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
}

#[derive(Debug)]
struct SkipServerVerification(Arc<rustls::crypto::CryptoProvider>);

impl SkipServerVerification {
    fn new(provider: Arc<rustls::crypto::CryptoProvider>) -> Arc<Self> {
        Arc::new(Self(provider))
    }
}

impl rustls::client::danger::ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}
