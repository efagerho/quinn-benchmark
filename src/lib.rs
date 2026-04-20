//! Shared helpers for the quinn benchmark binaries.
//!
//! Keeps TLS/crypto setup and self-signed certificate generation in one place
//! so each benchmark binary only has to focus on its specific workload.

use std::sync::{Arc, Once};

use anyhow::{Context, Result};
use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use rustls::{
    RootCertStore,
    pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer},
};

/// Install the `ring` crypto provider as rustls' process-wide default.
///
/// Safe to call from multiple binaries/threads; only the first call wins.
pub fn ensure_crypto_provider() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        // Ignore the error: another call site (possibly in a test harness) may
        // have already installed a provider.
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

/// Generate a fresh self-signed certificate for `localhost` along with its
/// PKCS#8-encoded private key.
pub fn generate_self_signed() -> (CertificateDer<'static>, PrivateKeyDer<'static>) {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()])
        .expect("failed to generate self-signed certificate");
    let key = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
    let cert_der = CertificateDer::from(cert.cert);
    (cert_der, PrivateKeyDer::Pkcs8(key))
}

/// Build a `quinn::ServerConfig` that presents the given self-signed cert.
pub fn build_server_config(
    cert: CertificateDer<'static>,
    key: PrivateKeyDer<'static>,
) -> Result<quinn::ServerConfig> {
    let crypto = rustls::ServerConfig::builder_with_provider(
        rustls::crypto::ring::default_provider().into(),
    )
    .with_protocol_versions(&[&rustls::version::TLS13])
    .context("select TLS 1.3 for server")?
    .with_no_client_auth()
    .with_single_cert(vec![cert], key)
    .context("install server certificate")?;

    let quic = QuicServerConfig::try_from(crypto).context("build QUIC server crypto")?;
    Ok(quinn::ServerConfig::with_crypto(Arc::new(quic)))
}

/// Build a `quinn::ClientConfig` that trusts the given self-signed cert.
pub fn build_client_config(cert: CertificateDer<'static>) -> Result<quinn::ClientConfig> {
    let mut roots = RootCertStore::empty();
    roots.add(cert).context("trust self-signed cert")?;

    let crypto = rustls::ClientConfig::builder_with_provider(
        rustls::crypto::ring::default_provider().into(),
    )
    .with_protocol_versions(&[&rustls::version::TLS13])
    .context("select TLS 1.3 for client")?
    .with_root_certificates(roots)
    .with_no_client_auth();

    let quic = QuicClientConfig::try_from(crypto).context("build QUIC client crypto")?;
    Ok(quinn::ClientConfig::new(Arc::new(quic)))
}
