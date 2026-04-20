use std::{fs, net::SocketAddr, path::PathBuf};

use anyhow::{Context, Result};
use clap::Parser;
use quinn_benchmark::{build_server_config, ensure_crypto_provider, generate_self_signed};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use tracing::error;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, default_value = "127.0.0.1:4433")]
    listen: SocketAddr,
    #[arg(long, default_value = "cert.der")]
    cert_out: PathBuf,
    #[arg(long, default_value = "key.der")]
    key_out: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    ensure_crypto_provider();
    let args = Args::parse();

    let (cert, key) = load_or_create_cert(&args)?;
    let server_config = build_server_config(cert, key)?;
    let endpoint = quinn::Endpoint::server(server_config, args.listen)?;
    println!("echo server listening on {}", endpoint.local_addr()?);

    while let Some(incoming) = endpoint.accept().await {
        tokio::spawn(async move {
            if let Err(e) = handle_connection(incoming).await {
                error!("connection failed: {e:#}");
            }
        });
    }

    Ok(())
}

fn load_or_create_cert(args: &Args) -> Result<(CertificateDer<'static>, PrivateKeyDer<'static>)> {
    if args.cert_out.exists() && args.key_out.exists() {
        let cert = fs::read(&args.cert_out).context("failed to read certificate file")?;
        let key = fs::read(&args.key_out).context("failed to read private key file")?;
        let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key));
        return Ok((CertificateDer::from(cert), key));
    }

    let (cert, key) = generate_self_signed();
    fs::write(&args.cert_out, cert.as_ref()).context("failed to write certificate file")?;
    fs::write(
        &args.key_out,
        key.secret_der().to_vec(),
    )
    .context("failed to write private key file")?;
    Ok((cert, key))
}

async fn handle_connection(incoming: quinn::Incoming) -> Result<()> {
    let connection = incoming.await?;
    loop {
        match connection.accept_bi().await {
            Ok((mut send, mut recv)) => {
                tokio::spawn(async move {
                    if let Err(e) = async {
                        while let Some(chunk) = recv.read_chunk(64 * 1024, true).await? {
                            send.write_all(&chunk.bytes).await?;
                        }
                        send.finish()?;
                        Ok::<_, anyhow::Error>(())
                    }
                    .await
                    {
                        error!("stream echo failed: {e:#}");
                    }
                });
            }
            Err(quinn::ConnectionError::ApplicationClosed { .. }) => return Ok(()),
            Err(quinn::ConnectionError::ConnectionClosed(_)) => return Ok(()),
            Err(e) => return Err(e.into()),
        }
    }
}
