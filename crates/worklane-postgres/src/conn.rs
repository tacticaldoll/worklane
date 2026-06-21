//! Connection-pool construction for the Postgres broker.

use deadpool_postgres::{ManagerConfig, Pool, RecyclingMethod, Runtime};
use tokio_postgres::NoTls;
use worklane_core::{Error, Result};

/// Build a `deadpool` connection pool of `max_size` connections from a Postgres
/// `url`. Credentials in any parse/build error are redacted before the message
/// escapes into `Error` (and onward to logs and dead-letter reasons).
pub(crate) fn build_pool(url: &str, max_size: usize) -> Result<Pool> {
    let pg_config: tokio_postgres::Config = url.parse().map_err(|e| {
        Error::Broker(worklane_core::redact_credentials(&format!(
            "invalid postgres url: {e}"
        )))
    })?;
    let mgr = deadpool_postgres::Manager::from_config(
        pg_config,
        NoTls,
        ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        },
    );
    finish_pool(mgr, max_size)
}

/// As [`build_pool`], but the manager negotiates TLS with rustls using the
/// system root certificates. Behind the `tls` feature.
#[cfg(feature = "tls")]
pub(crate) fn build_pool_tls(url: &str, max_size: usize) -> Result<Pool> {
    let pg_config: tokio_postgres::Config = url.parse().map_err(|e| {
        Error::Broker(worklane_core::redact_credentials(&format!(
            "invalid postgres url: {e}"
        )))
    })?;

    let mut roots = rustls::RootCertStore::empty();
    let loaded = rustls_native_certs::load_native_certs();
    for cert in loaded.certs {
        let _ = roots.add(cert);
    }
    if roots.is_empty() {
        return Err(Error::Broker(
            "no system root certificates were available for TLS".to_string(),
        ));
    }

    let provider = std::sync::Arc::new(rustls::crypto::ring::default_provider());
    let tls_config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| Error::Broker(format!("tls configuration failed: {e}")))?
        .with_root_certificates(roots)
        .with_no_client_auth();

    let tls = tokio_postgres_rustls::MakeRustlsConnect::new(tls_config);
    let mgr = deadpool_postgres::Manager::from_config(
        pg_config,
        tls,
        ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        },
    );
    finish_pool(mgr, max_size)
}

/// Build the pool from a configured manager (TLS or not).
fn finish_pool(mgr: deadpool_postgres::Manager, max_size: usize) -> Result<Pool> {
    Pool::builder(mgr)
        .max_size(max_size)
        .runtime(Runtime::Tokio1)
        .build()
        .map_err(|e| {
            Error::Broker(worklane_core::redact_credentials(&format!(
                "pool build failed: {e}"
            )))
        })
}
