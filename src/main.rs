use std::{borrow::Cow, sync::Arc};

use anyhow::{Context, Result, bail};
use coolify_glitchtip_bridge::{BridgeRuntime, Config, SentrySink, app};
use sentry::ClientOptions;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let config = Arc::new(Config::from_env()?);
    let dsn = config
        .glitchtip_dsn
        .parse::<sentry::types::Dsn>()
        .context("GLITCHTIP_DSN is invalid")?;
    let mut options = ClientOptions::new().shutdown_timeout(config.shutdown_timeout);
    options.environment = Some(Cow::Owned(config.environment.clone()));
    options.release = config.release.clone().map(Cow::Owned);
    let sentry_guard = sentry::init((dsn, options));
    if !sentry_guard.is_enabled() {
        bail!("GlitchTip client could not be enabled");
    }

    let runtime = BridgeRuntime::spawn(&config, Arc::new(SentrySink));
    let router = app(config.clone(), runtime.handle.clone());
    let listener = tokio::net::TcpListener::bind(config.bind_addr)
        .await
        .with_context(|| format!("could not bind {}", config.bind_addr))?;

    tracing::info!(
        address = %config.bind_addr,
        environment = %config.environment,
        version = env!("CARGO_PKG_VERSION"),
        "coolify-glitchtip-bridge started"
    );
    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("HTTP server failed")?;

    tracing::info!("shutting down and flushing buffered events");
    runtime.shutdown().await;
    drop(sentry_guard);
    Ok(())
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("coolify_glitchtip_bridge=info,tower_http=info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .compact()
        .init();
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
