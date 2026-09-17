use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;

use axum::routing::{get, post};
use axum::Router;
use clap::Parser;
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::trace::TraceLayer;

use relay_rs::client_ip::{parse_trusted_proxy_cidrs, ClientIpResolver};
use relay_rs::config::Config;
use relay_rs::logs::{self, LogStore, MAX_LOG_SIZE};
use relay_rs::quotas::{ConnectionQuota, IpRateLimiter, CONNECT_ATTEMPT_BURST, CONNECT_ATTEMPT_SUSTAINED};
use relay_rs::registry::Registry;
use relay_rs::snapshot::{self, SNAPSHOT_FLUSH_TIMEOUT};
use relay_rs::{cleanup, ws, AppState};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::parse();

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .json()
        .init();

    // Unset/unparseable TRUSTED_PROXY_CIDRS means "trust no proxy" — warn
    // loudly and continue rather than refusing to start (accepted design
    // decision, see README).
    let trusted_proxy_cidrs = match parse_trusted_proxy_cidrs(&config.trusted_proxy_cidrs) {
        Ok(cidrs) => cidrs,
        Err(_) => {
            tracing::warn!(
                value = %config.trusted_proxy_cidrs,
                "TRUSTED_PROXY_CIDRS failed to parse; trusting no proxies"
            );
            Vec::new()
        }
    };
    if trusted_proxy_cidrs.is_empty() {
        tracing::warn!(
            "TRUSTED_PROXY_CIDRS is unset — every request resolves to its raw TCP peer address. \
             If this relay sits behind a reverse proxy, IP-keyed quotas/rate limits will collapse \
             onto the proxy's address until this is configured. See the README."
        );
    }
    let client_ip = Arc::new(ClientIpResolver::new(trusted_proxy_cidrs));

    let state_file = PathBuf::from(&config.state_file);
    let restored = snapshot::load_snapshot(&state_file, SystemTime::now());

    let (snapshot_handle, snapshot_rx) = snapshot::channel();
    let registry = Arc::new(Registry::new(snapshot_handle.clone()));
    registry.restore(restored).await;

    let app = AppState {
        registry: registry.clone(),
        logs: Arc::new(LogStore::new()),
        client_ip,
        connections: Arc::new(ConnectionQuota::default()),
        connect_limiter: Arc::new(IpRateLimiter::new(CONNECT_ATTEMPT_BURST, CONNECT_ATTEMPT_SUSTAINED)),
        snapshot: snapshot_handle.clone(),
    };

    tokio::spawn(snapshot::run(snapshot_rx, registry, state_file));
    tokio::spawn(cleanup::run(app.clone()));

    let router = Router::new()
        .route("/ws", get(ws::ws_handler))
        .route("/logs", post(logs::post_logs).route_layer(RequestBodyLimitLayer::new(MAX_LOG_SIZE)))
        .route("/logs/:id", get(logs::get_logs))
        .route("/health", get(logs::health))
        .layer(TraceLayer::new_for_http())
        .with_state(app);

    let bind_addr = config.bind_addr();
    let listener = tokio::net::TcpListener::bind(&bind_addr).await?;
    tracing::info!(addr = %bind_addr, "listening");

    axum::serve(listener, router.into_make_service_with_connect_info::<SocketAddr>())
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    tracing::info!("shutting down, flushing snapshot");
    if let Err(e) = snapshot_handle.flush_and_stop(SNAPSHOT_FLUSH_TIMEOUT).await {
        tracing::error!(error = %e.0, "snapshot flush on shutdown failed");
    }

    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c().await.expect("failed to install Ctrl+C handler");
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
