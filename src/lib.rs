pub mod cleanup;
pub mod client_ip;
pub mod config;
pub mod connection;
pub mod error;
pub mod host_transfer;
pub mod ids;
pub mod logs;
pub mod protocol;
pub mod quotas;
pub mod registry;
pub mod room;
pub mod snapshot;
pub mod ws;

use std::sync::Arc;

use client_ip::ClientIpResolver;
use logs::LogStore;
use quotas::{ConnectionQuota, IpRateLimiter};
use registry::Registry;
use snapshot::SnapshotHandle;

/// Everything an axum handler needs, shared across every request. Cheap to
/// clone (every field is an `Arc` or an already-cheap handle).
#[derive(Clone)]
pub struct AppState {
    pub registry: Arc<Registry>,
    pub logs: Arc<LogStore>,
    pub client_ip: Arc<ClientIpResolver>,
    pub connections: Arc<ConnectionQuota>,
    pub connect_limiter: Arc<IpRateLimiter>,
    pub snapshot: SnapshotHandle,
}
