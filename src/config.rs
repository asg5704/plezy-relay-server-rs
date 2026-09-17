//! CLI/env configuration. Every flag has an env-var fallback so Railway's
//! dashboard env vars work without any flags at all.

use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "relay-rs", about = "Personal Watch Together relay")]
pub struct Config {
    /// Listen address (host:port). Overrides `--port`/`PORT` when set.
    #[arg(long, env = "ADDR")]
    pub addr: Option<String>,

    /// Port to bind on `0.0.0.0`. Railway injects `PORT` automatically.
    #[arg(long, env = "PORT", default_value = "8080")]
    pub port: u16,

    /// Directory for crash-log storage.
    #[arg(long, env = "LOG_DIR", default_value = "/data/logs")]
    pub log_dir: String,

    /// Path to the room-state snapshot file.
    #[arg(long, env = "STATE_FILE", default_value = "/data/rooms.json")]
    pub state_file: String,

    /// Comma-separated trusted reverse-proxy CIDRs. Unset means "trust no
    /// proxy" — every request resolves to its raw TCP peer address. See
    /// the README before setting this on Railway.
    #[arg(long, env = "TRUSTED_PROXY_CIDRS", default_value = "")]
    pub trusted_proxy_cidrs: String,
}

impl Config {
    pub fn bind_addr(&self) -> String {
        self.addr.clone().unwrap_or_else(|| format!("0.0.0.0:{}", self.port))
    }
}
