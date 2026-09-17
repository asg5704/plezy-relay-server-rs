//! Crash-log upload/download endpoints (`POST /logs`, `GET /logs/{id}`).
//! Mirrors Go's `logStore`/`artifact_store.go` logs path, without the
//! poster-store machinery (out of scope).

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::{Duration, SystemTime};

use axum::body::Bytes;
use axum::extract::{ConnectInfo, Path as AxumPath, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use rand::Rng;
use tokio::sync::{Mutex, Semaphore};

use crate::quotas::{FAILED_LOG_LOOKUP_BURST, FAILED_LOG_LOOKUP_SUSTAINED, LOG_UPLOAD_INTERVAL};
use crate::quotas::{IpRateLimiter, LastTimestampLimiter};
use crate::AppState;

pub const MAX_LOG_SIZE: usize = 1024 * 1024;
pub const MAX_LOG_ENTRIES: usize = 500;
pub const LOG_ID_LENGTH: usize = 5;
pub const LOG_MAX_AGE: Duration = Duration::from_secs(3 * 24 * 60 * 60);
const MAX_CONCURRENT_LOOKUPS: usize = 32;

const ID_ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";

fn generate_log_id() -> String {
    let mut rng = rand::thread_rng();
    (0..LOG_ID_LENGTH).map(|_| ID_ALPHABET[rng.gen_range(0..ID_ALPHABET.len())] as char).collect()
}

fn valid_log_id(id: &str) -> bool {
    id.len() == LOG_ID_LENGTH && id.bytes().all(|b| ID_ALPHABET.contains(&b))
}

struct Entry {
    data: Vec<u8>,
    created_at: SystemTime,
}

pub struct LogStore {
    entries: Mutex<HashMap<String, Entry>>,
    upload_limiter: LastTimestampLimiter,
    failed_lookup_limiter: IpRateLimiter,
    lookup_slots: Semaphore,
}

impl LogStore {
    pub fn new() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            upload_limiter: LastTimestampLimiter::new(LOG_UPLOAD_INTERVAL),
            failed_lookup_limiter: IpRateLimiter::new(FAILED_LOG_LOOKUP_BURST, FAILED_LOG_LOOKUP_SUSTAINED),
            lookup_slots: Semaphore::new(MAX_CONCURRENT_LOOKUPS),
        }
    }

    pub async fn cleanup(&self, now: SystemTime) {
        let mut entries = self.entries.lock().await;
        entries.retain(|_, e| now.duration_since(e.created_at).unwrap_or_default() < LOG_MAX_AGE);
        self.upload_limiter.cleanup();
    }
}

impl Default for LogStore {
    fn default() -> Self {
        Self::new()
    }
}

pub async fn post_logs(
    State(app): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let Ok(ip) = app.client_ip.resolve(peer.ip(), &forwarded_for(&headers)) else {
        return (StatusCode::BAD_REQUEST, "Invalid client address").into_response();
    };
    if !app.logs.upload_limiter.allow(ip) {
        return (StatusCode::TOO_MANY_REQUESTS, "Rate limited: 1 upload per minute").into_response();
    }
    if body.is_empty() {
        return (StatusCode::BAD_REQUEST, "Empty body").into_response();
    }
    if body.len() > MAX_LOG_SIZE {
        return (StatusCode::PAYLOAD_TOO_LARGE, "Log too large (max 1MB)").into_response();
    }

    let now = SystemTime::now();
    let mut entries = app.logs.entries.lock().await;
    entries.retain(|_, e| now.duration_since(e.created_at).unwrap_or_default() < LOG_MAX_AGE);
    if entries.len() >= MAX_LOG_ENTRIES {
        return (StatusCode::SERVICE_UNAVAILABLE, "Log store full").into_response();
    }
    let mut id = generate_log_id();
    while entries.contains_key(&id) {
        id = generate_log_id();
    }
    entries.insert(id.clone(), Entry { data: body.to_vec(), created_at: now });
    drop(entries);

    Json(serde_json::json!({ "id": id })).into_response()
}

pub async fn get_logs(
    State(app): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    AxumPath(id): AxumPath<String>,
    headers: HeaderMap,
) -> Response {
    let Ok(ip) = app.client_ip.resolve(peer.ip(), &forwarded_for(&headers)) else {
        return (StatusCode::BAD_REQUEST, "Invalid client address").into_response();
    };
    let Ok(_permit) = app.logs.lookup_slots.try_acquire() else {
        return (StatusCode::TOO_MANY_REQUESTS, "Too many concurrent lookups").into_response();
    };

    // A malformed id is treated identically to "not found" so the
    // validation rule itself isn't observable from outside.
    let found = if valid_log_id(&id) {
        let entries = app.logs.entries.lock().await;
        entries.get(&id).map(|e| e.data.clone())
    } else {
        None
    };

    let Some(data) = found else {
        if !app.logs.failed_lookup_limiter.allow(ip) {
            return (StatusCode::TOO_MANY_REQUESTS, "Too many failed lookups").into_response();
        }
        return (StatusCode::NOT_FOUND, "Not found").into_response();
    };

    let mut response = data.into_response();
    response.headers_mut().insert(axum::http::header::CONTENT_TYPE, HeaderValue::from_static("text/plain; charset=utf-8"));
    response.headers_mut().insert(axum::http::header::CACHE_CONTROL, HeaderValue::from_static("private, no-store"));
    response
}

pub async fn health() -> &'static str {
    "ok"
}

/// Splits every `X-Forwarded-For` header occurrence out as a plain `&str`
/// list, in wire order, for `ClientIpResolver::resolve`.
pub(crate) fn forwarded_for(headers: &HeaderMap) -> Vec<&str> {
    headers.get_all("x-forwarded-for").iter().filter_map(|v| v.to_str().ok()).collect()
}
