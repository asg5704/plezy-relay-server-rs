//! Rate limiters and connection/room quotas. Mirrors Go's `rate_limit.go`
//! (`rateLimiter`, `connTracker`) split across a few small pieces:
//!
//! - [`TokenBucket`]: hand-rolled burst+sustained bucket for the
//!   per-connection inbound-message limiter, which must throttle with an
//!   error reply and never disconnect — a semantic `governor` doesn't offer
//!   directly, so this one stays bespoke.
//! - [`IpRateLimiter`]: `governor`-backed keyed limiter for connect-attempt
//!   and failed-log-lookup limits, both burst+sustained per source IP.
//! - [`LastTimestampLimiter`]: the log-upload limiter (1 per IP per 60s) —
//!   Go implements this as a plain "last upload time" map, not a token
//!   bucket, so it's kept that way here rather than forced through
//!   `governor`.
//! - [`ConnectionQuota`]: global + per-IP concurrent WebSocket connection
//!   counters.

use std::collections::HashMap;
use std::net::IpAddr;
use std::num::NonZeroU32;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use governor::{clock::DefaultClock, state::keyed::DefaultKeyedStateStore, Quota, RateLimiter};

pub const CONNECT_ATTEMPT_BURST: u32 = 5;
pub const CONNECT_ATTEMPT_SUSTAINED: u32 = 1;
pub const FAILED_LOG_LOOKUP_BURST: u32 = 10;
pub const FAILED_LOG_LOOKUP_SUSTAINED: u32 = 1;
pub const LOG_UPLOAD_INTERVAL: Duration = Duration::from_secs(60);
pub const MAX_GLOBAL_CONNS: usize = 100;
pub const MAX_CONNS_PER_IP: usize = 5;
pub const PER_CONNECTION_MESSAGE_BURST: f64 = 30.0;
pub const PER_CONNECTION_MESSAGE_SUSTAINED: f64 = 10.0;

/// A simple leaky/token bucket: `burst` capacity, refilling at `sustained`
/// tokens/second. Used where "throttle, don't disconnect" is required.
pub struct TokenBucket {
    tokens: f64,
    max_tokens: f64,
    refill_per_sec: f64,
    last: Instant,
}

impl TokenBucket {
    pub fn new(burst: f64, sustained_per_sec: f64) -> Self {
        Self { tokens: burst, max_tokens: burst, refill_per_sec: sustained_per_sec, last: Instant::now() }
    }

    pub fn allow(&mut self) -> bool {
        self.allow_at(Instant::now())
    }

    fn allow_at(&mut self, now: Instant) -> bool {
        self.refill(now);
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }

    fn refill(&mut self, now: Instant) {
        if now < self.last {
            return;
        }
        let elapsed = now.duration_since(self.last).as_secs_f64();
        self.last = now;
        self.tokens = (self.tokens + elapsed * self.refill_per_sec).min(self.max_tokens);
    }
}

/// A `governor`-backed burst+sustained limiter keyed by source IP.
pub struct IpRateLimiter {
    inner: RateLimiter<IpAddr, DefaultKeyedStateStore<IpAddr>, DefaultClock>,
}

impl IpRateLimiter {
    pub fn new(burst: u32, sustained_per_sec: u32) -> Self {
        let quota = Quota::per_second(NonZeroU32::new(sustained_per_sec.max(1)).unwrap())
            .allow_burst(NonZeroU32::new(burst.max(1)).unwrap());
        Self { inner: RateLimiter::keyed(quota) }
    }

    pub fn allow(&self, ip: IpAddr) -> bool {
        self.inner.check_key(&ip).is_ok()
    }
}

/// Per-IP "at most one allowed action per `interval`" gate — used for the
/// crash-log upload endpoint, which Go rate-limits via a plain timestamp
/// map rather than a token bucket.
pub struct LastTimestampLimiter {
    interval: Duration,
    last: Mutex<HashMap<IpAddr, Instant>>,
}

impl LastTimestampLimiter {
    pub fn new(interval: Duration) -> Self {
        Self { interval, last: Mutex::new(HashMap::new()) }
    }

    pub fn allow(&self, ip: IpAddr) -> bool {
        let now = Instant::now();
        let mut map = self.last.lock().unwrap();
        if let Some(&last) = map.get(&ip) {
            if now.duration_since(last) < self.interval {
                return false;
            }
        }
        map.insert(ip, now);
        true
    }

    pub fn cleanup(&self) {
        let cutoff = self.interval;
        let now = Instant::now();
        let mut map = self.last.lock().unwrap();
        map.retain(|_, &mut last| now.duration_since(last) < cutoff);
    }
}

/// Global + per-IP concurrent WebSocket connection counters, mirroring
/// Go's `connTracker`'s connection-slot bookkeeping (room-count quota is
/// kept in `registry.rs` instead, since it's tied to room lifetime rather
/// than connection lifetime).
#[derive(Default)]
pub struct ConnectionQuota {
    inner: Mutex<ConnectionQuotaInner>,
}

#[derive(Default)]
struct ConnectionQuotaInner {
    global: usize,
    per_ip: HashMap<IpAddr, usize>,
}

impl ConnectionQuota {
    pub fn try_connect(&self, ip: IpAddr) -> bool {
        let mut inner = self.inner.lock().unwrap();
        if inner.global >= MAX_GLOBAL_CONNS {
            return false;
        }
        let count = inner.per_ip.entry(ip).or_insert(0);
        if *count >= MAX_CONNS_PER_IP {
            return false;
        }
        *count += 1;
        inner.global += 1;
        true
    }

    pub fn disconnect(&self, ip: IpAddr) {
        let mut inner = self.inner.lock().unwrap();
        let mut decremented = false;
        let mut now_zero = false;
        if let Some(count) = inner.per_ip.get_mut(&ip) {
            if *count > 0 {
                *count -= 1;
                decremented = true;
            }
            now_zero = *count == 0;
        }
        if now_zero {
            inner.per_ip.remove(&ip);
        }
        if decremented {
            inner.global = inner.global.saturating_sub(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_bucket_exhausts_and_refills() {
        let mut bucket = TokenBucket::new(2.0, 1.0);
        assert!(bucket.allow_at(Instant::now()));
        assert!(bucket.allow_at(Instant::now()));
        assert!(!bucket.allow_at(Instant::now()));
        let later = Instant::now() + Duration::from_secs(1);
        assert!(bucket.allow_at(later));
    }

    #[test]
    fn connection_quota_enforces_per_ip_and_global_caps() {
        let quota = ConnectionQuota::default();
        let ip: IpAddr = "10.0.0.1".parse().unwrap();
        for _ in 0..MAX_CONNS_PER_IP {
            assert!(quota.try_connect(ip));
        }
        assert!(!quota.try_connect(ip));
        quota.disconnect(ip);
        assert!(quota.try_connect(ip));
    }

    #[test]
    fn last_timestamp_limiter_blocks_within_interval() {
        let limiter = LastTimestampLimiter::new(Duration::from_secs(60));
        let ip: IpAddr = "10.0.0.2".parse().unwrap();
        assert!(limiter.allow(ip));
        assert!(!limiter.allow(ip));
    }

    #[test]
    fn ip_rate_limiter_allows_burst_then_blocks() {
        let limiter = IpRateLimiter::new(2, 1);
        let ip: IpAddr = "10.0.0.3".parse().unwrap();
        assert!(limiter.allow(ip));
        assert!(limiter.allow(ip));
        assert!(!limiter.allow(ip));
    }
}
