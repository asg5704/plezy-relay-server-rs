//! Client IP resolution: trusted-proxy CIDR allowlist + right-to-left
//! `X-Forwarded-For` walk, matching Go's `client_ip.go`. This is the
//! security-relevant boundary every quota/rate-limit keys off of — get it
//! wrong and either real users collapse onto one bucket (proxy untrusted)
//! or a spoofed header lets a client claim any IP it likes (proxy
//! over-trusted).

use std::net::{IpAddr, Ipv6Addr};

use ipnet::IpNet;

use crate::error::ClientIpError;

const MAX_FORWARDED_FOR_BYTES: usize = 4 * 1024;
const MAX_FORWARDED_FOR_HOPS: usize = 32;

#[derive(Clone, Default)]
pub struct ClientIpResolver {
    trusted_proxies: Vec<IpNet>,
}

/// Parses a comma-separated `TRUSTED_PROXY_CIDRS` value. Empty/whitespace
/// input means "trust nothing" (every request resolves to its raw TCP peer
/// address) — the safe default when the operator hasn't configured this yet.
pub fn parse_trusted_proxy_cidrs(value: &str) -> Result<Vec<IpNet>, ClientIpError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }
    trimmed
        .split(',')
        .map(|part| part.trim().parse::<IpNet>().map_err(|_| ClientIpError))
        .collect()
}

impl ClientIpResolver {
    pub fn new(trusted_proxies: Vec<IpNet>) -> Self {
        Self { trusted_proxies }
    }

    fn trusted(&self, addr: IpAddr) -> bool {
        let addr = unmap(addr);
        self.trusted_proxies.iter().any(|net| net.contains(&addr))
    }

    /// Resolves the client's address for `peer_addr` (the raw TCP peer
    /// address axum hands us) and the request's `X-Forwarded-For` header
    /// values (already split into one `&str` per header occurrence, in the
    /// order they appeared on the wire).
    pub fn resolve(&self, peer_addr: IpAddr, forwarded_for: &[&str]) -> Result<IpAddr, ClientIpError> {
        let peer = unmap(peer_addr);
        if !self.trusted(peer) {
            return Ok(normalize(peer));
        }
        if forwarded_for.is_empty() {
            return Ok(normalize(peer));
        }

        let mut total_bytes = 0usize;
        let mut hop_count = 0usize;
        for value in forwarded_for {
            total_bytes += value.len();
            if total_bytes > MAX_FORWARDED_FOR_BYTES {
                return Err(ClientIpError);
            }
            hop_count += value.matches(',').count() + 1;
            if hop_count > MAX_FORWARDED_FOR_HOPS {
                return Err(ClientIpError);
            }
        }

        // Walk header values right-to-left (nearest hop first), and within
        // each value walk its comma-separated hops right-to-left too.
        // Each hop must itself be trusted for us to keep peeling further
        // left; the first untrusted (or leftmost-exhausted) address found
        // this way is the resolved client IP.
        let mut selected = peer;
        let mut use_forwarded_hop = true;
        for value in forwarded_for.iter().rev() {
            for element in value.rsplit(',') {
                let element = element.trim();
                if element.is_empty() {
                    return Err(ClientIpError);
                }
                let addr: IpAddr = element.parse().map_err(|_| ClientIpError)?;
                if use_forwarded_hop {
                    if self.trusted(selected) {
                        selected = unmap(addr);
                    } else {
                        use_forwarded_hop = false;
                    }
                }
            }
        }
        Ok(normalize(selected))
    }
}

fn unmap(addr: IpAddr) -> IpAddr {
    match addr {
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => IpAddr::V4(v4),
            None => IpAddr::V6(v6),
        },
        v4 => v4,
    }
}

/// IPv4 addresses are used as-is; IPv6 addresses are masked to their /64 so
/// multiple addresses from the same residential IPv6 prefix share one quota
/// bucket.
fn normalize(addr: IpAddr) -> IpAddr {
    match addr {
        IpAddr::V4(_) => addr,
        IpAddr::V6(v6) => {
            let octets = v6.octets();
            let mut masked = [0u8; 16];
            masked[..8].copy_from_slice(&octets[..8]);
            IpAddr::V6(Ipv6Addr::from(masked))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resolver(cidrs: &str) -> ClientIpResolver {
        ClientIpResolver::new(parse_trusted_proxy_cidrs(cidrs).unwrap())
    }

    #[test]
    fn untrusted_peer_ignores_forwarded_for() {
        let r = resolver("");
        let peer: IpAddr = "203.0.113.5".parse().unwrap();
        let resolved = r.resolve(peer, &["198.51.100.1"]).unwrap();
        assert_eq!(resolved, peer);
    }

    #[test]
    fn trusted_peer_uses_rightmost_untrusted_hop() {
        let r = resolver("10.0.0.0/8");
        let peer: IpAddr = "10.0.0.1".parse().unwrap();
        // rightmost hop is the real client (203.0.113.9), left of that is
        // another untrusted hop that should NOT be consulted since the walk
        // stops at the first untrusted hop found from the right.
        let resolved = r.resolve(peer, &["198.51.100.1, 203.0.113.9"]).unwrap();
        assert_eq!(resolved, "203.0.113.9".parse::<IpAddr>().unwrap());
    }

    #[test]
    fn walk_continues_through_multiple_trusted_hops() {
        let r = resolver("10.0.0.0/8");
        let peer: IpAddr = "10.0.0.1".parse().unwrap();
        // Two trusted proxy hops (10.0.0.2) in front of the real client.
        let resolved = r.resolve(peer, &["203.0.113.9, 10.0.0.2"]).unwrap();
        assert_eq!(resolved, "203.0.113.9".parse::<IpAddr>().unwrap());
    }

    #[test]
    fn malformed_hop_is_rejected() {
        let r = resolver("10.0.0.0/8");
        let peer: IpAddr = "10.0.0.1".parse().unwrap();
        assert!(r.resolve(peer, &["not-an-ip"]).is_err());
    }

    #[test]
    fn oversized_header_is_rejected() {
        let r = resolver("10.0.0.0/8");
        let peer: IpAddr = "10.0.0.1".parse().unwrap();
        let huge = "1.1.1.1,".repeat(2000);
        assert!(r.resolve(peer, &[&huge]).is_err());
    }

    #[test]
    fn ipv6_is_masked_to_slash_64() {
        let r = resolver("");
        let peer: IpAddr = "2001:db8:1234:5678:aaaa:bbbb:cccc:dddd".parse().unwrap();
        let resolved = r.resolve(peer, &[]).unwrap();
        assert_eq!(resolved, "2001:db8:1234:5678::".parse::<IpAddr>().unwrap());
    }

    #[test]
    fn ipv4_mapped_ipv6_is_unmapped() {
        let r = resolver("");
        let peer: IpAddr = "::ffff:203.0.113.5".parse().unwrap();
        let resolved = r.resolve(peer, &[]).unwrap();
        assert_eq!(resolved, "203.0.113.5".parse::<IpAddr>().unwrap());
    }

    #[test]
    fn parse_cidrs_rejects_malformed_entries() {
        assert!(parse_trusted_proxy_cidrs("not-a-cidr").is_err());
        assert!(parse_trusted_proxy_cidrs("10.0.0.0/8, ").is_err());
        assert!(parse_trusted_proxy_cidrs("").unwrap().is_empty());
    }
}
